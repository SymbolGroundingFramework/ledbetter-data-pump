//! Postgres source (contract §9, Operational Details §1).
//!
//! Pure wire-protocol client: no Postgres installation is needed anywhere the
//! engine runs, and the same code reaches localhost, RDS/Aurora, Azure Database
//! for PostgreSQL, Redshift, Cockroach and Supabase — only the host changes.
//!
//! Partitioning follows the specified algorithm exactly:
//!   1. `SELECT MIN(col), MAX(col) FROM (<user query>) AS ldp_src`
//!   2. NULL/NULL â†’ zero partitions â†’ a successful, empty job
//!   3. otherwise split into `chunks` half-open ranges
//!   4. read with `WHERE col >= $1 AND col < $2`
//!
//! Rows are decoded straight into Arrow builders and flushed as batches, so a
//! partition larger than memory cannot blow up the process (DoD #5).

use arrow::array::{
    ArrayRef, BooleanBuilder, Date32Builder, Decimal128Builder, Float64Builder, Int32Builder,
    Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures_util::StreamExt;
use ldp_arrow::{arrow_type_for_pg_oid, decimal_from_str, is_decimal_oid, pg_type_name};
use ldp_core::plan::{PartitionKind, PartitionSpec, Settings, SourceSpec};
use ldp_core::{BatchStream, LdpError, Partition, Source};
use std::sync::{Arc, OnceLock};
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, NoTls, Row};

/// Decimal precision assumed for `numeric` columns. Postgres numerics are
/// effectively unbounded; 38 is Arrow's Decimal128 limit.
const DECIMAL_PRECISION: u8 = 38;

#[derive(Debug, Clone)]
struct PgColumn {
    name: String,
    oid: u32,
    arrow: DataType,
}

pub struct PostgresSource {
    client: Client,
    spec: SourceSpec,
    settings: Settings,
    columns: OnceLock<Arc<Vec<PgColumn>>>,
    schema: OnceLock<SchemaRef>,
    decimal_scale: OnceLock<i8>,
}

impl PostgresSource {
    /// Connect and spawn the connection driver task.
    pub async fn connect(
        spec: SourceSpec,
        settings: Settings,
        url: &str,
    ) -> Result<Self, LdpError> {
        let (client, connection) = tokio_postgres::connect(url, NoTls)
            .await
            .map_err(|e| LdpError::Source(format!("cannot connect to Postgres: {e}")))?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("ldp: postgres connection ended: {e}");
            }
        });
        Ok(Self {
            client,
            spec,
            settings,
            columns: OnceLock::new(),
            schema: OnceLock::new(),
            decimal_scale: OnceLock::new(),
        })
    }

    fn wrapped_query(&self) -> String {
        self.spec.query.trim().trim_end_matches(';').to_string()
    }

    /// `SELECT * FROM (<user query>) AS ldp_src LIMIT 0` reveals the column list
    /// and Postgres types without reading data.
    async fn load_columns(&self) -> Result<Arc<Vec<PgColumn>>, LdpError> {
        if let Some(c) = self.columns.get() {
            return Ok(Arc::clone(c));
        }
        let probe = format!(
            "SELECT * FROM ({}) AS ldp_src LIMIT 0",
            self.wrapped_query()
        );
        let stmt = self
            .client
            .prepare(&probe)
            .await
            .map_err(|e| LdpError::Source(format!("cannot plan source query: {e}")))?;

        let mut cols = Vec::with_capacity(stmt.columns().len());
        let mut errors = Vec::new();
        for c in stmt.columns() {
            let oid = c.type_().oid();
            let arrow = if is_decimal_oid(oid) {
                DataType::Decimal128(DECIMAL_PRECISION, 0)
            } else {
                match arrow_type_for_pg_oid(oid) {
                    Some(dt) => dt,
                    None => {
                        errors.push(format!(
                            "column '{}' has type {} (oid {oid}), which Phase 1 does not support; \
                             cast it in the query (e.g. {0}::text) or exclude it",
                            c.name(),
                            pg_type_name(oid)
                        ));
                        continue;
                    }
                }
            };
            cols.push(PgColumn {
                name: c.name().to_string(),
                oid,
                arrow,
            });
        }
        if !errors.is_empty() {
            return Err(LdpError::Config(errors.join("\n")));
        }
        if cols.is_empty() {
            return Err(LdpError::Config(
                "source query returns no columns".to_string(),
            ));
        }

        let scale = self.sample_decimal_scale(&cols).await?;
        for c in &mut cols {
            if is_decimal_oid(c.oid) {
                c.arrow = DataType::Decimal128(DECIMAL_PRECISION, scale);
            }
        }
        let _ = self.decimal_scale.set(scale);

        let columns = Arc::new(cols);
        let _ = self.columns.set(Arc::clone(&columns));
        Ok(columns)
    }

    /// Bounded sample to choose a decimal scale. Chosen over guessing because
    /// the contract forbids silent truncation.
    async fn sample_decimal_scale(&self, cols: &[PgColumn]) -> Result<i8, LdpError> {
        let numeric: Vec<&PgColumn> = cols.iter().filter(|c| is_decimal_oid(c.oid)).collect();
        if numeric.is_empty() {
            return Ok(0);
        }
        let mut max_scale: i8 = 0;
        for c in numeric {
            let sql = format!(
                "SELECT \"{}\"::text FROM ({}) AS ldp_src WHERE \"{}\" IS NOT NULL LIMIT 500",
                escape_ident(&c.name),
                self.wrapped_query(),
                escape_ident(&c.name)
            );
            let rows = self.client.query(&sql, &[]).await.map_err(|e| {
                LdpError::Source(format!("cannot sample numeric '{}': {e}", c.name))
            })?;
            for row in rows {
                if let Ok(Some(text)) = row.try_get::<_, Option<String>>(0) {
                    if let Some((_, frac)) = text.split_once('.') {
                        let digits = frac.trim_end_matches('0').len() as i8;
                        max_scale = max_scale.max(digits);
                    }
                }
            }
        }
        // Cap at 18: plenty for money and rates, and keeps 20 integer digits
        // plus scale inside i128.
        Ok(max_scale.clamp(0, 18))
    }

    fn partition_column<'a>(&self, cols: &'a [PgColumn]) -> Result<&'a PgColumn, LdpError> {
        let name = self.spec.partitioning.column.as_deref().ok_or_else(|| {
            LdpError::Config("partitioning.column is required for type range".to_string())
        })?;
        let col = cols.iter().find(|c| c.name == name).ok_or_else(|| {
            LdpError::Config(format!(
                "partition column '{name}' is not in the source query's output columns: [{}]",
                cols.iter()
                    .map(|c| c.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        })?;
        if !matches!(col.arrow, DataType::Int32 | DataType::Int64) {
            return Err(LdpError::Config(format!(
                "partition column '{name}' must be an integer type; found {}",
                col.arrow
            )));
        }
        Ok(col)
    }
}

/// Column list for the read query, casting numerics to text so they can be
/// carried as Decimal128 without a native numeric decoder.
fn select_list(cols: &[PgColumn]) -> String {
    cols.iter()
        .map(|c| {
            if is_decimal_oid(c.oid) {
                format!(
                    "\"{}\"::text AS \"{}\"",
                    escape_ident(&c.name),
                    escape_ident(&c.name)
                )
            } else {
                format!("\"{}\"", escape_ident(&c.name))
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

#[async_trait]
impl Source for PostgresSource {
    async fn schema(&self) -> Result<SchemaRef, LdpError> {
        if let Some(s) = self.schema.get() {
            return Ok(Arc::clone(s));
        }
        let cols = self.load_columns().await?;
        let fields: Vec<Field> = cols
            .iter()
            .map(|c| Field::new(&c.name, c.arrow.clone(), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let _ = self.schema.set(Arc::clone(&schema));
        Ok(schema)
    }

    async fn plan_partitions(&self, spec: &PartitionSpec) -> Result<Vec<Partition>, LdpError> {
        match spec.kind {
            PartitionKind::None => Ok(vec![Partition::single(0)]),
            PartitionKind::Range => {
                let cols = self.load_columns().await?;
                let col = self.partition_column(&cols)?;
                let name = &col.name;
                let chunks = spec.chunks.unwrap_or(8).max(1);
                let sql = format!(
                    "SELECT MIN(\"{0}\"), MAX(\"{0}\") FROM ({1}) AS ldp_src",
                    escape_ident(name),
                    self.wrapped_query()
                );
                let row = self.client.query_one(&sql, &[]).await.map_err(|e| {
                    LdpError::Source(format!("partition planning query failed: {e}"))
                })?;

                // MIN/MAX keep the column's width: int4 â†’ i32, int8 â†’ i64.
                let bounds: (Option<i64>, Option<i64>) = match col.arrow {
                    DataType::Int32 => (
                        row.try_get::<_, Option<i32>>(0)
                            .map_err(|e| LdpError::Source(e.to_string()))?
                            .map(|v| v as i64),
                        row.try_get::<_, Option<i32>>(1)
                            .map_err(|e| LdpError::Source(e.to_string()))?
                            .map(|v| v as i64),
                    ),
                    _ => (
                        row.try_get::<_, Option<i64>>(0)
                            .map_err(|e| LdpError::Source(e.to_string()))?,
                        row.try_get::<_, Option<i64>>(1)
                            .map_err(|e| LdpError::Source(e.to_string()))?,
                    ),
                };

                match bounds {
                    (None, _) | (_, None) => Ok(Vec::new()),
                    (Some(min), Some(max)) => Ok(ldp_core::plan_range(min, max, chunks)),
                }
            }
        }
    }

    async fn read_partition(&self, partition: &Partition) -> Result<BatchStream, LdpError> {
        let cols = self.load_columns().await?;
        let select = select_list(&cols);
        let mut sql = format!("SELECT {select} FROM ({}) AS ldp_src", self.wrapped_query());

        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
        let start_v: i64;
        let end_v: i64;
        if let (Some(start), Some(end)) = (partition.start, partition.end) {
            let name = self
                .spec
                .partitioning
                .column
                .as_deref()
                .ok_or_else(|| LdpError::Config("partition column missing".into()))?;
            sql.push_str(&format!(
                " WHERE \"{}\" >= $1 AND \"{}\" < $2",
                escape_ident(name),
                escape_ident(name)
            ));
            start_v = start;
            end_v = end;
            params.push(&start_v);
            params.push(&end_v);
        }

        let stream = self
            .client
            .query_raw(&sql, params)
            .await
            .map_err(|e| LdpError::Source(format!("cannot read partition: {e}")))?;

        let cols_for_task = Arc::clone(&cols);
        let scale = *self.decimal_scale.get().unwrap_or(&0);
        let max_rows = self.settings.max_batch_rows.max(1);
        let max_bytes = self.settings.max_batch_bytes.max(1024);

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<RecordBatch, LdpError>>(4);
        tokio::spawn(async move {
            let mut builders = Builders::new(&cols_for_task, scale);
            let mut rows_in_batch = 0usize;
            let mut bytes_in_batch = 0usize;
            // `RowStream` is `!Unpin` (it pins its own state), so pin it before
            // driving it with StreamExt::next.
            let mut stream = Box::pin(stream);
            while let Some(item) = stream.next().await {
                let row: Row = match item {
                    Ok(r) => r,
                    Err(e) => {
                        let _ = tx.send(Err(LdpError::Source(e.to_string()))).await;
                        return;
                    }
                };
                if let Err(e) = builders.push(&row) {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
                rows_in_batch += 1;
                bytes_in_batch += builders.approx_row_bytes(&row);
                if rows_in_batch >= max_rows || bytes_in_batch >= max_bytes {
                    match builders.take_batch() {
                        Ok(b) => {
                            if tx.send(Ok(b)).await.is_err() {
                                return;
                            }
                        }
                        Err(e) => {
                            let _ = tx.send(Err(e)).await;
                            return;
                        }
                    }
                    rows_in_batch = 0;
                    bytes_in_batch = 0;
                }
            }
            // Flush the tail, even when empty, so the sink always sees its
            // schema and can create a valid empty file (DoD #4).
            match builders.take_batch() {
                Ok(b) => {
                    let _ = tx.send(Ok(b)).await;
                }
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                }
            }
        });

        // Adapt the channel into the BoxStream the trait wants.
        let stream = futures_util::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        });
        Ok(Box::pin(stream))
    }
}

// ---------------------------------------------------------------------------
// Row â†’ Arrow builders
// ---------------------------------------------------------------------------

enum Builder {
    Int32(Int32Builder),
    Int64(Int64Builder),
    F64(Float64Builder),
    Utf8(StringBuilder),
    Bool(BooleanBuilder),
    Ts(TimestampMicrosecondBuilder),
    Date(Date32Builder),
    Decimal(Decimal128Builder),
}

struct Builders {
    cols: Arc<Vec<PgColumn>>,
    scale: i8,
    builders: Vec<Builder>,
    schema: SchemaRef,
    batch_rows: usize,
}

impl Builders {
    fn new(cols: &Arc<Vec<PgColumn>>, scale: i8) -> Self {
        let builders = cols
            .iter()
            .map(|c| match c.arrow {
                DataType::Int32 => Builder::Int32(Int32Builder::new()),
                DataType::Int64 => Builder::Int64(Int64Builder::new()),
                DataType::Float64 => Builder::F64(Float64Builder::new()),
                DataType::Utf8 => Builder::Utf8(StringBuilder::new()),
                DataType::Boolean => Builder::Bool(BooleanBuilder::new()),
                DataType::Timestamp(TimeUnit::Microsecond, None) => {
                    Builder::Ts(TimestampMicrosecondBuilder::new())
                }
                DataType::Date32 => Builder::Date(Date32Builder::new()),
                DataType::Decimal128(p, s) => Builder::Decimal(
                    Decimal128Builder::new()
                        .with_precision_and_scale(p, s)
                        .unwrap_or_else(|_| Decimal128Builder::new()),
                ),
                _ => Builder::Utf8(StringBuilder::new()),
            })
            .collect();

        let fields: Vec<Field> = cols
            .iter()
            .map(|c| Field::new(&c.name, c.arrow.clone(), true))
            .collect();
        Self {
            cols: Arc::clone(cols),
            scale,
            builders,
            schema: Arc::new(Schema::new(fields)),
            batch_rows: 0,
        }
    }

    fn push(&mut self, row: &Row) -> Result<(), LdpError> {
        for (i, col) in self.cols.iter().enumerate() {
            let b = &mut self.builders[i];
            let err =
                |e: tokio_postgres::Error| LdpError::Source(format!("column '{}': {e}", col.name));
            match b {
                Builder::Int32(x) => match row.try_get::<_, Option<i32>>(i) {
                    Ok(v) => x.append_option(v),
                    // INT2 arrives as i16.
                    Err(_) => match row.try_get::<_, Option<i16>>(i) {
                        Ok(v) => x.append_option(v.map(|v| v as i32)),
                        Err(e) => return Err(err(e)),
                    },
                },
                Builder::Int64(x) => x.append_option(row.try_get(i).map_err(err)?),
                Builder::F64(x) => x.append_option(row.try_get(i).map_err(err)?),
                Builder::Utf8(x) => {
                    x.append_option(row.try_get::<_, Option<&str>>(i).map_err(err)?)
                }
                Builder::Bool(x) => x.append_option(row.try_get(i).map_err(err)?),
                Builder::Ts(x) => {
                    let v: Option<chrono::NaiveDateTime> = row.try_get(i).map_err(err)?;
                    x.append_option(v.map(|d| d.and_utc().timestamp_micros()));
                }
                Builder::Date(x) => {
                    let v: Option<chrono::NaiveDate> = row.try_get(i).map_err(err)?;
                    x.append_option(v.map(|d| {
                        d.signed_duration_since(
                            chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch"),
                        )
                        .num_days() as i32
                    }));
                }
                Builder::Decimal(x) => {
                    let text: Option<&str> = row.try_get(i).map_err(err)?;
                    match text {
                        None => x.append_null(),
                        Some(t) => match decimal_from_str(t, DECIMAL_PRECISION, self.scale) {
                            Some(v) => x.append_value(v),
                            None => {
                                return Err(LdpError::Transform(format!(
                                    "column '{}' value '{t}' does not fit decimal(38,{}) without \
                                     truncation; cast it in the query or reduce decimal places",
                                    col.name, self.scale
                                )))
                            }
                        },
                    }
                }
            }
        }
        self.batch_rows += 1;
        Ok(())
    }

    /// Rough per-row size, only used to cap batch bytes.
    fn approx_row_bytes(&self, row: &Row) -> usize {
        let mut n = 0usize;
        for (i, col) in self.cols.iter().enumerate() {
            n += match col.arrow {
                DataType::Utf8 => row
                    .try_get::<_, Option<&str>>(i)
                    .ok()
                    .flatten()
                    .map(|s| s.len() + 4)
                    .unwrap_or(4),
                DataType::Decimal128(..) => 24,
                DataType::Timestamp(..) | DataType::Int64 | DataType::Date32 => 12,
                _ => 8,
            };
        }
        n
    }

    fn take_batch(&mut self) -> Result<RecordBatch, LdpError> {
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.builders.len());
        for b in &mut self.builders {
            arrays.push(match b {
                Builder::Int32(x) => Arc::new(x.finish()),
                Builder::Int64(x) => Arc::new(x.finish()),
                Builder::F64(x) => Arc::new(x.finish()),
                Builder::Utf8(x) => Arc::new(x.finish()),
                Builder::Bool(x) => Arc::new(x.finish()),
                Builder::Ts(x) => Arc::new(x.finish()),
                Builder::Date(x) => Arc::new(x.finish()),
                Builder::Decimal(x) => Arc::new(x.finish()),
            });
        }
        self.batch_rows = 0;
        RecordBatch::try_new(Arc::clone(&self.schema), arrays)
            .map_err(|e| LdpError::Transform(format!("cannot build batch: {e}")))
    }
}

/// Double-quote an identifier for generated SQL.
fn escape_ident(name: &str) -> String {
    name.replace('"', "\"\"")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, oid: u32, arrow: DataType) -> PgColumn {
        PgColumn {
            name: name.into(),
            oid,
            arrow,
        }
    }

    #[test]
    fn identifiers_are_quoted_safely() {
        assert_eq!(escape_ident("id"), "id");
        assert_eq!(escape_ident("weird\"name"), "weird\"\"name");
    }

    #[test]
    fn select_list_casts_numerics_to_text() {
        let cols = vec![
            col("id", ldp_arrow::pg_oid::INT8, DataType::Int64),
            col(
                "amount",
                ldp_arrow::pg_oid::NUMERIC,
                DataType::Decimal128(38, 2),
            ),
        ];
        assert_eq!(select_list(&cols), "\"id\", \"amount\"::text AS \"amount\"");
    }
}
