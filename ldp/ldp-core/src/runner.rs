//! The partitioned worker pool (contract §7, operational details §1).
//!
//! Shape: N workers pull partitions from a shared queue, read → transform, and
//! push batches into a bounded channel. One writer task owns the sinks, so
//! writes are serialised (Parquet/CSV writers are not thread-safe) while reads
//! and transforms run in parallel. The checkpoint is updated only after every
//! sink has accepted the chunk — that is the whole idempotency story.
//!
//! Backpressure comes from the bounded channel: a slow sink stalls the workers
//! instead of growing an unbounded queue in memory.

use crate::checkpoint::PipelineState;
use crate::chunk::DataChunk;
use crate::error::LdpError;
use crate::log::Logger;
use crate::plan::{ExecutionPlan, SinkSpec, TransformSpec};
use crate::traits::{Sink, Source, Transform};
use crate::transform::{DeriveTransform, FilterTransform};
use arrow::record_batch::RecordBatch;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// A sink plus the YAML spec it came from (for summaries and logs).
pub struct SinkBinding {
    pub sink: Arc<dyn Sink>,
    pub spec: SinkSpec,
}

impl SinkBinding {
    pub fn new(sink: Arc<dyn Sink>, spec: SinkSpec) -> Self {
        Self { sink, spec }
    }
}

#[derive(Debug, Default, Clone)]
pub struct RunReport {
    pub status: String,
    pub rows_read: u64,
    pub rows_written: u64,
    pub dropped_rows: u64,
    pub chunks_total: usize,
    pub chunks_completed: usize,
    pub chunks_skipped: usize,
    pub duration: Duration,
    pub error: Option<String>,
    pub target_rows: Vec<(String, u64)>,
    /// Rows the engine recounted from the published output, per target, in the
    /// same order as the sinks. `None` = that sink cannot verify itself.
    pub verified: Vec<Option<u64>>,
}

/// Outcome of the post-run integrity check.
struct Verification {
    per_target: Vec<Option<u64>>,
    error: Option<LdpError>,
}

enum Msg {
    Chunk(DataChunk),
    ChunkDone {
        chunk_id: u64,
        rows: u64,
        dropped: u64,
        elapsed: Duration,
    },
    ChunkFailed {
        chunk_id: u64,
        error: LdpError,
    },
}

/// Test hook: slow every batch down so a mid-run kill can be timed reliably.
/// Documented in HANDOFF.md; it changes no semantics.
fn throttle() -> Duration {
    std::env::var("LDP_THROTTLE_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or_default()
}

/// Test hook: abort after N completed chunks, to prove resume works without
/// racing a kill. Only ever set by the integration tests.
fn fail_after_chunks() -> Option<usize> {
    std::env::var("LDP_FAIL_AFTER_CHUNKS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
}

pub async fn run(
    plan: &ExecutionPlan,
    source: Arc<dyn Source>,
    sinks: Vec<SinkBinding>,
    logger: &Logger,
) -> Result<RunReport, LdpError> {
    let started = Instant::now();
    let mut report = RunReport {
        status: "failed".into(),
        ..Default::default()
    };

    // ---- build the transform chain -------------------------------------
    let order = plan.chain_order()?;
    let mut transforms: Vec<Arc<dyn Transform>> = Vec::with_capacity(order.len());
    for idx in &order {
        match &plan.transformations[*idx] {
            TransformSpec::Filter {
                id,
                condition,
                predicate,
                ..
            } => transforms.push(Arc::new(FilterTransform::new(
                id.clone(),
                condition.clone(),
                predicate.clone(),
            ))),
            TransformSpec::Derive { id, fields, .. } => {
                transforms.push(Arc::new(DeriveTransform::new(id.clone(), fields.clone())))
            }
        }
    }

    // ---- schema, before any row moves -----------------------------------
    let mut schema = source.schema().await?;
    for t in &transforms {
        schema = t.output_schema(schema.clone())?;
    }

    // Sinks are opened even when there is nothing to write, so an empty run
    // still produces a valid (empty) Parquet file — DoD #4.
    for b in &sinks {
        b.sink
            .open(schema.clone())
            .await
            .map_err(|e| LdpError::Sink(format!("target '{}': {e}", b.spec.id)))?;
    }

    // ---- planning --------------------------------------------------------
    let partitions = source.plan_partitions(&plan.source.partitioning).await?;
    report.chunks_total = partitions.len();

    // ---- checkpoint ------------------------------------------------------
    let state_path = PathBuf::from(&plan.settings.state_file);
    let mut state = PipelineState::load(&state_path, &plan.name)?;
    let resumable = sinks.iter().all(|b| b.sink.resumable());
    if !resumable && state.completed_count() > 0 {
        logger.warn(
            "checkpoint_ignored",
            "a target cannot resume from a checkpoint (single-file output); \
             replaying every partition and rewriting the file atomically",
        );
        state.completed_chunks.clear();
    }

    let queue: VecDeque<_> = partitions
        .iter()
        .filter(|p| !state.is_completed(p.id))
        .cloned()
        .collect();
    report.chunks_skipped = partitions.len() - queue.len();
    if report.chunks_skipped > 0 {
        logger.info(
            "resume",
            &format!(
                "skipping {} already-completed partition(s) from {}",
                report.chunks_skipped,
                state_path.display()
            ),
        );
    }

    // Nothing left to do: finalize (writes an empty file if there is no data)
    // and report success.
    if queue.is_empty() {
        for b in &sinks {
            b.sink
                .finalize()
                .await
                .map_err(|e| LdpError::Sink(format!("target '{}': {e}", b.spec.id)))?;
        }
        let check = verify_sinks(&sinks, 0, logger).await;
        report.verified = check.per_target;
        report.chunks_completed = report.chunks_skipped;
        report.duration = started.elapsed();
        match check.error {
            None => {
                report.status = "success".into();
                emit_summary(logger, &report, &sinks);
                return Ok(report);
            }
            Some(e) => {
                report.status = "failed".into();
                report.error = Some(e.to_string());
                emit_summary(logger, &report, &sinks);
                return Err(e);
            }
        }
    }

    // ---- workers ---------------------------------------------------------
    let workers = plan.settings.max_threads.max(1).min(queue.len().max(1));
    let channel_bound = (plan.settings.max_threads * 2).max(2);
    let (tx, mut rx) = mpsc::channel::<Msg>(channel_bound);
    let queue = Arc::new(Mutex::new(queue));
    let rows_read = Arc::new(AtomicU64::new(0));
    let dropped_total = Arc::new(AtomicU64::new(0));
    let pause = throttle();

    let mut handles = Vec::with_capacity(workers);
    for _ in 0..workers {
        let tx = tx.clone();
        let queue = Arc::clone(&queue);
        let source = Arc::clone(&source);
        let transforms = transforms.clone();
        let logger = logger.clone();
        let rows_read = Arc::clone(&rows_read);
        let dropped_total = Arc::clone(&dropped_total);
        handles.push(tokio::spawn(async move {
            loop {
                let next = {
                    let mut q = queue.lock().unwrap_or_else(|e| e.into_inner());
                    q.pop_front()
                };
                let Some(partition) = next else { break };
                let chunk_id = partition.id;
                let chunk_started = Instant::now();
                logger.chunk_started(chunk_id, partition.start, partition.end);

                // Partition-level retry with exponential backoff (addendum §6):
                // transient failures are retried, the partition is only failed
                // once attempts are exhausted.
                let mut attempt = 0u32;
                let stream = loop {
                    match source.read_partition(&partition).await {
                        Ok(s) => break Some(s),
                        Err(e) => {
                            attempt += 1;
                            if attempt >= 3 {
                                let _ = tx.send(Msg::ChunkFailed { chunk_id, error: e }).await;
                                return;
                            }
                            logger.warn(
                                "chunk_retry",
                                &format!("chunk {chunk_id} attempt {attempt} failed: {e}"),
                            );
                            tokio::time::sleep(Duration::from_millis(100 * 2u64.pow(attempt)))
                                .await;
                        }
                    }
                };
                let Some(mut stream) = stream else { return };

                use futures_util::StreamExt;
                let mut chunk_rows = 0u64;
                let mut chunk_dropped = 0u64;
                while let Some(item) = stream.next().await {
                    let batch: RecordBatch = match item {
                        Ok(b) => b,
                        Err(e) => {
                            let _ = tx.send(Msg::ChunkFailed { chunk_id, error: e }).await;
                            return;
                        }
                    };
                    rows_read.fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                    let mut chunk = DataChunk::new(chunk_id, batch);
                    let mut failed = None;
                    for t in &transforms {
                        match t.apply(&chunk) {
                            Ok(tr) => {
                                chunk_dropped += tr.dropped_rows as u64;
                                chunk = tr.chunk;
                            }
                            Err(e) => {
                                failed = Some(e);
                                break;
                            }
                        }
                    }
                    if let Some(error) = failed {
                        let _ = tx.send(Msg::ChunkFailed { chunk_id, error }).await;
                        return;
                    }
                    chunk_rows += chunk.num_rows() as u64;
                    if tx.send(Msg::Chunk(chunk)).await.is_err() {
                        return; // writer went away
                    }
                    if !pause.is_zero() {
                        tokio::time::sleep(pause).await;
                    }
                }
                dropped_total.fetch_add(chunk_dropped, Ordering::Relaxed);
                if tx
                    .send(Msg::ChunkDone {
                        chunk_id,
                        rows: chunk_rows,
                        dropped: chunk_dropped,
                        elapsed: chunk_started.elapsed(),
                    })
                    .await
                    .is_err()
                {
                    return;
                }
            }
        }));
    }
    drop(tx);

    // ---- writer ----------------------------------------------------------
    let mut rows_written = 0u64;
    let mut completed = 0usize;
    let mut error: Option<LdpError> = None;

    while let Some(msg) = rx.recv().await {
        match msg {
            Msg::Chunk(chunk) => {
                let mut ok = true;
                for b in &sinks {
                    if let Err(e) = b.sink.write_chunk(&chunk).await {
                        error = Some(LdpError::Sink(format!("target '{}': {e}", b.spec.id)));
                        ok = false;
                        break;
                    }
                }
                if !ok {
                    break;
                }
                rows_written += chunk.num_rows() as u64;
            }
            Msg::ChunkDone {
                chunk_id,
                rows,
                dropped,
                elapsed,
            } => {
                // Checkpoint only after every sink accepted every batch. This
                // is written even for sinks that cannot resume (csv): the state
                // file is also the run's audit record, and the loader is what
                // decides whether to trust it.
                state.mark_completed(chunk_id);
                if let Err(e) = state.save(&state_path) {
                    error = Some(e);
                    break;
                }
                completed += 1;
                report.dropped_rows += dropped;
                logger.chunk_completed(chunk_id, rows as usize, elapsed);

                if let Some(limit) = fail_after_chunks() {
                    if completed >= limit {
                        error = Some(LdpError::Source(format!(
                            "test hook LDP_FAIL_AFTER_CHUNKS={limit} reached"
                        )));
                        break;
                    }
                }
            }
            Msg::ChunkFailed { chunk_id, error: e } => {
                logger.error(Some(chunk_id), &e.to_string());
                error = Some(e);
                break;
            }
        }
    }

    // Stop workers: closing the receiver makes their sends fail.
    drop(rx);
    for h in handles {
        let _ = h.await;
    }

    report.rows_read = rows_read.load(Ordering::Relaxed);
    report.rows_written = rows_written;
    report.chunks_completed = completed;
    report.duration = started.elapsed();

    match error {
        None => {
            for b in &sinks {
                b.sink
                    .finalize()
                    .await
                    .map_err(|e| LdpError::Sink(format!("target '{}': {e}", b.spec.id)))?;
            }
            // Post-condition: the engine must agree with what is on disk. This
            // is what turns "silently short" into exit code 2.
            let check = verify_sinks(&sinks, rows_written, logger).await;
            report.verified = check.per_target;
            match check.error {
                None => {
                    report.status = "success".into();
                    emit_summary(logger, &report, &sinks);
                    Ok(report)
                }
                Some(e) => {
                    report.status = "failed".into();
                    report.error = Some(e.to_string());
                    emit_summary(logger, &report, &sinks);
                    Err(e)
                }
            }
        }
        Some(e) => {
            // Failed runs publish nothing: temp files are removed so the next
            // run cannot mistake debris for output (addendum §10).
            for b in &sinks {
                if let Err(abort_err) = b.sink.abort().await {
                    logger.warn(
                        "abort_failed",
                        &format!("target '{}': {abort_err}", b.spec.id),
                    );
                }
            }
            if !resumable {
                state.completed_chunks.clear();
                let _ = state.save(&state_path);
            }
            let _ = state.save(&state_path);
            report.error = Some(e.to_string());
            report.status = "failed".into();
            emit_summary(logger, &report, &sinks);
            Err(e)
        }
    }
}

/// Ask every sink to recount what it published and compare with the runner's
/// own tally. A mismatch is a failed run, not a warning: the engine's contract
/// is that the rows it reports are the rows that exist.
async fn verify_sinks(sinks: &[SinkBinding], expected: u64, logger: &Logger) -> Verification {
    let mut per_target: Vec<Option<u64>> = Vec::with_capacity(sinks.len());
    let mut error: Option<LdpError> = None;

    for b in sinks {
        match b.sink.verify_rows().await {
            Ok(Some(actual)) if actual != expected => {
                let msg = format!(
                    "output verification failed for target '{}': the engine wrote {expected} rows \
                     but {actual} are on disk (partial write, an overwritten file, or a sink bug)",
                    b.spec.id
                );
                logger.error(None, &msg);
                if error.is_none() {
                    error = Some(LdpError::Sink(msg));
                }
                per_target.push(Some(actual));
            }
            Ok(Some(actual)) => {
                logger.info(
                    "output_verified",
                    &format!("target '{}': {actual} rows verified on disk", b.spec.id),
                );
                per_target.push(Some(actual));
            }
            Ok(None) => per_target.push(None),
            Err(e) => {
                logger.error(None, &e.to_string());
                if error.is_none() {
                    error = Some(LdpError::Sink(format!(
                        "cannot verify target '{}': {e}",
                        b.spec.id
                    )));
                }
                per_target.push(None);
            }
        }
    }

    Verification { per_target, error }
}

fn emit_summary(logger: &Logger, report: &RunReport, sinks: &[SinkBinding]) {
    let per_target: Vec<(SinkSpec, u64, Option<u64>)> = sinks
        .iter()
        .enumerate()
        .map(|(i, b)| {
            (
                b.spec.clone(),
                report.rows_written,
                report.verified.get(i).copied().flatten(),
            )
        })
        .collect();
    logger.job_summary(
        &report.status,
        report.chunks_total,
        report.chunks_completed,
        report.chunks_skipped,
        report.rows_read,
        report.rows_written,
        report.dropped_rows,
        report.duration,
        report.error.as_deref(),
        &per_target,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::LogLevel;
    use crate::partition::Partition;
    use crate::plan::{Compression, PartitionSpec, Settings, SinkKind, SourceSpec};
    use crate::traits::BatchStream;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use async_trait::async_trait;
    use std::sync::Mutex as StdMutex;

    struct MockSource {
        schema: SchemaRef,
        batches_per_partition: usize,
        rows_per_batch: usize,
    }

    #[async_trait]
    impl Source for MockSource {
        async fn schema(&self) -> Result<SchemaRef, LdpError> {
            Ok(Arc::clone(&self.schema))
        }

        async fn plan_partitions(&self, _spec: &PartitionSpec) -> Result<Vec<Partition>, LdpError> {
            Ok(vec![Partition::single(0), Partition::single(1)])
        }

        async fn read_partition(&self, p: &Partition) -> Result<BatchStream, LdpError> {
            let mut out: Vec<Result<RecordBatch, LdpError>> = Vec::new();
            for b in 0..self.batches_per_partition {
                let base =
                    ((p.id as usize * self.batches_per_partition + b) * self.rows_per_batch) as i64;
                let values: Vec<i64> = (0..self.rows_per_batch as i64).map(|i| base + i).collect();
                let batch = RecordBatch::try_new(
                    Arc::clone(&self.schema),
                    vec![Arc::new(Int64Array::from(values))],
                )
                .expect("mock batch");
                out.push(Ok(batch));
            }
            Ok(Box::pin(futures_util::stream::iter(out)))
        }
    }

    struct MockSink {
        rows: StdMutex<Vec<usize>>,
        /// When set, verification deliberately reports this instead of the truth.
        lie: Option<u64>,
    }

    impl MockSink {
        fn new(lie: Option<u64>) -> Self {
            Self {
                rows: StdMutex::new(Vec::new()),
                lie,
            }
        }
        fn batches(&self) -> usize {
            self.rows.lock().unwrap().len()
        }
    }

    #[async_trait]
    impl Sink for MockSink {
        async fn open(&self, _schema: SchemaRef) -> Result<(), LdpError> {
            Ok(())
        }
        async fn write_chunk(&self, chunk: &DataChunk) -> Result<(), LdpError> {
            self.rows.lock().unwrap().push(chunk.num_rows());
            Ok(())
        }
        async fn finalize(&self) -> Result<(), LdpError> {
            Ok(())
        }
        async fn verify_rows(&self) -> Result<Option<u64>, LdpError> {
            if let Some(v) = self.lie {
                return Ok(Some(v));
            }
            let total: usize = self.rows.lock().unwrap().iter().sum();
            Ok(Some(total as u64))
        }
    }

    fn plan(dir: &std::path::Path) -> ExecutionPlan {
        ExecutionPlan {
            name: "mock".into(),
            settings: Settings {
                max_threads: 2,
                state_file: dir.join("state.json").to_string_lossy().to_string(),
                max_batch_rows: 1000,
                max_batch_bytes: 1_000_000,
            },
            connections: vec![],
            source: SourceSpec {
                id: "src".into(),
                connection: "pg".into(),
                query: "SELECT 1".into(),
                partitioning: PartitionSpec::none(),
            },
            transformations: vec![],
            targets: vec![SinkSpec {
                id: "out".into(),
                input: "src".into(),
                kind: SinkKind::Parquet,
                path: dir.join("out").to_string_lossy().to_string(),
                compression: Compression::None,
            }],
        }
    }

    fn source() -> Arc<dyn Source> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        Arc::new(MockSource {
            schema,
            batches_per_partition: 2,
            rows_per_batch: 2,
        })
    }

    #[tokio::test]
    async fn every_batch_of_every_partition_arrives_and_is_verified() {
        let dir = tempfile::tempdir().unwrap();
        let plan = plan(dir.path());
        let sink = Arc::new(MockSink::new(None));
        let bindings = vec![SinkBinding::new(sink.clone(), plan.targets[0].clone())];

        let report = run(
            &plan,
            source(),
            bindings,
            &Logger::new("t", LogLevel::Error),
        )
        .await
        .expect("run");

        // 2 partitions x 2 batches x 2 rows
        assert_eq!(report.rows_written, 8);
        assert_eq!(report.chunks_completed, 2);
        assert_eq!(sink.batches(), 4, "no batch may be dropped");
        assert_eq!(report.verified, vec![Some(8)]);
        assert_eq!(report.status, "success");
    }

    #[tokio::test]
    async fn a_sink_that_cannot_account_for_its_rows_fails_the_run() {
        // This is the guard for the original bug: the sink claims fewer rows on
        // disk than the engine handed it. It must be a failed run (exit 2), not
        // a cheerful summary.
        let dir = tempfile::tempdir().unwrap();
        let plan = plan(dir.path());
        let sink = Arc::new(MockSink::new(Some(3)));
        let bindings = vec![SinkBinding::new(sink, plan.targets[0].clone())];

        let err = run(
            &plan,
            source(),
            bindings,
            &Logger::new("t", LogLevel::Error),
        )
        .await
        .expect_err("verification mismatch must fail the run");
        assert_eq!(err.exit_code(), 2, "runtime failure, not configuration");
        assert!(
            err.to_string().contains("output verification failed"),
            "{err}"
        );
    }
}
