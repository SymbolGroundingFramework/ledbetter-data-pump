//! `ldp-config` — the control plane: read `pipeline.yaml`, validate it
//! strictly, and produce one immutable [`ExecutionPlan`].
//!
//! Everything an operator can get wrong is caught here, before a single query
//! is issued, and **all** problems are reported together (addendum §13) rather
//! than one per run.

use ldp_core::expr;
use ldp_core::plan::{
    Compression, ConnectionSpec, DeriveField, ExecutionPlan, PartitionKind, PartitionSpec,
    Settings, SinkKind, SinkSpec, SourceSpec, TransformSpec,
};
use ldp_core::{LdpError, ValidationErrors};
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

// ---------------------------------------------------------------------------
// YAML shape (strict: unknown fields are an error, not silently ignored)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct YamlRoot {
    pub version: String,
    pub pipeline: YamlPipeline,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct YamlPipeline {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub settings: Option<YamlSettings>,
    pub resources: YamlResources,
    pub dataflow: YamlDataflow,
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct YamlSettings {
    pub max_threads: Option<usize>,
    pub state_file: Option<String>,
    pub max_batch_rows: Option<usize>,
    pub max_batch_bytes: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct YamlResources {
    pub connections: Vec<YamlConnection>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct YamlConnection {
    pub name: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub url: String,
    #[serde(default)]
    pub pool_size: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct YamlDataflow {
    pub sources: Vec<YamlSource>,
    #[serde(default)]
    pub transformations: Vec<YamlTransform>,
    pub targets: Vec<YamlTarget>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct YamlSource {
    pub id: String,
    pub connection: String,
    pub query: String,
    #[serde(default)]
    pub partitioning: Option<YamlPartitioning>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct YamlPartitioning {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub column: Option<String>,
    #[serde(default)]
    pub chunks: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum YamlTransform {
    Filter {
        id: String,
        input: String,
        condition: String,
    },
    Derive {
        id: String,
        input: String,
        fields: Vec<YamlDeriveField>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct YamlDeriveField {
    pub name: String,
    pub expression: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct YamlTarget {
    pub id: String,
    pub input: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub path: String,
    #[serde(default)]
    pub compression: Option<String>,
}

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct Loaded {
    pub plan: ExecutionPlan,
    /// Non-fatal observations (e.g. "compression is ignored for csv").
    pub warnings: Vec<String>,
}

/// Read and validate a pipeline file.
pub fn load(path: &Path) -> Result<Loaded, LdpError> {
    let text = std::fs::read_to_string(path).map_err(|e| {
        LdpError::Config(format!("cannot read pipeline file {}: {e}", path.display()))
    })?;
    load_str(&text).map_err(|e| match e {
        LdpError::Config(msg) => LdpError::Config(format!("{}: {msg}", path.display())),
        other => other,
    })
}

/// Validate already-read YAML (the path-free entry point used by tests).
pub fn load_str(text: &str) -> Result<Loaded, LdpError> {
    let root: YamlRoot = serde_yaml::from_str(text)
        .map_err(|e| LdpError::Config(format!("invalid pipeline YAML: {e}")))?;
    build(root)
}

fn build(root: YamlRoot) -> Result<Loaded, LdpError> {
    let mut errs = ValidationErrors::new();
    let mut warnings = Vec::new();

    if root.version != "1.0" {
        errs.push(format!(
            "version must be exactly \"1.0\" (found \"{}\")",
            root.version
        ));
    }
    let p = root.pipeline;
    if p.name.trim().is_empty() {
        errs.push("pipeline.name must not be empty".to_string());
    }

    // ---- connections -----------------------------------------------------
    let mut connections = Vec::new();
    let mut seen_conns = HashSet::new();
    for c in &p.resources.connections {
        if !seen_conns.insert(c.name.clone()) {
            errs.push(format!("duplicate connection name '{}'", c.name));
            continue;
        }
        if c.kind != "postgres" {
            errs.push(format!(
                "connection '{}' has type '{}'; Phase 1 supports only 'postgres'",
                c.name, c.kind
            ));
            continue;
        }
        match resolve_url(&c.url) {
            Ok(url) => {
                let mut options = BTreeMap::new();
                if let Some(n) = c.pool_size {
                    options.insert("pool_size".to_string(), n.to_string());
                }
                connections.push(ConnectionSpec {
                    name: c.name.clone(),
                    driver: c.kind.clone(),
                    url,
                    options,
                });
            }
            Err(msg) => errs.push(format!("connection '{}': {msg}", c.name)),
        }
    }
    if p.resources.connections.is_empty() {
        errs.push("resources.connections must contain at least one connection".to_string());
    }

    // ---- source ----------------------------------------------------------
    if p.dataflow.sources.len() != 1 {
        errs.push(format!(
            "Phase 1 supports exactly one source (found {})",
            p.dataflow.sources.len()
        ));
    }
    let source = p.dataflow.sources.first();
    let mut source_spec = None;
    if let Some(s) = source {
        if s.query.trim().is_empty() {
            errs.push("source.query must not be empty".to_string());
        }
        if !seen_conns.contains(&s.connection) {
            errs.push(format!(
                "source '{}' references unknown connection '{}'",
                s.id, s.connection
            ));
        }
        let partitioning = match &s.partitioning {
            None => PartitionSpec::none(),
            Some(y) => match y.kind.as_str() {
                "none" => PartitionSpec::none(),
                "range" => {
                    let column = y.column.clone().unwrap_or_default();
                    if column.trim().is_empty() {
                        errs.push(format!(
                            "source '{}': partitioning.column is required for type range",
                            s.id
                        ));
                    } else if !query_mentions_column(&s.query, &column) {
                        errs.push(format!(
                            "source '{}': partition column '{}' does not appear in the query \
                             (add it to the SELECT list)",
                            s.id, column
                        ));
                    }
                    match y.chunks {
                        Some(c) if c >= 1 => {}
                        Some(c) => errs.push(format!(
                            "source '{}': partitioning.chunks must be >= 1 (found {c})",
                            s.id
                        )),
                        None => errs.push(format!(
                            "source '{}': partitioning.chunks is required for type range",
                            s.id
                        )),
                    }
                    PartitionSpec {
                        kind: PartitionKind::Range,
                        column: Some(column),
                        chunks: y.chunks,
                    }
                }
                other => {
                    errs.push(format!(
                        "source '{}': partitioning.type must be 'range' or 'none' (found '{other}')",
                        s.id
                    ));
                    PartitionSpec::none()
                }
            },
        };
        source_spec = Some(SourceSpec {
            id: s.id.clone(),
            connection: s.connection.clone(),
            query: s.query.trim_end_matches(';').trim().to_string(),
            partitioning,
        });
    }

    // ---- transformations -------------------------------------------------
    let mut transformations = Vec::new();
    let mut transform_ids: HashMap<String, String> = HashMap::new(); // id -> input
    for t in &p.dataflow.transformations {
        match t {
            YamlTransform::Filter {
                id,
                input,
                condition,
            } => {
                if transform_ids.contains_key(id) {
                    errs.push(format!("duplicate stage id '{id}'"));
                    continue;
                }
                if condition.trim().is_empty() {
                    errs.push(format!("filter '{id}': condition must not be empty"));
                    continue;
                }
                match expr::parse(condition) {
                    Ok(predicate) => {
                        transform_ids.insert(id.clone(), input.clone());
                        transformations.push(TransformSpec::Filter {
                            id: id.clone(),
                            input: input.clone(),
                            condition: condition.clone(),
                            predicate,
                        });
                    }
                    Err(e) => errs.push(format!("filter '{id}': {}", e.detail())),
                }
            }
            YamlTransform::Derive { id, input, fields } => {
                if transform_ids.contains_key(id) {
                    errs.push(format!("duplicate stage id '{id}'"));
                    continue;
                }
                if fields.is_empty() {
                    errs.push(format!("derive '{id}': fields must not be empty"));
                    continue;
                }
                let mut seen = HashSet::new();
                let mut built = Vec::new();
                let mut ok = true;
                for f in fields {
                    if f.name.trim().is_empty() {
                        errs.push(format!("derive '{id}': field name must not be empty"));
                        ok = false;
                        continue;
                    }
                    if !seen.insert(f.name.clone()) {
                        errs.push(format!("derive '{id}': duplicate field name '{}'", f.name));
                        ok = false;
                        continue;
                    }
                    match expr::parse(&f.expression) {
                        Ok(e) => built.push(DeriveField {
                            source_text: f.expression.clone(),
                            name: f.name.clone(),
                            expr: e,
                        }),
                        Err(e) => {
                            errs.push(format!("derive '{id}' field '{}': {}", f.name, e.detail()));
                            ok = false;
                        }
                    }
                }
                if ok {
                    transform_ids.insert(id.clone(), input.clone());
                    transformations.push(TransformSpec::Derive {
                        id: id.clone(),
                        input: input.clone(),
                        fields: built,
                    });
                }
            }
        }
    }

    // ---- graph checks ----------------------------------------------------
    let source_id = source.map(|s| s.id.clone()).unwrap_or_default();
    let mut ids: HashSet<String> = HashSet::new();
    ids.insert(source_id.clone());
    for id in transform_ids.keys() {
        ids.insert(id.clone());
    }
    for (id, input) in &transform_ids {
        if !ids.contains(input) {
            errs.push(format!("stage '{id}' references unknown input '{input}'"));
        }
    }
    if let Some(cycle) = find_cycle(&transform_ids) {
        errs.push(format!(
            "cycle detected in dataflow: {}",
            cycle.join(" -> ")
        ));
    }

    // Phase 1 is one linear chain: at most one stage may consume each output.
    let mut consumers: HashMap<&str, Vec<&str>> = HashMap::new();
    for (id, input) in &transform_ids {
        consumers
            .entry(input.as_str())
            .or_default()
            .push(id.as_str());
    }
    for (producer, kids) in &consumers {
        if kids.len() > 1 {
            errs.push(format!(
                "stage '{}' feeds {} stages ({}); Phase 1 supports a single linear chain",
                producer,
                kids.len(),
                kids.join(", ")
            ));
        }
    }

    // ---- targets ---------------------------------------------------------
    let mut targets = Vec::new();
    let mut seen_targets = HashSet::new();
    let mut target_inputs: Vec<String> = Vec::new();
    for t in &p.dataflow.targets {
        if !seen_targets.insert(t.id.clone()) {
            errs.push(format!("duplicate target id '{}'", t.id));
            continue;
        }
        if !ids.contains(&t.input) {
            errs.push(format!(
                "target '{}' references unknown input '{}'",
                t.id, t.input
            ));
            continue;
        }
        target_inputs.push(t.input.clone());
        let kind = match t.kind.as_str() {
            "parquet" => SinkKind::Parquet,
            "csv" => SinkKind::Csv,
            other => {
                errs.push(format!(
                    "target '{}': type must be 'parquet' or 'csv' (found '{other}')",
                    t.id
                ));
                continue;
            }
        };
        if t.path.trim().is_empty() {
            errs.push(format!("target '{}': path must not be empty", t.id));
            continue;
        }
        let compression = match t.compression.as_deref() {
            None => Compression::Zstd,
            Some("zstd") => Compression::Zstd,
            Some("snappy") => Compression::Snappy,
            Some("none") => Compression::None,
            Some(other) => {
                errs.push(format!(
                    "target '{}': compression must be zstd|snappy|none (found '{other}')",
                    t.id
                ));
                Compression::Zstd
            }
        };
        if kind == SinkKind::Csv && t.compression.is_some() {
            warnings.push(format!(
                "target '{}': compression is ignored for csv output",
                t.id
            ));
        }
        targets.push(SinkSpec {
            id: t.id.clone(),
            input: t.input.clone(),
            kind,
            path: t.path.clone(),
            compression,
        });
    }
    if p.dataflow.targets.is_empty() {
        errs.push("dataflow.targets must contain at least one target".to_string());
    }
    if target_inputs.len() > 1 {
        let first = &target_inputs[0];
        if target_inputs.iter().any(|i| i != first) {
            errs.push(
                "all targets must read the same stage in Phase 1 \
                 (conditional split / fan-out is Phase 2)"
                    .to_string(),
            );
        }
    }

    // ---- settings --------------------------------------------------------
    let s = p.settings.unwrap_or_default();
    let max_threads = s.max_threads.unwrap_or(8);
    if max_threads == 0 {
        errs.push("settings.max_threads must be >= 1".to_string());
    }
    let state_file = s
        .state_file
        .unwrap_or_else(|| format!(".ldp/{}.json", p.name));
    let max_batch_rows = s.max_batch_rows.unwrap_or(65_536);
    if max_batch_rows == 0 {
        errs.push("settings.max_batch_rows must be >= 1".to_string());
    }
    let max_batch_bytes = s.max_batch_bytes.unwrap_or(128_000_000);
    if max_batch_bytes < 1024 {
        errs.push("settings.max_batch_bytes must be at least 1024".to_string());
    }

    errs.into_result()?;

    let Some(source) = source_spec else {
        return Err(LdpError::Config("no source defined".into()));
    };

    Ok(Loaded {
        plan: ExecutionPlan {
            name: p.name,
            settings: Settings {
                max_threads,
                state_file,
                max_batch_rows,
                max_batch_bytes,
            },
            connections,
            source,
            transformations,
            targets,
        },
        warnings,
    })
}

/// `env://VAR` â†’ the variable's value. Plain URLs pass through. Anything else
/// is a configuration error (Operational Details §5).
fn resolve_url(raw: &str) -> Result<String, String> {
    let raw = raw.trim();
    if let Some(var) = raw.strip_prefix("env://") {
        if var.is_empty() {
            return Err("url 'env://' names no variable".into());
        }
        match std::env::var(var) {
            Ok(v) if !v.trim().is_empty() => Ok(v),
            Ok(_) => Err(format!("environment variable {var} is set but empty")),
            Err(_) => Err(format!(
                "environment variable {var} is not set (referenced as env://{var})"
            )),
        }
    } else if raw.starts_with("postgres://") || raw.starts_with("postgresql://") {
        Ok(raw.to_string())
    } else {
        Err(format!(
            "url must be 'env://VARIABLE' or start with postgres:// (found '{}')",
            ldp_core::plan::redact_url(raw)
        ))
    }
}

/// Best-effort check that the partition column is part of the query
/// (Operational Details §1 assumes the column is selected). Not a SQL parser:
/// it looks for the identifier as a whole word, case-insensitively.
fn query_mentions_column(query: &str, column: &str) -> bool {
    let q = query.to_ascii_lowercase();
    let c = column.to_ascii_lowercase();
    if c.is_empty() {
        return false;
    }
    let mut start = 0usize;
    while let Some(pos) = q[start..].find(&c) {
        let abs = start + pos;
        let before_ok = abs == 0
            || (!q.as_bytes()[abs - 1].is_ascii_alphanumeric() && q.as_bytes()[abs - 1] != b'_');
        let after = abs + c.len();
        let after_ok = after >= q.len()
            || (!q.as_bytes()[after].is_ascii_alphanumeric() && q.as_bytes()[after] != b'_');
        if before_ok && after_ok {
            return true;
        }
        start = abs + c.len();
    }
    false
}

fn find_cycle(edges: &HashMap<String, String>) -> Option<Vec<String>> {
    // edges: stage -> its input. Walk each chain to exhaustion; a repeat is a cycle.
    for start in edges.keys() {
        let mut seen = Vec::new();
        let mut cur = start.clone();
        loop {
            if seen.contains(&cur) {
                seen.push(cur);
                return Some(seen);
            }
            seen.push(cur.clone());
            match edges.get(&cur) {
                Some(next) => cur = next.clone(),
                None => break,
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn good() -> String {
        std::env::set_var("LDP_TEST_PG", "postgres://u:p@localhost:55432/db");
        r#"
version: "1.0"
pipeline:
  name: "customer_export"
  settings:
    max_threads: 4
    state_file: ".ldp/customer_export.json"
  resources:
    connections:
      - name: "pg"
        type: "postgres"
        url: "env://LDP_TEST_PG"
  dataflow:
    sources:
      - id: "src"
        connection: "pg"
        query: "SELECT id, name, status, amount, created_at FROM customers"
        partitioning:
          type: "range"
          column: "id"
          chunks: 4
    transformations:
      - id: "active_only"
        input: "src"
        type: "filter"
        condition: "status = 'ACTIVE' AND amount > 0"
      - id: "with_tax"
        input: "active_only"
        type: "derive"
        fields:
          - name: "taxed_amount"
            expression: "amount * 1.0825"
    targets:
      - id: "out"
        input: "with_tax"
        type: "parquet"
        path: "./output/customers/"
        compression: "zstd"
"#
        .to_string()
    }

    #[test]
    fn the_operational_details_example_validates_and_plans() {
        let l = load_str(&good()).expect("should validate");
        assert_eq!(l.plan.name, "customer_export");
        assert_eq!(l.plan.settings.max_threads, 4);
        assert_eq!(l.plan.targets.len(), 1);
        assert_eq!(l.plan.transformations.len(), 2);
        assert_eq!(l.plan.chain_order().unwrap().len(), 2);
        assert_eq!(l.plan.source.partitioning.kind, PartitionKind::Range);
        // secrets must never reach a log line
        let conn = l.plan.source_connection().unwrap();
        assert_eq!(conn.redacted_url(), "postgres://u:***@localhost:55432/db");
    }

    #[test]
    fn wrong_version_is_rejected() {
        let y = good().replace("version: \"1.0\"", "version: \"2.0\"");
        let err = load_str(&y).unwrap_err();
        assert!(err.to_string().contains("version must be exactly \"1.0\""));
    }

    #[test]
    fn unknown_input_is_rejected() {
        let y = good().replace("input: \"active_only\"", "input: \"nope\"");
        let err = load_str(&y).unwrap_err().to_string();
        assert!(err.contains("references unknown input 'nope'"), "{err}");
    }

    #[test]
    fn missing_env_var_is_a_config_error() {
        std::env::remove_var("LDP_MISSING_VAR");
        let y = good().replace("env://LDP_TEST_PG", "env://LDP_MISSING_VAR");
        let err = load_str(&y).unwrap_err().to_string();
        assert!(
            err.contains("environment variable LDP_MISSING_VAR is not set"),
            "{err}"
        );
    }

    #[test]
    fn unsupported_expression_fails_before_any_query() {
        let y = good().replace(
            "condition: \"status = 'ACTIVE' AND amount > 0\"",
            "condition: \"status IN ('ACTIVE')\"",
        );
        let err = load_str(&y).unwrap_err().to_string();
        assert!(err.contains("IN"), "{err}");
        assert!(err.contains("filter 'active_only'"), "{err}");
    }

    #[test]
    fn partition_column_must_be_in_the_query() {
        let y = good().replace("column: \"id\"", "column: \"customer_id\"");
        let err = load_str(&y).unwrap_err().to_string();
        assert!(err.contains("does not appear in the query"), "{err}");
    }

    #[test]
    fn duplicate_ids_are_rejected() {
        let y = good().replace("id: \"with_tax\"", "id: \"active_only\"");
        let err = load_str(&y).unwrap_err().to_string();
        assert!(err.contains("duplicate stage id 'active_only'"), "{err}");
    }

    #[test]
    fn fan_out_is_rejected_because_phase_one_is_linear() {
        let y = good().replace(
            "      - id: \"with_tax\"\n        input: \"active_only\"",
            "      - id: \"with_tax\"\n        input: \"src\"",
        );
        let err = load_str(&y).unwrap_err().to_string();
        assert!(err.contains("single linear chain"), "{err}");
    }

    #[test]
    fn targets_must_agree_on_input() {
        // Two targets are fine (parquet + csv of the same stage), but they must
        // read the same stage: per-branch inputs need Phase 2 conditional split.
        let y = good().replace(
            "        path: \"./output/customers/\"",
            "        path: \"./output/customers/\"\n      - id: \"raw\"\n        input: \"src\"\n        type: \"csv\"\n        path: \"./raw.csv\"",
        );
        let err = load_str(&y).unwrap_err().to_string();
        assert!(
            err.contains("all targets must read the same stage"),
            "{err}"
        );
    }

    #[test]
    fn two_targets_on_the_same_stage_are_allowed() {
        let y = good().replace(
            "        path: \"./output/customers/\"",
            "        path: \"./output/customers/\"\n      - id: \"csv_out\"\n        input: \"with_tax\"\n        type: \"csv\"\n        path: \"./out/customers.csv\"",
        );
        let l = load_str(&y).expect("same-stage fan-out is allowed");
        assert_eq!(l.plan.targets.len(), 2);
    }

    #[test]
    fn unsupported_connection_type_is_rejected() {
        let y = good().replace("type: \"postgres\"", "type: \"snowflake\"");
        let err = load_str(&y).unwrap_err().to_string();
        assert!(err.contains("Phase 1 supports only 'postgres'"), "{err}");
    }

    #[test]
    fn typo_in_settings_field_is_caught_not_ignored() {
        let y = good().replace("max_threads: 4", "max_thread: 4");
        let err = load_str(&y).unwrap_err().to_string();
        assert!(err.contains("unknown field"), "{err}");
    }

    #[test]
    fn defaults_are_applied() {
        let y = good()
            .replace("    max_threads: 4\n", "")
            .replace("    state_file: \".ldp/customer_export.json\"\n", "");
        let l = load_str(&y).unwrap();
        assert_eq!(l.plan.settings.max_threads, 8);
        assert_eq!(l.plan.settings.state_file, ".ldp/customer_export.json");
        assert_eq!(l.plan.settings.max_batch_rows, 65_536);
    }

    #[test]
    fn reports_several_problems_at_once() {
        let y = good()
            .replace("version: \"1.0\"", "version: \"9\"")
            .replace("column: \"id\"", "column: \"nope\"");
        let err = load_str(&y).unwrap_err().to_string();
        assert!(err.contains("problems found"), "{err}");
        assert!(err.contains("version"), "{err}");
        assert!(err.contains("does not appear"), "{err}");
    }
}
