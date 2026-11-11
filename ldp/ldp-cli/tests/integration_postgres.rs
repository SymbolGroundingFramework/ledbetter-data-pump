//! End-to-end integration test against a REAL Postgres (DoD #9).
//!
//! Requires `LDP_TEST_PG_URL`, e.g.
//!   postgres://postgres@127.0.0.1:55432/ldp
//! The workspace fixture provides that (`.\scripts\pg.ps1 start`). When the
//! variable is absent each test explains itself and returns, so `cargo test`
//! still passes on a machine without a database.
//!
//! What these prove:
//!   * Postgres → Parquet with real range partitioning, exact row accounting
//!   * a checkpointed chunk is skipped on the next run, without duplication
//!   * a zero-row source still produces a valid Parquet file
//!   * a column typo is rejected as a planning (exit 1) error before any write

use ldp_config::load_str;
use ldp_core::plan::{Compression, SinkSpec};
use ldp_core::runner::{run, SinkBinding};
use ldp_core::{LdpError, LogLevel, Logger, RunReport};
use ldp_parquet::ParquetSink;
use ldp_postgres::PostgresSource;
use std::path::Path;
use std::sync::Arc;

fn pg_url() -> Option<String> {
    match std::env::var("LDP_TEST_PG_URL") {
        Ok(v) if !v.trim().is_empty() => Some(v),
        _ => None,
    }
}

async fn seed(url: &str, rows: i64) {
    let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
        .await
        .expect("connect to LDP_TEST_PG_URL");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS ldp_it;
             CREATE TABLE ldp_it (id BIGINT PRIMARY KEY, name TEXT, amount DOUBLE PRECISION);
             INSERT INTO ldp_it SELECT g, 'row_' || g, (g % 100)::double precision
             FROM generate_series(1, {rows}) AS g;"
        ))
        .await
        .expect("seed ldp_it");
}

/// `filter` = None → straight copy; Some(cond) → single filter stage.
/// Windows paths inside a double-quoted YAML scalar would be read as escape
/// sequences (`\U...`), so emit forward slashes — valid on Windows and inert
/// in YAML.
fn yaml_path(p: &str) -> String {
    p.replace('\\', "/")
}

fn pipeline_yaml(out_dir: &str, state_file: &str, filter: Option<&str>) -> String {
    let out_dir = yaml_path(out_dir);
    let state_file = yaml_path(state_file);
    let transform = match filter {
        None => String::new(),
        Some(cond) => format!(
            "    transformations:\n      - id: \"f\"\n        input: \"src\"\n        type: \"filter\"\n        condition: \"{cond}\"\n"
        ),
    };
    let target_input = if filter.is_some() { "f" } else { "src" };
    format!(
        r#"
version: "1.0"
pipeline:
  name: "it_resume"
  settings:
    max_threads: 2
    state_file: "{state_file}"
    max_batch_rows: 1000
  resources:
    connections:
      - name: "pg"
        type: "postgres"
        url: "env://LDP_TEST_PG_URL"
  dataflow:
    sources:
      - id: "src"
        connection: "pg"
        query: "SELECT id, name, amount FROM ldp_it"
        partitioning:
          type: "range"
          column: "id"
          chunks: 2
{transform}    targets:
      - id: "out"
        input: "{target_input}"
        type: "parquet"
        path: "{out_dir}"
        compression: "snappy"
"#
    )
}

/// Count data rows across every Parquet file in a directory. A directory that
/// was never created has zero rows (that is what an aborted run leaves behind).
fn parquet_rows(dir: &Path) -> usize {
    let mut total = 0usize;
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return 0,
    };
    for entry in entries {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) != Some("parquet") {
            continue;
        }
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            std::fs::File::open(&path).unwrap(),
        )
        .unwrap()
        .build()
        .unwrap();
        for batch in reader {
            total += batch.unwrap().num_rows();
        }
    }
    total
}

async fn run_yaml(yaml: &str, url: &str, out_dir: &Path) -> Result<RunReport, LdpError> {
    let loaded = load_str(yaml).expect("test pipeline must validate");
    let plan = loaded.plan;
    let source = PostgresSource::connect(plan.source.clone(), plan.settings.clone(), url).await?;
    let spec: SinkSpec = plan.targets[0].clone();
    let sinks = vec![SinkBinding::new(
        Arc::new(ParquetSink::new(out_dir, Compression::Snappy)),
        spec,
    )];
    let logger = Logger::new(plan.name.clone(), LogLevel::Error);
    run(&plan, Arc::new(source), sinks, &logger).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_to_parquet_then_resume_without_duplicates() {
    let Some(url) = pg_url() else {
        eprintln!("SKIP: set LDP_TEST_PG_URL to run the Postgres integration tests");
        return;
    };
    std::env::set_var("LDP_TEST_PG_URL", &url);
    seed(&url, 10_000).await;

    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    let state = dir.path().join("state.json");
    let yaml = pipeline_yaml(out.to_str().unwrap(), state.to_str().unwrap(), None);

    // ---- first run -------------------------------------------------------
    let report = run_yaml(&yaml, &url, &out).await.expect("first run");
    assert_eq!(report.status, "success");
    assert_eq!(report.chunks_total, 2);
    assert_eq!(report.chunks_completed, 2);
    assert_eq!(report.rows_read, 10_000);
    assert_eq!(report.rows_written, 10_000);
    assert_eq!(parquet_rows(&out), 10_000);

    let files: Vec<String> = std::fs::read_dir(&out)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
        .collect();
    assert!(
        files.iter().all(|f| !f.ends_with(".tmp")),
        "temp debris: {files:?}"
    );

    let state_json = std::fs::read_to_string(&state).unwrap();
    let state_val: serde_json::Value = serde_json::from_str(&state_json).unwrap();
    assert_eq!(
        state_val["completed_chunks"].as_array().unwrap().len(),
        2,
        "both chunks checkpointed: {state_json}"
    );

    // ---- simulate "chunk 1 never happened" --------------------------------
    // Keep chunk 0's files, delete chunk 1's, checkpoint only chunk 0. The next
    // run must skip 0 (keeping its files) and re-read exactly chunk 1.
    for entry in std::fs::read_dir(&out).unwrap() {
        let p = entry.unwrap().path();
        if p.file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("part-00001")
        {
            std::fs::remove_file(p).unwrap();
        }
    }
    std::fs::write(
        &state,
        br#"{"schema_version":1,"pipeline":"it_resume","completed_chunks":[0]}"#,
    )
    .unwrap();

    let resumed = run_yaml(&yaml, &url, &out).await.expect("resume run");
    assert_eq!(resumed.status, "success");
    assert_eq!(resumed.chunks_skipped, 1, "chunk 0 must be skipped");
    assert_eq!(resumed.rows_read, 5_000, "only chunk 1 is read again");
    assert_eq!(resumed.rows_written, 5_000);
    assert_eq!(
        parquet_rows(&out),
        10_000,
        "resume must not duplicate chunk 0 nor lose chunk 1"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zero_row_source_is_a_successful_run_with_a_valid_file() {
    let Some(url) = pg_url() else {
        eprintln!("SKIP: set LDP_TEST_PG_URL to run the Postgres integration tests");
        return;
    };
    std::env::set_var("LDP_TEST_PG_URL", &url);
    seed(&url, 10).await;

    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out_empty");
    let state = dir.path().join("state_empty.json");
    // The filter excludes everything: MIN/MAX still exist, so partitions are
    // planned, but every batch is empty → zero rows written, valid file.
    let yaml = pipeline_yaml(
        out.to_str().unwrap(),
        state.to_str().unwrap(),
        Some("amount > 1000000"),
    );
    let report = run_yaml(&yaml, &url, &out).await.expect("empty run");
    assert_eq!(report.status, "success");
    assert_eq!(report.rows_written, 0);
    assert_eq!(report.rows_read, 10);
    assert_eq!(
        parquet_rows(&out),
        0,
        "a valid, empty Parquet file is written"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_column_is_rejected_before_any_rows_move() {
    let Some(url) = pg_url() else {
        eprintln!("SKIP: set LDP_TEST_PG_URL to run the Postgres integration tests");
        return;
    };
    std::env::set_var("LDP_TEST_PG_URL", &url);
    seed(&url, 10).await;
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out_bad");
    let state = dir.path().join("state_bad.json");
    let yaml = pipeline_yaml(
        out.to_str().unwrap(),
        state.to_str().unwrap(),
        Some("nope = 1"),
    );

    // Parsing succeeds (the column could only be checked against the live
    // schema), so the run must fail at planning time with exit code 1.
    let err = run_yaml(&yaml, &url, &out)
        .await
        .expect_err("unknown column must abort before writing");
    assert_eq!(err.exit_code(), 1, "planning error, not runtime: {err}");
    assert!(err.to_string().contains("unknown column 'nope'"), "{err}");
    assert_eq!(parquet_rows(&out), 0, "nothing may be published");
}
