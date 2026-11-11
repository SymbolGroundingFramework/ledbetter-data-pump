//! Structured logging — one flat JSON object per line on stdout.
//!
//! Schema is fixed by `docs/LDP_Phase1_Operational_Details.md` §2:
//! `timestamp`, `level`, `event`, `pipeline` are always present; event-specific
//! fields follow. Written by hand rather than via `tracing` because the spec
//! demands *flat* fields and exact names — matching that with a subscriber
//! would mean fighting its nesting, and this is 80 lines of obvious code.

use crate::plan::SinkSpec;
use serde_json::{json, Map, Value};
use std::io::Write;
use std::sync::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    Error = 0,
    Warn = 1,
    Info = 2,
    Debug = 3,
}

impl LogLevel {
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "error" => Some(LogLevel::Error),
            "warn" | "warning" => Some(LogLevel::Warn),
            "info" => Some(LogLevel::Info),
            "debug" | "trace" => Some(LogLevel::Debug),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            LogLevel::Error => "ERROR",
            LogLevel::Warn => "WARN",
            LogLevel::Info => "INFO",
            LogLevel::Debug => "DEBUG",
        }
    }
}

/// Serialises whole lines so concurrent workers cannot interleave halves of
/// two JSON objects.
static STDOUT_LOCK: Mutex<()> = Mutex::new(());

#[derive(Debug, Clone)]
pub struct Logger {
    pipeline: String,
    level: LogLevel,
}

impl Logger {
    pub fn new(pipeline: impl Into<String>, level: LogLevel) -> Self {
        Self {
            pipeline: pipeline.into(),
            level,
        }
    }

    pub fn level(&self) -> LogLevel {
        self.level
    }

    pub fn enabled(&self, level: LogLevel) -> bool {
        level <= self.level
    }

    pub fn emit(&self, level: LogLevel, event: &str, fields: Map<String, Value>) {
        if !self.enabled(level) {
            return;
        }
        let mut obj = Map::new();
        obj.insert(
            "timestamp".into(),
            Value::String(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)),
        );
        obj.insert("level".into(), Value::String(level.as_str().into()));
        obj.insert("event".into(), Value::String(event.into()));
        obj.insert("pipeline".into(), Value::String(self.pipeline.clone()));
        for (k, v) in fields {
            obj.insert(k, v);
        }
        let line = serde_json::to_string(&Value::Object(obj)).unwrap_or_default();
        let _guard = STDOUT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let mut out = std::io::stdout().lock();
        let _ = writeln!(out, "{line}");
        let _ = out.flush();
    }

    pub fn chunk_started(&self, chunk_id: u64, start: Option<i64>, end: Option<i64>) {
        let mut f = Map::new();
        f.insert("chunk_id".into(), json!(chunk_id));
        f.insert("partition_start".into(), json!(start));
        f.insert("partition_end".into(), json!(end));
        self.emit(LogLevel::Info, "chunk_started", f);
    }

    pub fn chunk_completed(&self, chunk_id: u64, rows: usize, duration: std::time::Duration) {
        let ms = duration.as_millis() as u64;
        // Zero-duration chunks would divide by zero; report rows as the rate.
        let rps = (rows as u64)
            .saturating_mul(1000)
            .checked_div(ms)
            .unwrap_or(rows as u64);
        let mut f = Map::new();
        f.insert("chunk_id".into(), json!(chunk_id));
        f.insert("rows".into(), json!(rows));
        f.insert("duration_ms".into(), json!(ms));
        f.insert("rows_per_sec".into(), json!(rps));
        self.emit(LogLevel::Info, "chunk_completed", f);
    }

    pub fn chunk_skipped(&self, chunk_id: u64) {
        let mut f = Map::new();
        f.insert("chunk_id".into(), json!(chunk_id));
        self.emit(LogLevel::Info, "chunk_skipped", f);
    }

    pub fn error(&self, chunk_id: Option<u64>, message: &str) {
        let mut f = Map::new();
        f.insert("chunk_id".into(), json!(chunk_id));
        f.insert("message".into(), json!(message));
        self.emit(LogLevel::Error, "error", f);
    }

    pub fn warn(&self, event: &str, message: &str) {
        let mut f = Map::new();
        f.insert("message".into(), json!(message));
        self.emit(LogLevel::Warn, event, f);
    }

    pub fn info(&self, event: &str, message: &str) {
        let mut f = Map::new();
        f.insert("message".into(), json!(message));
        self.emit(LogLevel::Info, event, f);
    }

    pub fn debug(&self, event: &str, message: &str) {
        let mut f = Map::new();
        f.insert("message".into(), json!(message));
        self.emit(LogLevel::Debug, event, f);
    }

    /// The mandatory final line: emitted on success, failure and cancel alike.
    #[allow(clippy::too_many_arguments)]
    pub fn job_summary(
        &self,
        status: &str,
        chunks_total: usize,
        chunks_completed: usize,
        chunks_skipped: usize,
        rows_read: u64,
        rows_written: u64,
        dropped_rows: u64,
        duration: std::time::Duration,
        error: Option<&str>,
        targets: &[(SinkSpec, u64, Option<u64>)],
    ) {
        let mut f = Map::new();
        f.insert("status".into(), json!(status));
        f.insert("chunks_total".into(), json!(chunks_total));
        f.insert("chunks_completed".into(), json!(chunks_completed));
        f.insert("chunks_skipped".into(), json!(chunks_skipped));
        f.insert("rows_read".into(), json!(rows_read));
        f.insert("rows_written".into(), json!(rows_written));
        f.insert("dropped_rows".into(), json!(dropped_rows));
        f.insert("duration_ms".into(), json!(duration.as_millis() as u64));
        f.insert("error".into(), json!(error));
        f.insert(
            "targets".into(),
            Value::Array(
                targets
                    .iter()
                    .map(|(spec, rows, verified)| {
                        json!({
                            "id": spec.id,
                            "type": spec.kind.as_str(),
                            "path": spec.path,
                            "rows_written": rows,
                            // Rows recounted from the published output. null
                            // means this sink cannot verify itself.
                            "verified_rows": verified,
                        })
                    })
                    .collect(),
            ),
        );
        // Failures are logged at ERROR so a log-level filter cannot hide them.
        let level = if status == "success" {
            LogLevel::Info
        } else {
            LogLevel::Error
        };
        self.emit(level, "job_summary", f);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn level_ordering_filters_correctly() {
        let l = Logger::new("p", LogLevel::Info);
        assert!(l.enabled(LogLevel::Error));
        assert!(l.enabled(LogLevel::Info));
        assert!(!l.enabled(LogLevel::Debug));
    }

    #[test]
    fn parses_levels() {
        assert_eq!(LogLevel::parse("INFO"), Some(LogLevel::Info));
        assert_eq!(LogLevel::parse("warning"), Some(LogLevel::Warn));
        assert_eq!(LogLevel::parse("trace"), Some(LogLevel::Debug));
        assert_eq!(LogLevel::parse("nope"), None);
    }
}
