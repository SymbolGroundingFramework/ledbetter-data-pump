//! Checkpoint state: which chunks are safely written, so a restart skips them.
//!
//! Contract §3.5 + addendum §8. Two invariants matter more than anything else
//! here:
//!
//! 1. **Writes are atomic** — write `state.json.tmp`, fsync, rename over the
//!    original. A crash mid-write leaves the previous good state untouched.
//! 2. **A corrupt or foreign state file is an error, never a silent reset.**
//!    Silently starting from zero would re-read everything and (for a
//!    non-idempotent sink) duplicate rows.

use crate::error::LdpError;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::io::Write;
use std::path::Path;

/// Bumped when the on-disk shape changes; older versions are refused loudly
/// rather than mis-parsed (addendum §8: "state file contains schema version").
pub const STATE_SCHEMA_VERSION: u32 = 1;

fn default_schema_version() -> u32 {
    STATE_SCHEMA_VERSION
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct PipelineState {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    /// Which pipeline this state belongs to. Guards against two pipelines
    /// accidentally sharing a state file.
    #[serde(default)]
    pub pipeline: String,
    #[serde(default)]
    pub completed_chunks: HashSet<u64>,
    #[serde(default)]
    pub updated_at: Option<String>,
    #[serde(default)]
    pub run_id: Option<String>,
}

impl PipelineState {
    pub fn new(pipeline: &str) -> Self {
        Self {
            schema_version: STATE_SCHEMA_VERSION,
            pipeline: pipeline.to_string(),
            completed_chunks: HashSet::new(),
            updated_at: None,
            run_id: None,
        }
    }

    /// Load state for `pipeline`, or a fresh state if the file does not exist.
    pub fn load(path: &Path, pipeline: &str) -> Result<Self, LdpError> {
        let raw = match fs::read_to_string(path) {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Self::new(pipeline)),
            Err(e) => {
                return Err(LdpError::Checkpoint(format!(
                    "cannot read state file {}: {e}",
                    path.display()
                )))
            }
        };
        let state: PipelineState = serde_json::from_str(&raw).map_err(|e| {
            LdpError::Checkpoint(format!(
                "state file {} is not valid checkpoint JSON ({e}); \
                 delete it to start over, or restore it — LDP will not guess",
                path.display()
            ))
        })?;
        if state.schema_version != STATE_SCHEMA_VERSION {
            return Err(LdpError::Checkpoint(format!(
                "state file {} has schema_version {} but this build understands {}",
                path.display(),
                state.schema_version,
                STATE_SCHEMA_VERSION
            )));
        }
        if !state.pipeline.is_empty() && state.pipeline != pipeline {
            return Err(LdpError::Checkpoint(format!(
                "state file {} belongs to pipeline '{}', not '{}'",
                path.display(),
                state.pipeline,
                pipeline
            )));
        }
        Ok(state)
    }

    /// Atomically persist. Creates the parent directory when needed.
    pub fn save(&self, path: &Path) -> Result<(), LdpError> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).map_err(|e| {
                    LdpError::Checkpoint(format!(
                        "cannot create state directory {}: {e}",
                        parent.display()
                    ))
                })?;
            }
        }
        let body = serde_json::to_vec_pretty(self)
            .map_err(|e| LdpError::Checkpoint(format!("cannot serialise state: {e}")))?;

        // Same directory ⇒ same filesystem ⇒ rename is atomic.
        let tmp = path.with_extension("json.tmp");
        {
            let mut f = fs::File::create(&tmp).map_err(|e| {
                LdpError::Checkpoint(format!("cannot create {}: {e}", tmp.display()))
            })?;
            f.write_all(&body).and_then(|_| f.sync_all()).map_err(|e| {
                LdpError::Checkpoint(format!("cannot write {}: {e}", tmp.display()))
            })?;
        }
        fs::rename(&tmp, path).map_err(|e| {
            // Leave no debris behind if the rename failed.
            let _ = fs::remove_file(&tmp);
            LdpError::Checkpoint(format!("cannot publish state file {}: {e}", path.display()))
        })
    }

    pub fn is_completed(&self, id: u64) -> bool {
        self.completed_chunks.contains(&id)
    }

    pub fn mark_completed(&mut self, id: u64) {
        self.completed_chunks.insert(id);
        self.updated_at = Some(now_rfc3339());
    }

    pub fn completed_count(&self) -> usize {
        self.completed_chunks.len()
    }
}

fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn tmpdir() -> PathBuf {
        let p = std::env::temp_dir().join(format!("ldp-ckpt-{}", uuid_like()));
        fs::create_dir_all(&p).unwrap();
        p
    }

    fn uuid_like() -> String {
        // No uuid crate: enough uniqueness for a test directory.
        let n = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{n}-{}", std::process::id())
    }

    #[test]
    fn round_trips_and_is_atomic() {
        let dir = tmpdir();
        let path = dir.join("state.json");
        let mut s = PipelineState::new("orders");
        s.mark_completed(3);
        s.mark_completed(7);
        s.save(&path).unwrap();

        // No temp file left behind.
        assert!(!dir.join("state.json.tmp").exists());

        let loaded = PipelineState::load(&path, "orders").unwrap();
        assert!(loaded.is_completed(3));
        assert!(loaded.is_completed(7));
        assert!(!loaded.is_completed(4));
        assert_eq!(loaded.completed_count(), 2);
        assert_eq!(loaded.schema_version, STATE_SCHEMA_VERSION);
    }

    #[test]
    fn missing_file_is_fresh_state_not_an_error() {
        let dir = tmpdir();
        let s = PipelineState::load(&dir.join("nope.json"), "orders").unwrap();
        assert_eq!(s.completed_count(), 0);
        assert_eq!(s.pipeline, "orders");
    }

    #[test]
    fn corrupt_file_is_a_loud_error() {
        let dir = tmpdir();
        let path = dir.join("state.json");
        fs::write(&path, b"{ this is not json").unwrap();
        let err = PipelineState::load(&path, "orders").unwrap_err();
        assert!(matches!(err, LdpError::Checkpoint(_)));
        assert!(err.to_string().contains("not valid checkpoint JSON"));
    }

    #[test]
    fn foreign_pipeline_is_rejected() {
        let dir = tmpdir();
        let path = dir.join("state.json");
        PipelineState::new("orders").save(&path).unwrap();
        let err = PipelineState::load(&path, "customers").unwrap_err();
        assert!(err.to_string().contains("belongs to pipeline 'orders'"));
    }

    #[test]
    fn future_schema_version_is_rejected() {
        let dir = tmpdir();
        let path = dir.join("state.json");
        fs::write(&path, br#"{"schema_version": 99, "pipeline": "orders"}"#).unwrap();
        let err = PipelineState::load(&path, "orders").unwrap_err();
        assert!(err.to_string().contains("schema_version 99"));
    }
}
