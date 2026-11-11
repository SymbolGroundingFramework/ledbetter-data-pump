//! The immutable execution plan handed from the control plane to the engine.
//!
//! Lives in `ldp-core` rather than `ldp-config` (see `lib.rs` for why). It is
//! driver-agnostic on purpose: connectors are described by a
//! [`ConnectionSpec`] and resolved to concrete implementations by the CLI, so
//! adding TDS/Snowflake/Oracle later never touches these types.

use crate::expr::Expr;
use crate::LdpError;
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct Settings {
    pub max_threads: usize,
    pub state_file: String,
    pub max_batch_rows: usize,
    pub max_batch_bytes: usize,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            max_threads: 8,
            state_file: String::new(), // always filled in from the pipeline name
            max_batch_rows: 65_536,
            max_batch_bytes: 128_000_000,
        }
    }
}

/// A named connection. `url` has already had `env://VAR` resolved; the
/// password must never be formatted into a log line (Operational Details §5).
#[derive(Debug, Clone)]
pub struct ConnectionSpec {
    pub name: String,
    pub driver: String,
    pub url: String,
    pub options: BTreeMap<String, String>,
}

impl ConnectionSpec {
    /// `postgres://user:***@host:port/db` — safe for logs and error messages.
    pub fn redacted_url(&self) -> String {
        redact_url(&self.url)
    }
}

/// Replace the password component of a URL with `***`.
///
/// Deliberately hand-written instead of using a URL crate: it must never fail
/// and never echo a secret, even for malformed input.
pub fn redact_url(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return "***".to_string();
    };
    let (scheme, rest) = url.split_at(scheme_end + 3);
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (authority, tail) = rest.split_at(authority_end);
    match authority.rsplit_once('@') {
        Some((creds, host)) => match creds.split_once(':') {
            Some((user, _password)) => format!("{scheme}{user}:***@{host}{tail}"),
            // user without a password: nothing to redact
            None => format!("{scheme}{creds}@{host}{tail}"),
        },
        None => url.to_string(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionKind {
    Range,
    None,
}

#[derive(Debug, Clone)]
pub struct PartitionSpec {
    pub kind: PartitionKind,
    pub column: Option<String>,
    pub chunks: Option<usize>,
}

impl PartitionSpec {
    pub fn none() -> Self {
        Self {
            kind: PartitionKind::None,
            column: None,
            chunks: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SourceSpec {
    pub id: String,
    pub connection: String,
    pub query: String,
    pub partitioning: PartitionSpec,
}

#[derive(Debug, Clone)]
pub struct DeriveField {
    /// The SQL-ish text from the YAML, kept for logs and error messages.
    pub source_text: String,
    pub name: String,
    pub expr: Expr,
}

#[derive(Debug, Clone)]
pub enum TransformSpec {
    Filter {
        id: String,
        input: String,
        condition: String,
        predicate: Expr,
    },
    Derive {
        id: String,
        input: String,
        fields: Vec<DeriveField>,
    },
}

impl TransformSpec {
    pub fn id(&self) -> &str {
        match self {
            TransformSpec::Filter { id, .. } | TransformSpec::Derive { id, .. } => id,
        }
    }

    pub fn input(&self) -> &str {
        match self {
            TransformSpec::Filter { input, .. } | TransformSpec::Derive { input, .. } => input,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkKind {
    Parquet,
    Csv,
}

impl SinkKind {
    pub fn as_str(self) -> &'static str {
        match self {
            SinkKind::Parquet => "parquet",
            SinkKind::Csv => "csv",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None,
    Snappy,
    Zstd,
}

impl Compression {
    pub fn as_str(self) -> &'static str {
        match self {
            Compression::None => "none",
            Compression::Snappy => "snappy",
            Compression::Zstd => "zstd",
        }
    }
}

#[derive(Debug, Clone)]
pub struct SinkSpec {
    pub id: String,
    pub input: String,
    pub kind: SinkKind,
    pub path: String,
    pub compression: Compression,
}

#[derive(Debug, Clone)]
pub struct ExecutionPlan {
    pub name: String,
    pub settings: Settings,
    pub connections: Vec<ConnectionSpec>,
    pub source: SourceSpec,
    pub transformations: Vec<TransformSpec>,
    pub targets: Vec<SinkSpec>,
}

impl ExecutionPlan {
    /// The transformation ids in dependency order, starting at the source.
    ///
    /// Phase 1 accepts exactly one linear chain (one source, no fan-out), which
    /// the semantic checker enforces; this therefore cannot fail for a valid
    /// plan and returns an error only if validation was bypassed.
    pub fn chain_order(&self) -> Result<Vec<usize>, LdpError> {
        let mut order = Vec::with_capacity(self.transformations.len());
        let mut current = self.source.id.clone();
        let mut remaining: Vec<usize> = (0..self.transformations.len()).collect();
        while !remaining.is_empty() {
            let Some(pos) = remaining
                .iter()
                .position(|&i| self.transformations[i].input() == current)
            else {
                return Err(LdpError::Config(format!(
                    "transformation chain is not linear: no stage takes input '{current}'"
                )));
            };
            let idx = remaining.remove(pos);
            current = self.transformations[idx].id().to_string();
            order.push(idx);
        }
        Ok(order)
    }

    pub fn connection(&self, name: &str) -> Option<&ConnectionSpec> {
        self.connections.iter().find(|c| c.name == name)
    }

    pub fn source_connection(&self) -> Option<&ConnectionSpec> {
        self.connection(&self.source.connection)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_password_only() {
        assert_eq!(
            redact_url("postgres://alice:sekret@db.example:5432/app"),
            "postgres://alice:***@db.example:5432/app"
        );
        assert_eq!(
            redact_url("postgres://alice@db:5432/app"),
            "postgres://alice@db:5432/app"
        );
        // no credentials at all
        assert_eq!(
            redact_url("postgres://db:5432/app"),
            "postgres://db:5432/app"
        );
        // malformed input never leaks
        assert_eq!(redact_url("not a url"), "***");
        assert_eq!(
            redact_url("postgres://u:p@h/db?x=1"),
            "postgres://u:***@h/db?x=1"
        );
    }
}
