//! Error type shared by every crate (contract §3.4).
//!
//! Exit-code mapping follows `docs/LDP_Phase1_Operational_Details.md` §3:
//! configuration/validation/planning → 1, anything that goes wrong once data
//! movement started → 2.

use std::fmt;

#[derive(Debug, thiserror::Error)]
pub enum LdpError {
    #[error("configuration error: {0}")]
    Config(String),

    #[error("source error: {0}")]
    Source(String),

    #[error("transform error: {0}")]
    Transform(String),

    #[error("sink error: {0}")]
    Sink(String),

    #[error("checkpoint error: {0}")]
    Checkpoint(String),

    #[error("expression error on row: {0}")]
    Expression(String),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl LdpError {
    /// Process exit code for this error class.
    pub fn exit_code(&self) -> i32 {
        match self {
            // Everything decided before the first byte moves.
            LdpError::Config(_) => 1,
            // Everything that can only be known once we are moving data.
            _ => 2,
        }
    }

    /// The message without the "configuration error:"-style class prefix.
    ///
    /// Used when one error is embedded in another (e.g. an expression parse
    /// failure inside "filter 'x': ..."), so the operator does not read
    /// "configuration error: configuration error:".
    pub fn detail(&self) -> String {
        match self {
            LdpError::Config(s)
            | LdpError::Source(s)
            | LdpError::Transform(s)
            | LdpError::Sink(s)
            | LdpError::Checkpoint(s)
            | LdpError::Expression(s) => s.clone(),
            LdpError::Other(e) => e.to_string(),
        }
    }
}

/// A validation failure that carries *all* the problems found, not just the
/// first one (addendum §13: "emit all errors at once").
#[derive(Debug, Clone, Default)]
pub struct ValidationErrors {
    pub messages: Vec<String>,
}

impl ValidationErrors {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, message: impl Into<String>) {
        self.messages.push(message.into());
    }

    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    pub fn into_result(self) -> Result<(), LdpError> {
        if self.is_empty() {
            Ok(())
        } else {
            Err(LdpError::Config(self.join()))
        }
    }

    pub fn join(&self) -> String {
        if self.messages.len() == 1 {
            self.messages[0].clone()
        } else {
            let mut out = format!("{} problems found:", self.messages.len());
            for m in &self.messages {
                out.push_str("\n  - ");
                out.push_str(m);
            }
            out
        }
    }
}

impl fmt::Display for ValidationErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.join())
    }
}
