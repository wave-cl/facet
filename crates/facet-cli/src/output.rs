//! Printing, and the exit code an error maps to.

use facet_client::ClientError;
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Human,
    Json,
}

/// A mistake in the arguments that clap could not catch (exit code 2).
#[derive(Debug)]
pub struct Usage(pub String);

impl std::fmt::Display for Usage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Usage {}

/// The daemon reports a degraded state (exit code 1).
#[derive(Debug)]
pub struct Degraded(pub String);

impl std::fmt::Display for Degraded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Degraded {}

/// 1 for a refusal or a degraded state, 2 for usage or an unreachable
/// daemon.
pub fn exit_code(e: &anyhow::Error) -> i32 {
    if e.downcast_ref::<Usage>().is_some() {
        return 2;
    }
    if e.downcast_ref::<Degraded>().is_some() {
        return 1;
    }
    match e.downcast_ref::<ClientError>() {
        Some(ClientError::Transport(_)) | Some(ClientError::Unavailable(_)) => 2,
        Some(_) => 1,
        None => 2,
    }
}

pub fn render_error(e: &anyhow::Error) -> String {
    match e.downcast_ref::<ClientError>() {
        Some(ClientError::Transport(t)) => format!("cannot reach the daemon: {t}"),
        Some(ClientError::NotFound(m)) => format!("{m} (see `facet projections`)"),
        Some(other) => other.to_string(),
        None => format!("{e:#}"),
    }
}

/// Parses a key or value argument as JSON, else takes it as a string.
pub fn json_arg(s: &str) -> Value {
    serde_json::from_str(s).unwrap_or_else(|_| Value::String(s.to_owned()))
}

/// Compact JSON of a key: a one-part key prints bare.
pub fn key_text(k: &Value) -> String {
    match k {
        Value::Array(a) if a.len() == 1 => a[0].to_string(),
        other => other.to_string(),
    }
}
