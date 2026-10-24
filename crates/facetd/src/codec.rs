//! Between the wire and the library: statuses for the engine's and the
//! runtime's errors.

use facet::{Error, RuntimeError};
use tonic::Status;

/// The status an engine error becomes.
pub fn status(e: Error) -> Status {
    match e {
        Error::NotDeclared(_) | Error::NoProjection(_) => Status::not_found(e.to_string()),
        Error::Key(_)
        | Error::Value(_)
        | Error::InvalidCursor(_)
        | Error::Validation(_)
        | Error::Declaration(_)
        | Error::KindMismatch { .. }
        | Error::Limit(_) => Status::invalid_argument(e.to_string()),
        Error::InUse(_) | Error::CheckpointNotAdvancing { .. } => {
            Status::failed_precondition(e.to_string())
        }
        other => Status::internal(other.to_string()),
    }
}

/// The status a runtime error becomes.
pub fn runtime_status(e: RuntimeError) -> Status {
    match e {
        RuntimeError::NoProjection(_) => Status::not_found(e.to_string()),
        RuntimeError::Store(s) => status(s),
        RuntimeError::Stopping => Status::unavailable(e.to_string()),
        RuntimeError::BoundTimeout {
            projection,
            checkpoint,
            at_least,
        } => {
            let mut s = Status::unavailable(format!(
                "projection {projection} is at {checkpoint}, not yet at {at_least}"
            ));
            insert(&mut s, facet_proto::CHECKPOINT_HEADER, checkpoint);
            insert(&mut s, "facet-at-least", at_least);
            s
        }
        other => Status::internal(other.to_string()),
    }
}

/// Adds a decimal metadata value.
pub fn insert(s: &mut Status, key: &'static str, value: u64) {
    if let Ok(v) = value.to_string().parse() {
        s.metadata_mut().insert(key, v);
    }
}
