//! The dead letter table every projection carries.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::schema::{CollectionDecl, CollectionKind, ColumnDecl, TypeSchema};
use crate::value::Timestamp;

/// The name of the table.
pub const DEAD_LETTERS: &str = "__dead";

/// An event a handler failed on and the projection skipped
/// ([`super::OnError::Skip`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeadLetter {
    /// The event's global position (the primary key).
    pub position: u64,
    /// Its aggregate, if any.
    pub aggregate: Option<Uuid>,
    /// Its index in the aggregate.
    pub index: u64,
    /// Its tags.
    pub tags: Vec<Bytes>,
    /// The handler's error.
    pub error: String,
    /// When it was skipped.
    pub at: Timestamp,
}

pub(crate) fn decl() -> CollectionDecl {
    let col = |name: &str, ty, nullable| ColumnDecl {
        name: name.into(),
        ty,
        nullable,
    };
    CollectionDecl {
        name: DEAD_LETTERS.into(),
        kind: CollectionKind::Table {
            columns: vec![
                col("position", TypeSchema::Uint, false),
                col("aggregate", TypeSchema::Uuid, true),
                col("index", TypeSchema::Uint, false),
                col("tags", TypeSchema::List(Box::new(TypeSchema::Bytes)), false),
                col("error", TypeSchema::Str, false),
                col("at", TypeSchema::Timestamp, false),
            ],
            primary_key: vec!["position".into()],
            indexes: vec![],
        },
    }
}
