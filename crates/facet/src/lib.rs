//! facet: a projection database over chron.
//!
//! The engine keeps one redb file per projection under a store
//! directory bound to a chron database. A projection's batch runs inside
//! one write transaction and commits with its checkpoint, so a crash never
//! leaves rows ahead of or behind the checkpoint.
//!
//! ```text
//! <dir>/<projection>.redb
//!   meta         format, db_id, projection, version, decl, checkpoint, created_at
//!   c:<name>     one table per collection (see each kind's docs)
//! ```

#![deny(missing_docs)]
#![forbid(unsafe_code)]

pub mod error;
pub mod keyenc;
pub mod limits;
pub mod meta;
pub mod prim;
pub mod projection;
pub mod reader;
pub mod schema;
pub mod store;
pub mod value;
pub mod vbin;
pub mod watch;
pub mod writer;

pub use error::{ApplyError, Error, Result};
pub use keyenc::{KeyKind, KeyPart, KeyTuple, KeyType};
pub use meta::{Checkpoint, FORMAT, Mark};
pub use prim::buckets::{Bucket, window_start};
pub use prim::{Num, Scan, Score};
pub use projection::Projection;
pub use reader::{Cursor, Page, Snapshot};
pub use schema::{
    CollectionDecl, CollectionKind, ColumnDecl, IndexDecl, KeySchema, NumKind, ProjectionDecl,
    ScoreKind, TypeSchema, ValueSchema, Width,
};
pub use store::{Fsync, OnDbMismatch, Options, ProjectionInfo, Rebuilt, Registered, Store};
pub use value::{Timestamp, Value, from_value, to_value};
pub use watch::{BatchSummary, Changed, CollectionChange};
pub use writer::Writer;

pub use bytes::Bytes;
pub use rust_decimal::Decimal;
pub use uuid::Uuid;
