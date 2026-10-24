//! The error type.

use uuid::Uuid;

/// What can go wrong in the engine.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A file system error.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// A storage (redb) error.
    #[error("storage: {0}")]
    Storage(#[from] redb::Error),
    /// A projection file does not hold what the engine wrote.
    #[error("{file}: {reason}")]
    Corrupt {
        /// The file.
        file: String,
        /// What was wrong with it.
        reason: String,
    },
    /// The projection file was built from another chron database.
    #[error("{file} is bound to chron database {bound}, not {expected}")]
    WrongDatabase {
        /// The file.
        file: String,
        /// The id the file carries.
        bound: Uuid,
        /// The id the store was opened with.
        expected: Uuid,
    },
    /// The projection has a live handle, so it cannot be registered again
    /// or dropped.
    #[error("projection {0} is open")]
    InUse(String),
    /// No projection file of that name.
    #[error("projection {0} does not exist")]
    NoProjection(String),
    /// The declaration is not valid.
    #[error("declaration: {0}")]
    Declaration(String),
    /// A value does not fit the declared schema.
    #[error("validation: {0}")]
    Validation(String),
    /// A size or count limit was exceeded.
    #[error("limit: {0}")]
    Limit(String),
    /// A batch tried to commit a checkpoint at or below the current one.
    #[error("the checkpoint does not advance: {current} -> {proposed}")]
    CheckpointNotAdvancing {
        /// The committed checkpoint's `next`.
        current: u64,
        /// What the batch proposed.
        proposed: u64,
    },
    /// A page cursor does not belong to this scan.
    #[error("invalid cursor: {0}")]
    InvalidCursor(String),
    /// An arithmetic overflow.
    #[error("overflow: {0}")]
    Overflow(String),
    /// A unique index already holds the key.
    #[error("unique index {index} already holds {key}")]
    UniqueViolation {
        /// The index.
        index: String,
        /// The key, rendered.
        key: String,
    },
    /// The collection is not in the projection's declaration.
    #[error("collection {0} is not declared")]
    NotDeclared(String),
    /// The collection was declared as another kind, or with other key or
    /// value types, than the handle asks for.
    #[error("collection {name}: declared {declared}, asked {asked}")]
    KindMismatch {
        /// The collection.
        name: String,
        /// What the declaration says.
        declared: String,
        /// What the handle asked for.
        asked: String,
    },
    /// A key could not be encoded or decoded.
    #[error("key: {0}")]
    Key(String),
    /// A value could not be encoded or decoded.
    #[error("value: {0}")]
    Value(String),
    /// The batch closure panicked; nothing was written.
    #[error("the batch panicked: {0}")]
    Panicked(String),
}

/// `Result` with the engine's error.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// What [`crate::ProjectionData::apply`] returns when the batch did not
/// commit.
#[derive(Debug, thiserror::Error)]
pub enum ApplyError<E> {
    /// The batch closure returned an error; nothing was written.
    #[error("batch: {0}")]
    Handler(E),
    /// The engine refused or failed; nothing was written.
    #[error(transparent)]
    Store(Error),
}

impl<E> From<Error> for ApplyError<E> {
    fn from(e: Error) -> Self {
        ApplyError::Store(e)
    }
}

macro_rules! storage_from {
    ($($t:ty),*) => {$(
        impl From<$t> for Error {
            fn from(e: $t) -> Self {
                Error::Storage(e.into())
            }
        }
    )*};
}
storage_from!(
    redb::DatabaseError,
    redb::TransactionError,
    redb::TableError,
    redb::StorageError,
    redb::CommitError,
    redb::SetDurabilityError
);

impl serde::ser::Error for Error {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        Error::Value(msg.to_string())
    }
}

impl serde::de::Error for Error {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        Error::Value(msg.to_string())
    }
}
