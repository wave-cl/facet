//! Errors of handlers and of the runtime.

/// What a handler returns when it cannot apply an event.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum HandlerError {
    /// The payload did not decode as the handler's type.
    #[error("decode: {0}")]
    Decode(String),
    /// The handler refused the event.
    #[error("{0}")]
    Handler(String),
    /// The handler panicked.
    #[error("panic: {0}")]
    Panic(String),
    /// The engine refused a write.
    #[error(transparent)]
    Store(#[from] crate::Error),
}

impl From<String> for HandlerError {
    fn from(s: String) -> Self {
        HandlerError::Handler(s)
    }
}

impl From<&str> for HandlerError {
    fn from(s: &str) -> Self {
        HandlerError::Handler(s.to_owned())
    }
}

impl From<serde_json::Error> for HandlerError {
    fn from(e: serde_json::Error) -> Self {
        HandlerError::Decode(e.to_string())
    }
}

/// What the runtime cannot do.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RuntimeError {
    /// chron refused or could not be reached.
    #[error("chron: {0}")]
    Chron(#[from] chron_client::ClientError),
    /// The store refused.
    #[error(transparent)]
    Store(#[from] crate::Error),
    /// The application or the options are not usable.
    #[error("configuration: {0}")]
    Config(String),
    /// No projection of that name is registered.
    #[error("projection {0} is not registered")]
    NoProjection(String),
    /// A wait for a position ran out.
    #[error("projection {projection} is at {checkpoint}, not yet at {at_least}")]
    BoundTimeout {
        /// The projection.
        projection: String,
        /// Its checkpoint's `next` when the wait ran out.
        checkpoint: u64,
        /// What was asked for.
        at_least: u64,
    },
    /// The runtime is shutting down.
    #[error("the runtime is stopping")]
    Stopping,
    /// A file system error.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}
