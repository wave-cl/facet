use tonic::{Code, Status};

/// What a call can fail with. The daemon's status codes become variants;
/// the rest is carried as [`ClientError::Status`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ClientError {
    /// The read asked to wait for a position the projection did not
    /// reach in time.
    #[error("the projection is at {checkpoint}, not yet at {at_least} (chron head {chron_head})")]
    BoundTimeout {
        /// The projection's checkpoint when the wait ran out.
        checkpoint: u64,
        /// What the read asked for.
        at_least: u64,
        /// chron's confirmed head as the daemon knows it.
        chron_head: u64,
    },
    /// A watch fell behind; re-read from `position` and watch again.
    #[error("the watch fell behind; re-read from {position}")]
    WatchLagged {
        /// The checkpoint to re-read from.
        position: u64,
    },
    /// No such projection, collection, index or orphan.
    #[error("{0}")]
    NotFound(String),
    /// The projection is failed (its error is the message), or the
    /// operation does not apply.
    #[error("{0}")]
    FailedPrecondition(String),
    /// A malformed request: a key that does not fit the collection, a
    /// bad cursor, a width not declared.
    #[error("{0}")]
    InvalidArgument(String),
    /// The call needs a token.
    #[error("{0}")]
    Unauthenticated(String),
    /// The call needs the admin token.
    #[error("{0}")]
    PermissionDenied(String),
    /// The daemon is stopping, or chron is unreachable for a wait.
    #[error("{0}")]
    Unavailable(String),
    /// The connection could not be made or was lost.
    #[error("transport: {0}")]
    Transport(#[from] tonic::transport::Error),
    /// Any other status.
    #[error("{0}")]
    Status(Status),
    /// A response that did not parse.
    #[error("decode: {0}")]
    Decode(String),
}

fn header<T: std::str::FromStr>(s: &Status, key: &str) -> Option<T> {
    s.metadata().get(key)?.to_str().ok()?.parse().ok()
}

impl From<Status> for ClientError {
    fn from(s: Status) -> Self {
        match s.code() {
            Code::Unavailable
                if let (Some(checkpoint), Some(chron_head), Some(at_least)) = (
                    header::<u64>(&s, facet_proto::CHECKPOINT_HEADER),
                    header::<u64>(&s, facet_proto::CHRON_HEAD_HEADER),
                    header::<u64>(&s, "facet-at-least"),
                ) =>
            {
                ClientError::BoundTimeout {
                    checkpoint,
                    at_least,
                    chron_head,
                }
            }
            Code::OutOfRange
                if let Some(position) = header::<u64>(&s, facet_proto::POSITION_HEADER) =>
            {
                ClientError::WatchLagged { position }
            }
            Code::NotFound => ClientError::NotFound(s.message().to_string()),
            Code::FailedPrecondition => ClientError::FailedPrecondition(s.message().to_string()),
            Code::InvalidArgument => ClientError::InvalidArgument(s.message().to_string()),
            Code::Unauthenticated => ClientError::Unauthenticated(s.message().to_string()),
            Code::PermissionDenied => ClientError::PermissionDenied(s.message().to_string()),
            Code::Unavailable => ClientError::Unavailable(s.message().to_string()),
            _ => ClientError::Status(s),
        }
    }
}
