//! A typed async client for `facetd`: reads over the `facet.v1.Query`
//! service and operations over `facet.v1.Admin`, with keys and values as
//! JSON.
//!
//! ```no_run
//! # async fn demo() -> Result<(), facet_client::ClientError> {
//! use facet_client::{Client, ClientConfig, ReadAt};
//! let c = Client::connect("http://127.0.0.1:4343", ClientConfig::default()).await?;
//! let (balance, seen) = c
//!     .get("bank", "balance", Some(&serde_json::json!("00000000-0000-0000-0000-000000000001")), ReadAt::default())
//!     .await?;
//! println!("{balance:?} at checkpoint {}", seen.checkpoint);
//! # Ok(()) }
//! ```

#![deny(missing_docs)]
#![forbid(unsafe_code)]

mod client;
mod error;

pub use client::{
    Change, ChangeStream, Client, ClientConfig, CollectionInfo, DaemonStatus, DeadLetter, Entry,
    Identity, Page, ProjectionStatus, ReadAt, Scan, Seen, SourceStatus, TlsConfig,
};
pub use error::ClientError;
pub use facet_proto::v1::State;
