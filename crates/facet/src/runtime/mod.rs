//! The SDK (how a projection is written) and the runtime (what tails
//! chron and applies it).
//!
//! A [`Projection`] names its source, declares its collections through a
//! [`Decl`] and registers typed handlers over chron events. An [`App`]
//! gathers projections; a [`Runtime`] opens a [`crate::Store`] bound to
//! the chron database it connects to, registers every projection and
//! runs one task per projection that subscribes to its source, applies
//! batches of events inside one transaction each with the checkpoint,
//! and serves reads through a [`Handle`].

mod app;
mod apply;
mod collections;
mod cx;
mod deadletter;
mod error;
mod handle;
mod options;
mod projection;
mod rt;
mod runner;
mod status;

pub use app::App;
pub use apply::fingerprint;
pub use collections::{
    BucketsHandle, CounterHandle, Decl, ListHandle, MapHandle, RegisterHandle, SetHandle,
    SortedSetHandle, TableBuilder, TableHandle,
};
pub use cx::Cx;
pub use deadletter::{DEAD_LETTERS, DeadLetter};
pub use error::{HandlerError, RuntimeError};
pub use handle::Handle;
pub use options::RuntimeOptions;
pub use projection::{Codec, OnError, Projection, ProjectionBuilder, Source, StreamPolicy};
pub use rt::Runtime;
pub use status::{State, Status};

pub use chron_client::{self, Client, ClientConfig, Event};
