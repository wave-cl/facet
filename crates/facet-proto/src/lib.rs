//! The `facet.v1` gRPC contract: generated messages, the `Query` and
//! `Admin` services' client and server, and the response metadata keys
//! the daemon sets.

pub mod v1 {
    #![allow(clippy::all, missing_docs)]
    // Generated from `proto/facet/v1/facet.proto` and committed, so no
    // `protoc` is needed to build; `FACET_PROTO_REGEN=1 cargo build -p
    // facet-proto` regenerates it (see build.rs).
    include!("generated/facet.v1.rs");
}

/// On an `UNAVAILABLE` for a read whose `at_least` was not reached in
/// time: the projection's checkpoint, as decimal text.
pub const CHECKPOINT_HEADER: &str = "facet-checkpoint";
/// On the same status: chron's confirmed head as the daemon knows it.
pub const CHRON_HEAD_HEADER: &str = "facet-chron-head";
/// On an `OUT_OF_RANGE` ending a `Watch` that fell behind: the
/// checkpoint to re-read from.
pub const POSITION_HEADER: &str = "facet-position";
/// The largest `wait_ms` a read may ask for.
pub const MAX_WAIT_MS: u32 = 30_000;
/// The `wait_ms` used when a read asks for `at_least` without one.
pub const DEFAULT_WAIT_MS: u32 = 5_000;
