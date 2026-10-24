#![allow(dead_code)]

use std::path::Path;

use facet::{
    Checkpoint, CollectionDecl, CollectionKind, Fsync, KeyKind, KeySchema, Options, ProjectionData,
    ProjectionDecl, Store, TypeSchema, Uuid, ValueSchema,
};

pub fn tmp() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

/// The chron database every test store is bound to.
pub const DB: Uuid = Uuid::from_u128(0x1234);

/// `Fsync::Never`: fast, and a process crash is the only crash these tests
/// simulate; the crash test sets its own.
pub fn opts() -> Options {
    Options::new().fsync(Fsync::Never)
}

pub fn store(dir: impl AsRef<Path>) -> Store {
    Store::open(dir, DB, opts()).unwrap()
}

pub fn store_with(dir: impl AsRef<Path>, opts: Options) -> Store {
    Store::open(dir, DB, opts).unwrap()
}

pub fn decl(name: &str, version: &str, collections: Vec<CollectionDecl>) -> ProjectionDecl {
    ProjectionDecl {
        name: name.into(),
        version: version.into(),
        collections,
    }
}

pub fn register_decl(name: &str) -> CollectionDecl {
    CollectionDecl {
        name: name.into(),
        kind: CollectionKind::Register {
            value: ValueSchema::Any,
        },
    }
}

pub fn map_decl(name: &str, key: &[KeyKind], value: ValueSchema) -> CollectionDecl {
    CollectionDecl {
        name: name.into(),
        kind: CollectionKind::Map {
            key: KeySchema(key.to_vec()),
            value,
        },
    }
}

pub fn typed(t: TypeSchema) -> ValueSchema {
    ValueSchema::Typed(t)
}

/// A projection `p` with a register `r` and a map `m: (uint) -> any`.
pub fn simple_decl(version: &str) -> ProjectionDecl {
    decl(
        "p",
        version,
        vec![
            register_decl("r"),
            map_decl("m", &[KeyKind::Uint], ValueSchema::Any),
        ],
    )
}

pub fn open_simple(dir: impl AsRef<Path>) -> (Store, ProjectionData) {
    let s = store(dir);
    let (p, _) = s.register(&simple_decl("1")).unwrap();
    (s, p)
}

/// A checkpoint whose `next` is `n`, as if the event at `n - 1` was the
/// last applied.
pub fn cp(n: u64) -> Checkpoint {
    assert!(n > 0);
    Checkpoint::after(n - 1, n as u32)
}
