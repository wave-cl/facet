//! The rebuild rule at registration, binding to the chron database, and
//! `ProjectionData::rebuild`.

use facet::{Changed, Error, OnDbMismatch, Rebuilt, Uuid};

use crate::common::*;

fn fill(p: &facet::ProjectionData) {
    p.apply::<_, Error>(cp(10), |w| {
        w.register("r")?.set(&"v")?;
        w.map::<u64, i64>("m")?.put(&1, &1)
    })
    .unwrap();
}

fn rows(p: &facet::ProjectionData) -> (Option<String>, u64) {
    let snap = p.snapshot().unwrap();
    (
        snap.register::<String>("r").unwrap().get().unwrap(),
        snap.map::<u64, i64>("m").unwrap().len().unwrap(),
    )
}

#[test]
fn the_same_declaration_keeps_everything() {
    let d = tmp();
    let s = store(d.path());
    let (p, _) = s.register(&simple_decl("1")).unwrap();
    fill(&p);
    drop(p);
    let (p, reg) = s.register(&simple_decl("1")).unwrap();
    assert_eq!(reg.rebuilt, Rebuilt::No);
    assert_eq!(reg.checkpoint, cp(10));
    assert_eq!(rows(&p), (Some("v".into()), 1));
}

#[test]
fn a_changed_version_or_declaration_rebuilds() {
    let d = tmp();
    let s = store(d.path());
    let (p, _) = s.register(&simple_decl("1")).unwrap();
    fill(&p);
    drop(p);
    let (p, reg) = s.register(&simple_decl("2")).unwrap();
    assert_eq!(reg.rebuilt, Rebuilt::VersionChanged);
    assert_eq!(reg.checkpoint.next, 0);
    assert_eq!(p.checkpoint().next, 0);
    assert_eq!(rows(&p), (None, 0));
    fill(&p);
    drop(p);

    let mut changed = simple_decl("2");
    changed.collections.push(register_decl("extra"));
    let (p, reg) = s.register(&changed).unwrap();
    assert_eq!(reg.rebuilt, Rebuilt::DeclChanged);
    assert_eq!(rows(&p), (None, 0));
    fill(&p);
    drop(p);

    // The old declaration again: a change in either direction rebuilds.
    let (p, reg) = s.register(&simple_decl("2")).unwrap();
    assert_eq!(reg.rebuilt, Rebuilt::DeclChanged);
    assert_eq!(rows(&p), (None, 0));
}

#[test]
fn another_format_rebuilds() {
    let d = tmp();
    let s = store(d.path());
    let (p, _) = s.register(&simple_decl("1")).unwrap();
    fill(&p);
    drop(p);
    // A file written by a future engine: its format byte differs.
    {
        let db = redb::Database::open(d.path().join("p.redb")).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut t = txn
                .open_table(redb::TableDefinition::<&str, &[u8]>::new("meta"))
                .unwrap();
            t.insert("format", 99u32.to_be_bytes().as_slice()).unwrap();
        }
        txn.commit().unwrap();
    }
    let (p, reg) = s.register(&simple_decl("1")).unwrap();
    assert_eq!(reg.rebuilt, Rebuilt::FormatChanged);
    assert_eq!(rows(&p), (None, 0));
}

#[test]
fn a_file_from_another_chron_database_is_refused_or_rebuilt() {
    let d = tmp();
    let s = store(d.path());
    let (p, _) = s.register(&simple_decl("1")).unwrap();
    fill(&p);
    drop(p);
    let other = Uuid::from_u128(0x9999);
    let s2 = facet::Store::open(d.path(), other, opts()).unwrap();
    match s2.register(&simple_decl("1")) {
        Err(Error::WrongDatabase {
            bound, expected, ..
        }) => {
            assert_eq!(bound, DB);
            assert_eq!(expected, other);
        }
        other => panic!("{other:?}"),
    }
    // Refused means untouched.
    let (p, reg) = s.register(&simple_decl("1")).unwrap();
    assert_eq!(reg.rebuilt, Rebuilt::No);
    assert_eq!(rows(&p), (Some("v".into()), 1));
    drop(p);

    let s3 = facet::Store::open(
        d.path(),
        other,
        opts().on_db_mismatch(OnDbMismatch::Rebuild),
    )
    .unwrap();
    let (p, reg) = s3.register(&simple_decl("1")).unwrap();
    assert_eq!(reg.rebuilt, Rebuilt::DatabaseChanged);
    assert_eq!(rows(&p), (None, 0));
    drop(p);
    // And now it is bound to the other id.
    assert!(matches!(
        s.register(&simple_decl("1")),
        Err(Error::WrongDatabase { .. })
    ));
}

#[test]
fn a_file_holding_another_projection_is_corrupt() {
    let d = tmp();
    let s = store(d.path());
    let (p, _) = s.register(&simple_decl("1")).unwrap();
    drop(p);
    std::fs::rename(d.path().join("p.redb"), d.path().join("q.redb")).unwrap();
    let mut q = simple_decl("1");
    q.name = "q".into();
    assert!(matches!(s.register(&q), Err(Error::Corrupt { .. })));
}

#[test]
fn rebuild_drops_the_collections_and_zeroes_the_checkpoint() {
    let d = tmp();
    let s = store(d.path());
    let (p, _) = s.register(&simple_decl("1")).unwrap();
    fill(&p);
    let mut rx = p.subscribe();
    let _ = rx.try_recv();
    p.rebuild().unwrap();
    assert_eq!(p.checkpoint().next, 0);
    assert_eq!(rows(&p), (None, 0));
    let summary = rx.try_recv().unwrap();
    assert_eq!(summary.checkpoint.next, 0);
    assert_eq!(summary.collection("r"), Some(&Changed::All));
    assert_eq!(summary.collection("m"), Some(&Changed::All));
    // The declaration is kept, so a reopen does not rebuild again.
    fill(&p);
    drop(p);
    let (_, reg) = s.register(&simple_decl("1")).unwrap();
    assert_eq!(reg.rebuilt, Rebuilt::No);
    assert_eq!(reg.checkpoint, cp(10));
}
