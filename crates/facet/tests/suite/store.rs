//! The store: registration, batches, snapshots, scans, the watch feeds.

use std::collections::BTreeMap;

use facet::{
    ApplyError, Changed, Cursor, Error, Fsync, KeyKind, Options, Rebuilt, TypeSchema, Uuid, Value,
    ValueSchema,
};

use crate::common::*;

#[test]
fn a_fresh_projection_starts_at_zero_and_keeps_what_it_commits() {
    let d = tmp();
    let s = store(d.path());
    let (p, reg) = s.register(&simple_decl("1")).unwrap();
    assert_eq!(reg.rebuilt, Rebuilt::Fresh);
    assert_eq!(reg.checkpoint.next, 0);
    assert_eq!(p.checkpoint().next, 0);
    assert!(d.path().join("p.redb").is_file());

    let n = p
        .apply::<_, Error>(cp(3), |w| {
            w.register("r")?.set(&"hello")?;
            let mut m = w.map::<u64, String>("m")?;
            m.put(&1, &"one".into())?;
            m.put(&2, &"two".into())?;
            assert_eq!(
                m.get(&1)?.as_deref(),
                Some("one"),
                "the batch sees its own writes"
            );
            assert!(m.remove(&2)?);
            assert!(!m.remove(&2)?);
            m.contains(&1)
        })
        .unwrap();
    assert!(n);
    assert_eq!(p.checkpoint(), cp(3));

    let snap = p.snapshot().unwrap();
    assert_eq!(snap.checkpoint(), cp(3));
    assert_eq!(
        snap.register::<String>("r")
            .unwrap()
            .get()
            .unwrap()
            .as_deref(),
        Some("hello")
    );
    let m = snap.map::<u64, String>("m").unwrap();
    assert_eq!(m.get(&1).unwrap().as_deref(), Some("one"));
    assert_eq!(m.get(&2).unwrap(), None);
    assert_eq!(m.len().unwrap(), 1);

    // Reopened: same declaration, nothing rebuilt, checkpoint and rows kept.
    drop((snap, p));
    let (p, reg) = s.register(&simple_decl("1")).unwrap();
    assert_eq!(reg.rebuilt, Rebuilt::No);
    assert_eq!(reg.checkpoint, cp(3));
    let snap = p.snapshot().unwrap();
    assert_eq!(
        snap.map::<u64, String>("m")
            .unwrap()
            .get(&1)
            .unwrap()
            .as_deref(),
        Some("one")
    );
}

#[test]
fn a_failed_or_panicking_batch_leaves_the_file_as_it_was() {
    let d = tmp();
    let (_s, p) = open_simple(d.path());
    p.apply::<_, Error>(cp(1), |w| w.map::<u64, i64>("m")?.put(&1, &10))
        .unwrap();
    let before = p.checkpoint();

    let err = p
        .apply(cp(2), |w| {
            w.map::<u64, i64>("m").unwrap().put(&2, &20).unwrap();
            w.register("r").unwrap().set(&1).unwrap();
            Err::<(), _>("no")
        })
        .unwrap_err();
    assert!(matches!(err, ApplyError::Handler("no")));
    let err = p
        .apply::<(), Error>(cp(2), |w| {
            w.map::<u64, i64>("m")?.put(&3, &30)?;
            panic!("boom");
        })
        .unwrap_err();
    assert!(
        matches!(err, ApplyError::Store(Error::Panicked(ref m)) if m == "boom"),
        "{err}"
    );
    // A later batch still works: the lock was not poisoned.
    assert_eq!(p.checkpoint(), before);
    let snap = p.snapshot().unwrap();
    let m = snap.map::<u64, i64>("m").unwrap();
    assert_eq!(m.get(&1).unwrap(), Some(10));
    assert_eq!(m.get(&2).unwrap(), None);
    assert_eq!(m.get(&3).unwrap(), None);
    assert_eq!(snap.register::<i64>("r").unwrap().get().unwrap(), None);
    p.apply::<_, Error>(cp(2), |w| w.map::<u64, i64>("m")?.put(&2, &20))
        .unwrap();
    assert_eq!(p.checkpoint(), cp(2));
}

#[test]
fn the_checkpoint_must_advance() {
    let d = tmp();
    let (_s, p) = open_simple(d.path());
    p.apply::<_, Error>(cp(5), |_| Ok(())).unwrap();
    for n in [1, 5] {
        let err = p
            .apply::<_, Error>(cp(n), |w| w.map::<u64, i64>("m")?.put(&1, &1))
            .unwrap_err();
        assert!(
            matches!(err, ApplyError::Store(Error::CheckpointNotAdvancing { current: 5, proposed }) if proposed == n),
            "{err}"
        );
    }
    assert_eq!(
        p.snapshot()
            .unwrap()
            .map::<u64, i64>("m")
            .unwrap()
            .len()
            .unwrap(),
        0
    );
    p.apply::<_, Error>(cp(6), |_| Ok(())).unwrap();
}

#[test]
fn handles_are_checked_against_the_declaration() {
    let d = tmp();
    let (_s, p) = open_simple(d.path());
    let err = p
        .apply::<(), Error>(cp(1), |w| {
            w.map::<u64, i64>("nope")?;
            Ok(())
        })
        .unwrap_err();
    assert!(matches!(err, ApplyError::Handler(Error::NotDeclared(ref n)) if n == "nope"));
    let err = p
        .apply::<(), Error>(cp(1), |w| {
            w.map::<String, i64>("m")?;
            Ok(())
        })
        .unwrap_err();
    assert!(
        matches!(err, ApplyError::Handler(Error::KindMismatch { .. })),
        "{err}"
    );
    let err = p
        .apply::<(), Error>(cp(1), |w| {
            w.register::<i64>("m")?;
            Ok(())
        })
        .unwrap_err();
    assert!(
        matches!(err, ApplyError::Handler(Error::KindMismatch { .. })),
        "{err}"
    );
    let snap = p.snapshot().unwrap();
    assert!(matches!(
        snap.map::<u64, i64>("nope"),
        Err(Error::NotDeclared(_))
    ));
    assert!(matches!(
        snap.map::<(u64, u64), i64>("m"),
        Err(Error::KindMismatch { .. })
    ));
}

#[test]
fn typed_values_are_validated_and_canonicalised() {
    let d = tmp();
    let s = store(d.path());
    let dec = decl(
        "t",
        "1",
        vec![map_decl("ids", &[KeyKind::Uint], typed(TypeSchema::Uuid))],
    );
    let (p, _) = s.register(&dec).unwrap();
    let id = Uuid::from_u128(42);
    p.apply::<_, Error>(cp(1), |w| {
        let mut m = w.map::<u64, Uuid>("ids")?;
        m.put(&1, &id)?; // serde gives a string; the schema makes it a uuid
        Ok(())
    })
    .unwrap();
    let err = p
        .apply::<(), Error>(cp(2), |w| {
            w.map::<u64, String>("ids")?
                .put(&2, &"not a uuid".to_string())?;
            Ok(())
        })
        .unwrap_err();
    assert!(
        matches!(err, ApplyError::Handler(Error::Validation(_))),
        "{err}"
    );
    let snap = p.snapshot().unwrap();
    assert_eq!(
        snap.map::<u64, Uuid>("ids").unwrap().get(&1).unwrap(),
        Some(id)
    );
    assert_eq!(
        snap.map::<u64, Uuid>("ids").unwrap().get_value(&1).unwrap(),
        Some(Value::Uuid(id))
    );
}

#[test]
fn scans_page_in_both_directions_within_a_prefix() {
    let d = tmp();
    let s = store(d.path());
    let dec = decl(
        "s",
        "1",
        vec![map_decl(
            "m",
            &[KeyKind::Str, KeyKind::Int],
            ValueSchema::Any,
        )],
    );
    let (p, _) = s.register(&dec).unwrap();
    let mut expected: BTreeMap<(String, i64), i64> = BTreeMap::new();
    p.apply::<_, Error>(cp(1), |w| {
        let mut m = w.map::<(String, i64), i64>("m")?;
        for part in ["a", "a\0", "b"] {
            for i in -3..3 {
                m.put(&(part.to_string(), i), &i)?;
                expected.insert((part.to_string(), i), i);
            }
        }
        Ok(())
    })
    .unwrap();
    let snap = p.snapshot().unwrap();
    let m = snap.map::<(String, i64), i64>("m").unwrap();
    assert_eq!(m.len().unwrap(), 18);

    // The whole thing, forward, in key order.
    let all = m.scan().run().unwrap();
    assert_eq!(all.items.len(), 18);
    assert!(all.next.is_none());
    assert_eq!(
        all.items,
        expected
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect::<Vec<_>>()
    );
    assert_eq!(all.checkpoint, cp(1));

    // A prefix: "a" alone, not "a\0".
    let a = m.scan().prefix(&("a".to_string(),)).run().unwrap();
    assert_eq!(a.items.len(), 6);
    assert!(a.items.iter().all(|((s, _), _)| s == "a"));
    assert_eq!(
        m.count_prefix(&("a".to_string(),), 100).unwrap(),
        (6, false)
    );
    assert_eq!(m.count_prefix(&("a".to_string(),), 4).unwrap(), (4, true));

    // Pages of 4, forward and reverse, concatenate to the full scans.
    for reverse in [false, true] {
        let mut got = Vec::new();
        let mut cursor: Option<Cursor> = None;
        let mut pages = 0;
        loop {
            let mut sc = m.scan().limit(4);
            if reverse {
                sc = sc.reverse();
            }
            if let Some(c) = &cursor {
                sc = sc.after(c);
            }
            let page = sc.run().unwrap();
            pages += 1;
            assert!(page.items.len() <= 4);
            got.extend(page.items);
            match page.next {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        assert_eq!(pages, 5);
        let mut want = all.items.clone();
        if reverse {
            want.reverse();
        }
        assert_eq!(got, want, "reverse {reverse}");
    }

    // Bounds are inclusive.
    let r = m
        .scan()
        .from(&("a".to_string(), -1))
        .to(&("a\0".to_string(), 0))
        .run()
        .unwrap();
    assert_eq!(r.items.first().unwrap().0, ("a".to_string(), -1));
    assert_eq!(r.items.last().unwrap().0, ("a\0".to_string(), 0));
    assert_eq!(r.items.len(), 8);

    // A cursor from another prefix is refused.
    let b_first = m.scan().prefix(&("b".to_string(),)).limit(1).run().unwrap();
    let c = b_first.next.unwrap();
    assert!(matches!(
        m.scan().prefix(&("a".to_string(),)).after(&c).run(),
        Err(Error::InvalidCursor(_))
    ));
    // And a prefix that is not one of the key.
    assert!(matches!(
        m.scan().prefix(&(1u64,)).run(),
        Err(Error::Key(_))
    ));
    // The last page's `next` is None exactly when the set is exhausted.
    let exact = m.scan().prefix(&("b".to_string(),)).limit(6).run().unwrap();
    assert_eq!(exact.items.len(), 6);
    assert!(exact.next.is_none());
    let short = m.scan().prefix(&("b".to_string(),)).limit(5).run().unwrap();
    assert!(short.next.is_some());
}

#[test]
fn a_projection_cannot_be_registered_twice_or_dropped_while_open() {
    let d = tmp();
    let s = store(d.path());
    let (p, _) = s.register(&simple_decl("1")).unwrap();
    assert!(matches!(
        s.register(&simple_decl("1")),
        Err(Error::InUse(_))
    ));
    assert!(matches!(s.drop_projection("p"), Err(Error::InUse(_))));
    drop(p);
    assert!(s.drop_projection("p").unwrap());
    assert!(!s.drop_projection("p").unwrap());
    assert!(!d.path().join("p.redb").exists());
    let (_, reg) = s.register(&simple_decl("1")).unwrap();
    assert_eq!(reg.rebuilt, Rebuilt::Fresh);
}

#[test]
fn list_shows_every_file_open_or_not() {
    let d = tmp();
    let s = store(d.path());
    let (p, _) = s.register(&simple_decl("1")).unwrap();
    p.apply::<_, Error>(cp(4), |_| Ok(())).unwrap();
    let (q, _) = s
        .register(&decl("q", "7", vec![register_decl("r")]))
        .unwrap();
    drop(q);
    let list = s.list().unwrap();
    let names: Vec<_> = list.iter().map(|i| i.name.as_str()).collect();
    assert_eq!(names, ["p", "q"]);
    assert_eq!(list[0].checkpoint, cp(4));
    assert!(list[0].open);
    assert_eq!(list[1].version, "7");
    assert!(!list[1].open);
    assert!(list[1].bytes > 0);
}

#[test]
fn the_watch_feeds_report_every_committed_batch_in_order() {
    let d = tmp();
    let s = store_with(d.path(), opts().summary_max_keys(3));
    let (p, _) = s.register(&simple_decl("1")).unwrap();
    let mut cps = p.watch_checkpoint();
    let mut rx = p.subscribe();
    assert_eq!(cps.borrow_and_update().next, 0);

    p.apply::<_, Error>(cp(1), |w| {
        let mut m = w.map::<u64, i64>("m")?;
        m.put(&2, &2)?;
        m.put(&1, &1)?;
        m.put(&2, &3)?;
        Ok(())
    })
    .unwrap();
    // An aborted batch publishes nothing.
    let _ = p.apply(cp(2), |w| {
        w.map::<u64, i64>("m").unwrap().put(&9, &9).unwrap();
        Err::<(), &str>("no")
    });
    p.apply::<_, Error>(cp(2), |w| {
        let mut m = w.map::<u64, i64>("m")?;
        for k in 10..14 {
            m.put(&k, &0)?;
        }
        w.register("r")?.set(&1)?;
        Ok(())
    })
    .unwrap();

    let first = rx.try_recv().unwrap();
    assert_eq!(first.checkpoint, cp(1));
    assert_eq!(
        first.collection("m"),
        Some(&Changed::Keys(vec![
            1u64.to_key().into(),
            2u64.to_key().into()
        ]))
    );
    assert_eq!(first.collection("r"), None);
    let second = rx.try_recv().unwrap();
    assert_eq!(second.checkpoint, cp(2));
    assert_eq!(
        second.collection("m"),
        Some(&Changed::All),
        "4 keys > max 3"
    );
    assert_eq!(
        second.collection("r"),
        Some(&Changed::Keys(vec![bytes::Bytes::new()]))
    );
    assert!(rx.try_recv().is_err());
    assert!(cps.has_changed().unwrap());
    assert_eq!(cps.borrow_and_update().next, 2);

    // A receiver that fell behind is told so, and a snapshot taken then is
    // at least as far as what it missed.
    let mut slow = p.subscribe();
    for n in 3..3 + 70 {
        p.apply::<_, Error>(cp(n), |_| Ok(())).unwrap();
    }
    assert!(matches!(
        slow.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_))
    ));
    assert_eq!(p.snapshot().unwrap().checkpoint(), cp(72));
}

use facet::KeyType;

#[test]
fn fsync_always_and_flush_work() {
    let d = tmp();
    let s = store_with(d.path(), Options::new().fsync(Fsync::Always));
    let (p, _) = s.register(&simple_decl("1")).unwrap();
    p.apply::<_, Error>(cp(1), |w| w.register("r")?.set(&5))
        .unwrap();
    s.flush().unwrap();
    p.flush().unwrap();
    drop(p);
    let (p, reg) = s.register(&simple_decl("1")).unwrap();
    assert_eq!(reg.checkpoint, cp(1));
    assert_eq!(
        p.snapshot()
            .unwrap()
            .register::<i64>("r")
            .unwrap()
            .get()
            .unwrap(),
        Some(5)
    );
}

#[test]
fn limits_fail_the_batch_and_keep_the_previous_state() {
    let d = tmp();
    let s = store(d.path());
    let dec = decl(
        "l",
        "1",
        vec![map_decl("m", &[KeyKind::Bytes], ValueSchema::Any)],
    );
    let (p, _) = s.register(&dec).unwrap();
    p.apply::<_, Error>(cp(1), |w| w.map::<Vec<u8>, i64>("m")?.put(&vec![1], &1))
        .unwrap();
    let err = p
        .apply::<(), Error>(cp(2), |w| {
            let mut m = w.map::<Vec<u8>, i64>("m")?;
            m.put(&vec![2], &2)?;
            m.put(&vec![0u8; facet::limits::MAX_KEY_BYTES], &3)?; // 1024 + terminator
            Ok(())
        })
        .unwrap_err();
    assert!(matches!(err, ApplyError::Handler(Error::Limit(_))), "{err}");
    let err = p
        .apply::<(), Error>(cp(2), |w| {
            w.map::<Vec<u8>, Vec<u8>>("m")?
                .put(&vec![3], &vec![0u8; facet::limits::MAX_VALUE_BYTES + 1])?;
            Ok(())
        })
        .unwrap_err();
    assert!(matches!(err, ApplyError::Handler(Error::Limit(_))), "{err}");
    let snap = p.snapshot().unwrap();
    assert_eq!(snap.checkpoint(), cp(1));
    assert_eq!(snap.map::<Vec<u8>, i64>("m").unwrap().len().unwrap(), 1);
}
