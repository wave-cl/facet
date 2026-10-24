//! Every collection kind: its operations, its scans, its errors.

use std::collections::BTreeMap;

use facet::{
    ApplyError, Bucket, CollectionDecl, CollectionKind, ColumnDecl, Decimal, Error, IndexDecl,
    KeyKind, KeySchema, NumKind, ScoreKind, Timestamp, TypeSchema, Uuid, ValueSchema, Width,
    window_start,
};
use serde::{Deserialize, Serialize};

use crate::common::*;

fn col(name: &str, ty: TypeSchema, nullable: bool) -> ColumnDecl {
    ColumnDecl {
        name: name.into(),
        ty,
        nullable,
    }
}

/// Every kind, declared once.
pub fn all_kinds_decl(version: &str) -> facet::ProjectionDecl {
    decl(
        "all",
        version,
        vec![
            register_decl("r"),
            map_decl("m", &[KeyKind::Uint], ValueSchema::Any),
            CollectionDecl {
                name: "s".into(),
                kind: CollectionKind::Set {
                    member: KeySchema(vec![KeyKind::Str, KeyKind::Int]),
                },
            },
            CollectionDecl {
                name: "c".into(),
                kind: CollectionKind::Counter {
                    key: KeySchema(vec![]),
                    num: NumKind::Int,
                },
            },
            CollectionDecl {
                name: "cd".into(),
                kind: CollectionKind::Counter {
                    key: KeySchema(vec![KeyKind::Str]),
                    num: NumKind::Decimal,
                },
            },
            CollectionDecl {
                name: "l".into(),
                kind: CollectionKind::List {
                    partition: KeySchema(vec![KeyKind::Str]),
                    value: ValueSchema::Any,
                },
            },
            CollectionDecl {
                name: "t".into(),
                kind: CollectionKind::Table {
                    columns: vec![
                        col("id", TypeSchema::Uuid, false),
                        col("account", TypeSchema::Uuid, false),
                        col("amount", TypeSchema::Decimal, false),
                        col("note", TypeSchema::Str, true),
                        col("tags", TypeSchema::List(Box::new(TypeSchema::Str)), false),
                        col("code", TypeSchema::Int, false),
                    ],
                    primary_key: vec!["id".into()],
                    indexes: vec![
                        IndexDecl {
                            name: "by_account".into(),
                            columns: vec!["account".into(), "amount".into()],
                            unique: false,
                        },
                        IndexDecl {
                            name: "by_note".into(),
                            columns: vec!["note".into()],
                            unique: false,
                        },
                        IndexDecl {
                            name: "by_code".into(),
                            columns: vec!["code".into()],
                            unique: true,
                        },
                    ],
                },
            },
            CollectionDecl {
                name: "z".into(),
                kind: CollectionKind::SortedSet {
                    partition: KeySchema(vec![KeyKind::Str]),
                    member: KeySchema(vec![KeyKind::Uuid]),
                    score: ScoreKind::Int,
                },
            },
            CollectionDecl {
                name: "b".into(),
                kind: CollectionKind::Buckets {
                    key: KeySchema(vec![KeyKind::Str]),
                    num: NumKind::Int,
                    widths: vec![Width::Minute, Width::Hour],
                },
            },
        ],
    )
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Txn {
    pub id: Uuid,
    pub account: Uuid,
    pub amount: Decimal,
    pub note: Option<String>,
    pub tags: Vec<String>,
    pub code: i64,
}

fn txn(id: u128, account: u128, amount: &str, note: Option<&str>, code: i64) -> Txn {
    Txn {
        id: Uuid::from_u128(id),
        account: Uuid::from_u128(account),
        amount: amount.parse().unwrap(),
        note: note.map(String::from),
        tags: vec!["t".into()],
        code,
    }
}

fn open_all(dir: &std::path::Path) -> (facet::Store, facet::ProjectionData) {
    let s = store(dir);
    let (p, _) = s.register(&all_kinds_decl("1")).unwrap();
    (s, p)
}

#[test]
fn sets_hold_members_once() {
    let d = tmp();
    let (_s, p) = open_all(d.path());
    let r = p
        .apply::<_, Error>(cp(1), |w| {
            let mut s = w.set::<(String, i64)>("s")?;
            assert!(s.insert(&("a".into(), 1))?);
            assert!(!s.insert(&("a".into(), 1))?);
            assert!(s.insert(&("a".into(), -1))?);
            assert!(s.insert(&("b".into(), 0))?);
            assert!(s.contains(&("a".into(), 1))?);
            assert!(s.remove(&("b".into(), 0))?);
            assert!(!s.remove(&("b".into(), 0))?);
            s.contains(&("b".into(), 0))
        })
        .unwrap();
    assert!(!r);
    let snap = p.snapshot().unwrap();
    let s = snap.set::<(String, i64)>("s").unwrap();
    assert_eq!(s.len().unwrap(), 2);
    assert!(s.contains(&("a".into(), -1)).unwrap());
    assert!(!s.contains(&("b".into(), 0)).unwrap());
    let all = s.scan().run().unwrap().items;
    assert_eq!(all, vec![("a".to_string(), -1), ("a".to_string(), 1)]);
    assert_eq!(
        s.scan().prefix(&("a".to_string(),)).count(10).unwrap(),
        (2, false)
    );
    assert!(matches!(
        snap.set::<(String,)>("s"),
        Err(Error::KindMismatch { .. })
    ));
}

#[test]
fn counters_add_and_overflow_fails_the_batch() {
    let d = tmp();
    let (_s, p) = open_all(d.path());
    p.apply::<_, Error>(cp(1), |w| {
        let mut c = w.counter::<(), i64>("c")?;
        assert_eq!(c.get(&())?, None);
        assert_eq!(c.add(&(), 5)?, 5);
        assert_eq!(c.add(&(), -2)?, 3);
        let mut cd = w.counter::<String, Decimal>("cd")?;
        assert_eq!(
            cd.add(&"x".into(), Decimal::new(15, 1))?,
            Decimal::new(15, 1)
        );
        assert_eq!(
            cd.add(&"x".into(), Decimal::new(25, 2))?,
            Decimal::new(175, 2)
        );
        cd.set(&"y".into(), Decimal::ONE)?;
        assert!(cd.remove(&"y".into())?);
        Ok(())
    })
    .unwrap();
    let err = p
        .apply::<(), Error>(cp(2), |w| {
            w.counter::<(), i64>("c")?.add(&(), i64::MAX)?;
            Ok(())
        })
        .unwrap_err();
    assert!(
        matches!(err, ApplyError::Handler(Error::Overflow(_))),
        "{err}"
    );
    let snap = p.snapshot().unwrap();
    assert_eq!(
        snap.counter::<(), i64>("c").unwrap().get(&()).unwrap(),
        Some(3)
    );
    let cd = snap.counter::<String, Decimal>("cd").unwrap();
    assert_eq!(cd.get(&"x".into()).unwrap(), Some(Decimal::new(175, 2)));
    assert_eq!(cd.len().unwrap(), 1);
    assert_eq!(
        cd.scan().run().unwrap().items,
        vec![("x".to_string(), Decimal::new(175, 2))]
    );
    assert!(matches!(
        snap.counter::<(), Decimal>("c"),
        Err(Error::KindMismatch { .. })
    ));
}

#[test]
fn lists_keep_order_across_pushes_removes_and_truncation() {
    let d = tmp();
    let (_s, p) = open_all(d.path());
    let part = "feed".to_string();
    p.apply::<_, Error>(cp(1), |w| {
        let mut l = w.list::<String, String>("l")?;
        assert_eq!(l.push_back(&part, &"b".into())?, 0);
        assert_eq!(l.push_back(&part, &"c".into())?, 1);
        assert_eq!(l.push_front(&part, &"a".into())?, -1);
        assert_eq!(l.len(&part)?, 3);
        assert_eq!(l.get(&part, 1)?.as_deref(), Some("c"));
        assert!(l.set(&part, 1, &"C".into())?);
        assert!(!l.set(&part, 7, &"x".into())?);
        assert_eq!(l.push_many(&part, &["d".to_string(), "e".to_string()])?, 2);
        assert!(l.remove(&part, 0)?); // "b" leaves a hole
        assert!(!l.remove(&part, 0)?);
        assert_eq!(l.len(&part)?, 4);
        // another partition is independent
        l.push_back(&"other".into(), &"o".into())?;
        Ok(())
    })
    .unwrap();
    let snap = p.snapshot().unwrap();
    let l = snap.list::<String, String>("l").unwrap();
    assert_eq!(l.len(&part).unwrap(), 4);
    let items = l.range(&part).unwrap().run().unwrap().items;
    assert_eq!(
        items,
        vec![
            (-1, "a".to_string()),
            (1, "C".to_string()),
            (2, "d".to_string()),
            (3, "e".to_string())
        ]
    );
    assert_eq!(
        l.range(&part)
            .unwrap()
            .reverse()
            .limit(2)
            .run()
            .unwrap()
            .items,
        vec![(3, "e".to_string()), (2, "d".to_string())]
    );
    assert_eq!(l.first(&part).unwrap(), Some((-1, "a".to_string())));
    assert_eq!(l.last(&part).unwrap(), Some((3, "e".to_string())));
    assert_eq!(l.get(&part, 0).unwrap(), None);
    assert_eq!(l.len(&"other".into()).unwrap(), 1);
    assert_eq!(l.len(&"none".into()).unwrap(), 0);
    drop(snap);

    p.apply::<_, Error>(cp(2), |w| {
        let mut l = w.list::<String, String>("l")?;
        assert_eq!(l.truncate_front(&part, 2)?, 2); // drops a, C
        assert_eq!(l.truncate_front(&part, 5)?, 0);
        assert_eq!(l.truncate_back(&part, 1)?, 1); // drops e
        assert_eq!(l.len(&part)?, 1);
        assert_eq!(l.push_back(&part, &"f".into())?, 4, "numbering continues");
        assert_eq!(l.clear(&"other".into())?, 1);
        assert_eq!(l.clear(&"other".into())?, 0);
        assert_eq!(
            l.push_back(&"other".into(), &"o2".into())?,
            0,
            "cleared lists start over"
        );
        Ok(())
    })
    .unwrap();
    let snap = p.snapshot().unwrap();
    let l = snap.list::<String, String>("l").unwrap();
    assert_eq!(
        l.range(&part).unwrap().run().unwrap().items,
        vec![(2, "d".to_string()), (4, "f".to_string())]
    );
    assert_eq!(
        l.range(&"other".into()).unwrap().run().unwrap().items,
        vec![(0, "o2".to_string())]
    );
}

#[test]
fn tables_validate_rows_and_maintain_their_indexes() {
    let d = tmp();
    let (_s, p) = open_all(d.path());
    let a = Uuid::from_u128(0xa);
    let b = Uuid::from_u128(0xb);
    p.apply::<_, Error>(cp(1), |w| {
        let mut t = w.table::<Uuid, Txn>("t")?;
        t.upsert(&txn(1, 0xa, "10.50", Some("rent"), 1))?;
        t.upsert(&txn(2, 0xa, "3", None, 2))?;
        t.upsert(&txn(3, 0xb, "7.25", Some("food"), 3))?;
        assert_eq!(
            t.get(&Uuid::from_u128(2))?.unwrap().amount,
            Decimal::from(3)
        );
        assert!(t.contains(&Uuid::from_u128(3))?);
        Ok(())
    })
    .unwrap();
    let snap = p.snapshot().unwrap();
    let t = snap.table::<Uuid, Txn>("t").unwrap();
    assert_eq!(t.len().unwrap(), 3);
    assert_eq!(
        t.get(&Uuid::from_u128(1)).unwrap().unwrap().note.as_deref(),
        Some("rent")
    );
    let vals = t.get_values(&Uuid::from_u128(2)).unwrap().unwrap();
    assert_eq!(vals["note"], facet::Value::Null);
    assert_eq!(vals["amount"], facet::Value::Decimal(Decimal::from(3)));
    // by account, amounts ascending within it
    let rows = t
        .by_index::<(Uuid, Decimal)>("by_account")
        .unwrap()
        .prefix(&(a,))
        .run()
        .unwrap()
        .items;
    assert_eq!(
        rows.iter().map(|r| r.id.as_u128()).collect::<Vec<_>>(),
        vec![2, 1]
    );
    let rows = t
        .by_index::<(Uuid, Decimal)>("by_account")
        .unwrap()
        .prefix(&(b,))
        .run()
        .unwrap()
        .items;
    assert_eq!(
        rows.iter().map(|r| r.id.as_u128()).collect::<Vec<_>>(),
        vec![3]
    );
    // a nullable column indexes nulls first, typed as Option
    let notes = t
        .by_index::<(Option<String>,)>("by_note")
        .unwrap()
        .run()
        .unwrap()
        .items;
    assert_eq!(
        notes.iter().map(|r| r.id.as_u128()).collect::<Vec<_>>(),
        vec![2, 3, 1]
    );
    let null_only = t
        .by_index::<(Option<String>,)>("by_note")
        .unwrap()
        .prefix(&(None::<String>,))
        .run()
        .unwrap()
        .items;
    assert_eq!(null_only.len(), 1);
    // the index key must match, nullability included
    assert!(matches!(
        t.by_index::<(String,)>("by_note"),
        Err(Error::KindMismatch { .. })
    ));
    assert!(matches!(
        t.by_index::<(Uuid,)>("by_account"),
        Err(Error::KindMismatch { .. })
    ));
    assert!(matches!(
        t.by_index::<(i64,)>("nope"),
        Err(Error::NotDeclared(_))
    ));
    // range over the index
    let mid = t
        .by_index::<(Uuid, Decimal)>("by_account")
        .unwrap()
        .from(&(a, Decimal::from(5)))
        .to_prefix(&(b,))
        .run()
        .unwrap()
        .items;
    assert_eq!(
        mid.iter().map(|r| r.id.as_u128()).collect::<Vec<_>>(),
        vec![1, 3]
    );
    drop(snap);

    // Updating an indexed column moves the entry; deleting removes it.
    p.apply::<_, Error>(cp(2), |w| {
        let mut t = w.table::<Uuid, Txn>("t")?;
        t.upsert(&txn(1, 0xb, "10.50", None, 1))?; // account a -> b, note -> null
        assert!(t.delete(&Uuid::from_u128(3))?);
        assert!(!t.delete(&Uuid::from_u128(3))?);
        t.update(&Uuid::from_u128(2), |r| {
            let mut r = r.unwrap();
            r.amount = Decimal::from(99);
            Some(r)
        })?;
        Ok(())
    })
    .unwrap();
    let snap = p.snapshot().unwrap();
    let t = snap.table::<Uuid, Txn>("t").unwrap();
    assert_eq!(t.len().unwrap(), 2);
    let by_a = t
        .by_index::<(Uuid, Decimal)>("by_account")
        .unwrap()
        .prefix(&(a,))
        .run()
        .unwrap()
        .items;
    assert_eq!(
        by_a.iter().map(|r| r.id.as_u128()).collect::<Vec<_>>(),
        vec![2]
    );
    assert_eq!(by_a[0].amount, Decimal::from(99));
    let by_b = t
        .by_index::<(Uuid, Decimal)>("by_account")
        .unwrap()
        .prefix(&(b,))
        .run()
        .unwrap()
        .items;
    assert_eq!(
        by_b.iter().map(|r| r.id.as_u128()).collect::<Vec<_>>(),
        vec![1]
    );
    let nulls = t
        .by_index::<(Option<String>,)>("by_note")
        .unwrap()
        .prefix(&(None::<String>,))
        .run()
        .unwrap()
        .items;
    assert_eq!(
        nulls.iter().map(|r| r.id.as_u128()).collect::<Vec<_>>(),
        vec![1, 2]
    );
    // No index entry survives for the deleted row.
    for ix in ["by_account", "by_note", "by_code"] {
        let raw = redb::Database::open(d.path().join("all.redb"));
        drop(raw); // the file is open by the store; count through the index scans instead
        let _ = ix;
    }
    assert_eq!(
        t.by_index::<(i64,)>("by_code")
            .unwrap()
            .run()
            .unwrap()
            .items
            .len(),
        2
    );
    drop(snap);

    // Validation: a wrong type, an unknown column, a null in a non-null
    // column, a unique violation; each fails the batch.
    #[derive(Serialize)]
    struct Loose {
        id: Uuid,
        account: Uuid,
        amount: String,
        tags: Vec<String>,
        code: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        extra: Option<u8>,
    }
    let base = || Loose {
        id: Uuid::from_u128(9),
        account: a,
        amount: "1".into(),
        tags: vec![],
        code: 9,
        extra: None,
    };
    let cases: Vec<(Loose, &str)> = vec![
        (
            Loose {
                amount: "abc".into(),
                ..base()
            },
            "column amount",
        ),
        (
            Loose {
                extra: Some(1),
                ..base()
            },
            "is not a column",
        ),
        (Loose { code: 1, ..base() }, "unique"),
    ];
    for (row, needle) in cases {
        let err = p
            .apply::<(), Error>(cp(3), |w| {
                w.table::<Uuid, Loose>("t")?.upsert(&row)?;
                Ok(())
            })
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains(needle), "{msg:?} lacks {needle:?}");
    }
    #[derive(Serialize)]
    struct NoAccount {
        id: Uuid,
        amount: String,
        tags: Vec<String>,
        code: i64,
    }
    let err = p
        .apply::<(), Error>(cp(3), |w| {
            w.table::<Uuid, NoAccount>("t")?.upsert(&NoAccount {
                id: Uuid::from_u128(9),
                amount: "1".into(),
                tags: vec![],
                code: 9,
            })?;
            Ok(())
        })
        .unwrap_err();
    assert!(err.to_string().contains("column account is null"), "{err}");
    // A row may replace itself under the same unique value.
    p.apply::<_, Error>(cp(3), |w| {
        w.table::<Uuid, Txn>("t")?
            .upsert(&txn(1, 0xb, "11", None, 1))
    })
    .unwrap();
    assert_eq!(
        p.snapshot()
            .unwrap()
            .table::<Uuid, Txn>("t")
            .unwrap()
            .len()
            .unwrap(),
        2
    );
}

#[test]
fn sorted_sets_rank_by_score_then_member() {
    let d = tmp();
    let (_s, p) = open_all(d.path());
    let u = |n: u128| Uuid::from_u128(n);
    let part = "board".to_string();
    p.apply::<_, Error>(cp(1), |w| {
        let mut z = w.sorted_set::<String, Uuid, i64>("z")?;
        assert_eq!(z.put(&part, &u(1), 10)?, None);
        assert_eq!(z.put(&part, &u(2), 30)?, None);
        assert_eq!(z.put(&part, &u(3), 20)?, None);
        assert_eq!(z.put(&part, &u(4), 20)?, None);
        assert_eq!(z.put(&part, &u(1), 25)?, Some(10)); // moves
        assert_eq!(z.score(&part, &u(1))?, Some(25));
        assert_eq!(z.remove(&part, &u(4))?, Some(20));
        assert_eq!(z.remove(&part, &u(4))?, None);
        z.put(&"other".into(), &u(9), 1)?;
        Ok(())
    })
    .unwrap();
    let snap = p.snapshot().unwrap();
    let z = snap.sorted_set::<String, Uuid, i64>("z").unwrap();
    assert_eq!(z.len().unwrap(), 4);
    let asc = z.by_score(&part).unwrap().run().unwrap().items;
    assert_eq!(asc, vec![(u(3), 20), (u(1), 25), (u(2), 30)]);
    assert_eq!(z.top(&part, 2).unwrap().items, vec![(u(2), 30), (u(1), 25)]);
    assert_eq!(
        z.by_score(&part)
            .unwrap()
            .from_prefix(&(21i64,))
            .to_prefix(&(30i64,))
            .run()
            .unwrap()
            .items,
        vec![(u(1), 25), (u(2), 30)]
    );
    assert_eq!(
        z.by_score(&part)
            .unwrap()
            .from_prefix(&(21i64,))
            .to_prefix(&(29i64,))
            .run()
            .unwrap()
            .items,
        vec![(u(1), 25)]
    );
    assert_eq!(
        z.members(&part).unwrap().run().unwrap().items,
        vec![(u(1), 25), (u(2), 30), (u(3), 20)]
    );
    assert_eq!(z.rank(&part, &u(3), 100).unwrap(), Some(0));
    assert_eq!(z.rank(&part, &u(2), 100).unwrap(), Some(2));
    assert_eq!(z.rank(&part, &u(2), 2).unwrap(), None, "past the cap");
    assert_eq!(z.rank(&part, &u(4), 100).unwrap(), None);
    assert_eq!(z.score(&"other".into(), &u(9)).unwrap(), Some(1));
    assert_eq!(z.top(&"none".into(), 5).unwrap().items, vec![]);
    assert!(matches!(
        snap.sorted_set::<String, Uuid, Decimal>("z"),
        Err(Error::KindMismatch { .. })
    ));
}

#[test]
fn buckets_roll_values_into_every_declared_width() {
    let d = tmp();
    let (_s, p) = open_all(d.path());
    let min = Width::Minute.nanos() as i64;
    let hour = Width::Hour.nanos() as i64;
    let k = "deposits".to_string();
    p.apply::<_, Error>(cp(1), |w| {
        let mut b = w.buckets::<String, i64>("b")?;
        b.add(&k, 10, 5)?;
        b.add(&k, min - 1, 7)?;
        b.add(&k, min, 1)?;
        b.add(&k, hour + 3, 100)?;
        b.add(&k, -1, 2)?; // the window before the epoch
        assert_eq!(
            b.get(&k, Width::Minute, 0)?,
            Some(Bucket {
                start: 0,
                count: 2,
                sum: 12,
                min: 5,
                max: 7
            })
        );
        assert!(matches!(
            b.get(&k, Width::Day, 0),
            Err(Error::Validation(_))
        ));
        Ok(())
    })
    .unwrap();
    assert_eq!(window_start(-1, Width::Minute), -min);
    assert_eq!(window_start(hour + 3, Width::Hour), hour);
    let snap = p.snapshot().unwrap();
    let b = snap.buckets::<String, i64>("b").unwrap();
    assert_eq!(b.widths(), &[Width::Minute, Width::Hour]);
    let minutes = b.range(&k, Width::Minute).unwrap().run().unwrap().items;
    assert_eq!(
        minutes
            .iter()
            .map(|b| (b.start, b.count, b.sum))
            .collect::<Vec<_>>(),
        vec![(-min, 1, 2), (0, 2, 12), (min, 1, 1), (hour, 1, 100)]
    );
    let hours = b.range(&k, Width::Hour).unwrap().run().unwrap().items;
    assert_eq!(
        hours
            .iter()
            .map(|b| (b.start, b.count, b.sum, b.min, b.max))
            .collect::<Vec<_>>(),
        vec![
            (-hour, 1, 2, 2, 2),
            (0, 3, 13, 1, 7),
            (hour, 1, 100, 100, 100)
        ]
    );
    assert_eq!(
        b.range(&k, Width::Minute)
            .unwrap()
            .from(&(0,))
            .to(&(min,))
            .run()
            .unwrap()
            .items
            .len(),
        2
    );
    assert_eq!(b.at(&k, Width::Hour, 59 * min).unwrap().unwrap().count, 3);
    assert_eq!(b.get(&"none".into(), Width::Hour, 0).unwrap(), None);
    assert!(matches!(
        snap.buckets::<String, Decimal>("b"),
        Err(Error::KindMismatch { .. })
    ));
}

#[test]
fn timestamps_and_decimals_score_sorted_sets() {
    let d = tmp();
    let s = store(d.path());
    let dec = decl(
        "ts",
        "1",
        vec![
            CollectionDecl {
                name: "recent".into(),
                kind: CollectionKind::SortedSet {
                    partition: KeySchema(vec![]),
                    member: KeySchema(vec![KeyKind::Str]),
                    score: ScoreKind::Timestamp,
                },
            },
            CollectionDecl {
                name: "prices".into(),
                kind: CollectionKind::SortedSet {
                    partition: KeySchema(vec![]),
                    member: KeySchema(vec![KeyKind::Str]),
                    score: ScoreKind::Decimal,
                },
            },
        ],
    );
    let (p, _) = s.register(&dec).unwrap();
    p.apply::<_, Error>(cp(1), |w| {
        let mut z = w.sorted_set::<(), String, Timestamp>("recent")?;
        z.put(&(), &"old".into(), Timestamp(-5))?;
        z.put(&(), &"new".into(), Timestamp(5))?;
        let mut pz = w.sorted_set::<(), String, Decimal>("prices")?;
        pz.put(&(), &"cheap".into(), Decimal::new(99, 2))?;
        pz.put(&(), &"dear".into(), Decimal::new(100, 0))?;
        pz.put(&(), &"mid".into(), Decimal::new(1, 0))?;
        Ok(())
    })
    .unwrap();
    let snap = p.snapshot().unwrap();
    assert_eq!(
        snap.sorted_set::<(), String, Timestamp>("recent")
            .unwrap()
            .top(&(), 1)
            .unwrap()
            .items,
        vec![("new".to_string(), Timestamp(5))]
    );
    let prices = snap.sorted_set::<(), String, Decimal>("prices").unwrap();
    assert_eq!(
        prices
            .by_score(&())
            .unwrap()
            .run()
            .unwrap()
            .items
            .iter()
            .map(|(m, _)| m.as_str())
            .collect::<Vec<_>>(),
        vec!["cheap", "mid", "dear"]
    );
}

#[test]
fn the_watch_feed_names_partitions_and_elements() {
    let d = tmp();
    let (_s, p) = open_all(d.path());
    let mut rx = p.subscribe();
    p.apply::<_, Error>(cp(1), |w| {
        w.list::<String, String>("l")?
            .push_back(&"f".into(), &"x".into())?;
        w.sorted_set::<String, Uuid, i64>("z")?
            .put(&"b".into(), &Uuid::from_u128(1), 1)?;
        w.buckets::<String, i64>("b")?.add(&"k".into(), 0, 1)?;
        Ok(())
    })
    .unwrap();
    let s = rx.try_recv().unwrap();
    let keys = |name: &str| match s.collection(name).unwrap() {
        facet::Changed::Keys(k) => k.clone(),
        facet::Changed::All => panic!("all"),
    };
    use facet::KeyType;
    assert!(keys("l").contains(&"f".to_string().to_key().into()));
    assert_eq!(keys("l").len(), 2);
    assert!(keys("z").contains(&"b".to_string().to_key().into()));
    assert_eq!(keys("z").len(), 2);
    assert!(keys("b").contains(&"k".to_string().to_key().into()));
    assert_eq!(keys("b").len(), 3, "the key and one bucket per width");
    let _ = BTreeMap::<u8, u8>::new();
}
