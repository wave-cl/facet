//! A model of the collections' semantics, checked against random
//! operation sequences with reopens, aborted batches and rebuilds:
//! every read, forward and reverse page walk, and every batch summary
//! must agree with in-memory references.

use std::collections::{BTreeMap, BTreeSet};

use facet::{
    Changed, CollectionDecl, CollectionKind, ColumnDecl, Error, IndexDecl, KeyKind, KeySchema,
    KeyType, NumKind, ScoreKind, TypeSchema, ValueSchema, Width,
};
use proptest::prelude::*;
use serde::{Deserialize, Serialize};

use crate::common::*;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, PartialOrd, Ord, Eq)]
struct Row {
    id: u64,
    group: String,
    rank: i64,
    label: Option<String>,
}

#[derive(Debug, Clone)]
enum Op {
    MapPut(u8, i64),
    MapRemove(u8),
    SetInsert(u8),
    SetRemove(u8),
    CounterAdd(u8, i64),
    ListPushBack(u8, i64),
    ListPushFront(u8, i64),
    ListRemoveNth(u8, u8),
    ListTruncateFront(u8, u8),
    ListClear(u8),
    TableUpsert(u8, u8, i64, Option<u8>),
    TableDelete(u8),
    ZPut(u8, u8, i64),
    ZRemove(u8, u8),
    BucketAdd(u8, i64, i64),
    /// Ends the batch with an error: nothing in it lands.
    Abort,
    Reopen,
    Rebuild,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        6 => (any::<u8>(), any::<i64>()).prop_map(|(k, v)| Op::MapPut(k % 16, v)),
        3 => any::<u8>().prop_map(|k| Op::MapRemove(k % 16)),
        4 => any::<u8>().prop_map(|k| Op::SetInsert(k % 16)),
        2 => any::<u8>().prop_map(|k| Op::SetRemove(k % 16)),
        4 => (any::<u8>(), -1000i64..1000).prop_map(|(k, v)| Op::CounterAdd(k % 4, v)),
        4 => (any::<u8>(), any::<i64>()).prop_map(|(p, v)| Op::ListPushBack(p % 3, v)),
        2 => (any::<u8>(), any::<i64>()).prop_map(|(p, v)| Op::ListPushFront(p % 3, v)),
        2 => (any::<u8>(), any::<u8>()).prop_map(|(p, n)| Op::ListRemoveNth(p % 3, n)),
        1 => (any::<u8>(), any::<u8>()).prop_map(|(p, n)| Op::ListTruncateFront(p % 3, n % 4)),
        1 => any::<u8>().prop_map(|p| Op::ListClear(p % 3)),
        5 => (any::<u8>(), any::<u8>(), -50i64..50, proptest::option::of(any::<u8>()))
            .prop_map(|(id, g, r, l)| Op::TableUpsert(id % 12, g % 3, r, l.map(|l| l % 3))),
        2 => any::<u8>().prop_map(|id| Op::TableDelete(id % 12)),
        4 => (any::<u8>(), any::<u8>(), -20i64..20).prop_map(|(p, m, s)| Op::ZPut(p % 2, m % 8, s)),
        2 => (any::<u8>(), any::<u8>()).prop_map(|(p, m)| Op::ZRemove(p % 2, m % 8)),
        3 => (any::<u8>(), -200_000_000_000i64..200_000_000_000, -100i64..100)
            .prop_map(|(k, at, by)| Op::BucketAdd(k % 2, at, by)),
        2 => Just(Op::Abort),
        1 => Just(Op::Reopen),
        1 => Just(Op::Rebuild),
    ]
}

/// count, sum, min, max.
type Agg = (u64, i64, i64, i64);

#[derive(Default, Clone, Debug)]
struct Model {
    map: BTreeMap<u64, i64>,
    set: BTreeSet<(u64,)>,
    counter: BTreeMap<u64, i64>,
    /// partition -> (first, next, elements by seq)
    lists: BTreeMap<String, (i64, i64, BTreeMap<i64, i64>)>,
    table: BTreeMap<u64, Row>,
    zset: BTreeMap<(String, u64), i64>,
    buckets: BTreeMap<(String, u8, i64), Agg>,
}

const WIDTHS: [Width; 2] = [
    Width::Minute,
    Width::Custom {
        nanos: 7_000_000_000,
    },
];

fn model_decl() -> facet::ProjectionDecl {
    let col = |n: &str, ty, nullable| ColumnDecl {
        name: n.into(),
        ty,
        nullable,
    };
    decl(
        "model",
        "1",
        vec![
            map_decl("m", &[KeyKind::Uint], ValueSchema::Any),
            CollectionDecl {
                name: "s".into(),
                kind: CollectionKind::Set {
                    member: KeySchema(vec![KeyKind::Uint]),
                },
            },
            CollectionDecl {
                name: "c".into(),
                kind: CollectionKind::Counter {
                    key: KeySchema(vec![KeyKind::Uint]),
                    num: NumKind::Int,
                },
            },
            CollectionDecl {
                name: "l".into(),
                kind: CollectionKind::List {
                    partition: KeySchema(vec![KeyKind::Str]),
                    value: ValueSchema::Typed(TypeSchema::Int),
                },
            },
            CollectionDecl {
                name: "t".into(),
                kind: CollectionKind::Table {
                    columns: vec![
                        col("id", TypeSchema::Uint, false),
                        col("group", TypeSchema::Str, false),
                        col("rank", TypeSchema::Int, false),
                        col("label", TypeSchema::Str, true),
                    ],
                    primary_key: vec!["id".into()],
                    indexes: vec![
                        IndexDecl {
                            name: "by_group".into(),
                            columns: vec!["group".into(), "rank".into()],
                            unique: false,
                        },
                        IndexDecl {
                            name: "by_label".into(),
                            columns: vec!["label".into()],
                            unique: false,
                        },
                    ],
                },
            },
            CollectionDecl {
                name: "z".into(),
                kind: CollectionKind::SortedSet {
                    partition: KeySchema(vec![KeyKind::Str]),
                    member: KeySchema(vec![KeyKind::Uint]),
                    score: ScoreKind::Int,
                },
            },
            CollectionDecl {
                name: "b".into(),
                kind: CollectionKind::Buckets {
                    key: KeySchema(vec![KeyKind::Str]),
                    num: NumKind::Int,
                    widths: WIDTHS.to_vec(),
                },
            },
        ],
    )
}

fn part(p: u8) -> String {
    format!("p{p}")
}

/// Applies one op to the model, returning the changed keys per
/// collection (what the summary must list).
fn apply_model(m: &mut Model, op: &Op, changed: &mut BTreeMap<&'static str, BTreeSet<Vec<u8>>>) {
    let mut touch = |c: &'static str, k: Vec<u8>| {
        changed.entry(c).or_default().insert(k);
    };
    match op {
        Op::MapPut(k, v) => {
            m.map.insert(u64::from(*k), *v);
            touch("m", u64::from(*k).to_key());
        }
        Op::MapRemove(k) => {
            if m.map.remove(&u64::from(*k)).is_some() {
                touch("m", u64::from(*k).to_key());
            }
        }
        Op::SetInsert(k) => {
            if m.set.insert((u64::from(*k),)) {
                touch("s", u64::from(*k).to_key());
            }
        }
        Op::SetRemove(k) => {
            if m.set.remove(&(u64::from(*k),)) {
                touch("s", u64::from(*k).to_key());
            }
        }
        Op::CounterAdd(k, v) => {
            *m.counter.entry(u64::from(*k)).or_default() += v;
            touch("c", u64::from(*k).to_key());
        }
        Op::ListPushBack(p, v) => {
            let e = m.lists.entry(part(*p)).or_insert((0, 0, BTreeMap::new()));
            let seq = e.1;
            e.1 += 1;
            e.2.insert(seq, *v);
            touch("l", part(*p).to_key());
            touch("l", (part(*p), seq).to_key());
        }
        Op::ListPushFront(p, v) => {
            let e = m.lists.entry(part(*p)).or_insert((0, 0, BTreeMap::new()));
            e.0 -= 1;
            let seq = e.0;
            e.2.insert(seq, *v);
            touch("l", part(*p).to_key());
            touch("l", (part(*p), seq).to_key());
        }
        Op::ListRemoveNth(p, n) => {
            if let Some(e) = m.lists.get_mut(&part(*p))
                && let Some(&seq) = e.2.keys().nth(usize::from(*n) % e.2.len().max(1))
            {
                e.2.remove(&seq);
                touch("l", part(*p).to_key());
                touch("l", (part(*p), seq).to_key());
            }
        }
        Op::ListTruncateFront(p, keep) => {
            if let Some(e) = m.lists.get_mut(&part(*p)) {
                while e.2.len() > usize::from(*keep) {
                    let (&seq, _) = e.2.iter().next().unwrap();
                    e.2.remove(&seq);
                    touch("l", part(*p).to_key());
                    touch("l", (part(*p), seq).to_key());
                }
            }
        }
        Op::ListClear(p) => {
            if let Some(e) = m.lists.remove(&part(*p))
                && (e.0 != 0 || e.1 != 0 || !e.2.is_empty())
            {
                touch("l", part(*p).to_key());
            }
        }
        Op::TableUpsert(id, g, r, l) => {
            m.table.insert(
                u64::from(*id),
                Row {
                    id: u64::from(*id),
                    group: part(*g),
                    rank: *r,
                    label: l.map(|l| format!("l{l}")),
                },
            );
            touch("t", u64::from(*id).to_key());
        }
        Op::TableDelete(id) => {
            if m.table.remove(&u64::from(*id)).is_some() {
                touch("t", u64::from(*id).to_key());
            }
        }
        Op::ZPut(p, mem, s) => {
            m.zset.insert((part(*p), u64::from(*mem)), *s);
            touch("z", part(*p).to_key());
            touch("z", (part(*p), u64::from(*mem)).to_key());
        }
        Op::ZRemove(p, mem) => {
            if m.zset.remove(&(part(*p), u64::from(*mem))).is_some() {
                touch("z", part(*p).to_key());
                touch("z", (part(*p), u64::from(*mem)).to_key());
            }
        }
        Op::BucketAdd(k, at, by) => {
            touch("b", part(*k).to_key());
            for (i, w) in WIDTHS.iter().enumerate() {
                let start = facet::window_start(*at, *w);
                let e = m
                    .buckets
                    .entry((part(*k), i as u8, start))
                    .or_insert((0, 0, *by, *by));
                e.0 += 1;
                e.1 += by;
                e.2 = e.2.min(*by);
                e.3 = e.3.max(*by);
                let mut key = part(*k).to_key();
                key.push(i as u8);
                facet::keyenc::encode_i64(start, &mut key);
                touch("b", key);
            }
        }
        Op::Abort | Op::Reopen | Op::Rebuild => {}
    }
}

fn apply_engine(w: &mut facet::Writer<'_>, op: &Op) -> Result<(), Error> {
    match op {
        Op::MapPut(k, v) => w.map::<u64, i64>("m")?.put(&u64::from(*k), v),
        Op::MapRemove(k) => w.map::<u64, i64>("m")?.remove(&u64::from(*k)).map(|_| ()),
        Op::SetInsert(k) => w.set::<(u64,)>("s")?.insert(&(u64::from(*k),)).map(|_| ()),
        Op::SetRemove(k) => w.set::<(u64,)>("s")?.remove(&(u64::from(*k),)).map(|_| ()),
        Op::CounterAdd(k, v) => w
            .counter::<u64, i64>("c")?
            .add(&u64::from(*k), *v)
            .map(|_| ()),
        Op::ListPushBack(p, v) => w
            .list::<String, i64>("l")?
            .push_back(&part(*p), v)
            .map(|_| ()),
        Op::ListPushFront(p, v) => w
            .list::<String, i64>("l")?
            .push_front(&part(*p), v)
            .map(|_| ()),
        Op::ListRemoveNth(p, n) => {
            // The nth live element: found by reading the partition.
            let mut l = w.list::<String, i64>("l")?;
            let len = l.len(&part(*p))?;
            if len == 0 {
                return Ok(());
            }
            let target = u64::from(*n) % len;
            // Walk sequence numbers from the smallest possible: the engine
            // has no "nth" read, so probe the known range.
            let mut seen = 0;
            for seq in -300i64..300 {
                if l.get(&part(*p), seq)?.is_some() {
                    if seen == target {
                        l.remove(&part(*p), seq)?;
                        return Ok(());
                    }
                    seen += 1;
                }
            }
            Ok(())
        }
        Op::ListTruncateFront(p, keep) => w
            .list::<String, i64>("l")?
            .truncate_front(&part(*p), u64::from(*keep))
            .map(|_| ()),
        Op::ListClear(p) => w.list::<String, i64>("l")?.clear(&part(*p)).map(|_| ()),
        Op::TableUpsert(id, g, r, l) => w.table::<u64, Row>("t")?.upsert(&Row {
            id: u64::from(*id),
            group: part(*g),
            rank: *r,
            label: l.map(|l| format!("l{l}")),
        }),
        Op::TableDelete(id) => w
            .table::<u64, Row>("t")?
            .delete(&u64::from(*id))
            .map(|_| ()),
        Op::ZPut(p, mem, s) => w
            .sorted_set::<String, u64, i64>("z")?
            .put(&part(*p), &u64::from(*mem), *s)
            .map(|_| ()),
        Op::ZRemove(p, mem) => w
            .sorted_set::<String, u64, i64>("z")?
            .remove(&part(*p), &u64::from(*mem))
            .map(|_| ()),
        Op::BucketAdd(k, at, by) => w.buckets::<String, i64>("b")?.add(&part(*k), *at, *by),
        Op::Abort | Op::Reopen | Op::Rebuild => Ok(()),
    }
}

/// Walks a scan in pages of `page` and returns every item.
fn walk<'s, K: KeyType, T>(
    mut make: impl FnMut() -> facet::Scan<'s, K, T>,
    reverse: bool,
    page: usize,
) -> Vec<T> {
    let mut out = Vec::new();
    let mut cursor = None;
    loop {
        let mut sc = make().limit(page);
        if reverse {
            sc = sc.reverse();
        }
        if let Some(c) = &cursor {
            sc = sc.after(c);
        }
        let p = sc.run().unwrap();
        out.extend(p.items);
        match p.next {
            Some(c) => cursor = Some(c),
            None => return out,
        }
    }
}

fn check(p: &facet::Projection, m: &Model, page: usize) {
    let snap = p.snapshot().unwrap();
    // map
    let map = snap.map::<u64, i64>("m").unwrap();
    assert_eq!(map.len().unwrap(), m.map.len() as u64);
    let want: Vec<(u64, i64)> = m.map.iter().map(|(k, v)| (*k, *v)).collect();
    assert_eq!(walk(|| map.scan(), false, page), want);
    let mut rev = want.clone();
    rev.reverse();
    assert_eq!(walk(|| map.scan(), true, page), rev);
    for k in 0..16u64 {
        assert_eq!(map.get(&k).unwrap(), m.map.get(&k).copied());
    }
    // set
    let set = snap.set::<(u64,)>("s").unwrap();
    assert_eq!(
        walk(|| set.scan(), false, page),
        m.set.iter().copied().collect::<Vec<_>>()
    );
    // counter
    let c = snap.counter::<u64, i64>("c").unwrap();
    assert_eq!(
        walk(|| c.scan(), false, page),
        m.counter.iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>()
    );
    // lists
    let l = snap.list::<String, i64>("l").unwrap();
    for pn in 0..3u8 {
        let name = part(pn);
        let want: Vec<(i64, i64)> = m
            .lists
            .get(&name)
            .map(|e| e.2.iter().map(|(s, v)| (*s, *v)).collect())
            .unwrap_or_default();
        assert_eq!(l.len(&name).unwrap(), want.len() as u64, "list {name} len");
        assert_eq!(
            walk(|| l.range(&name).unwrap(), false, page),
            want,
            "list {name}"
        );
        let mut rev = want.clone();
        rev.reverse();
        assert_eq!(walk(|| l.range(&name).unwrap(), true, page), rev);
    }
    // table, by pk and by both indexes
    let t = snap.table::<u64, Row>("t").unwrap();
    assert_eq!(
        walk(|| t.scan(), false, page),
        m.table.values().cloned().collect::<Vec<_>>()
    );
    let mut by_group: Vec<&Row> = m.table.values().collect();
    by_group.sort_by(|a, b| (&a.group, a.rank, a.id).cmp(&(&b.group, b.rank, b.id)));
    assert_eq!(
        walk(
            || t.by_index::<(String, i64)>("by_group").unwrap(),
            false,
            page
        ),
        by_group.iter().map(|r| (*r).clone()).collect::<Vec<_>>()
    );
    let mut by_label: Vec<&Row> = m.table.values().collect();
    by_label.sort_by(|a, b| (&a.label, a.id).cmp(&(&b.label, b.id)));
    assert_eq!(
        walk(
            || t.by_index::<(Option<String>,)>("by_label").unwrap(),
            true,
            page
        ),
        by_label
            .iter()
            .rev()
            .map(|r| (*r).clone())
            .collect::<Vec<_>>()
    );
    for g in 0..3u8 {
        let want: Vec<Row> = by_group
            .iter()
            .filter(|r| r.group == part(g))
            .map(|r| (*r).clone())
            .collect();
        assert_eq!(
            walk(
                || t.by_index::<(String, i64)>("by_group")
                    .unwrap()
                    .prefix(&(part(g),)),
                false,
                page
            ),
            want
        );
    }
    // sorted sets
    let z = snap.sorted_set::<String, u64, i64>("z").unwrap();
    for pn in 0..2u8 {
        let name = part(pn);
        let mut want: Vec<(u64, i64)> = m
            .zset
            .iter()
            .filter(|((p, _), _)| *p == name)
            .map(|((_, mem), s)| (*mem, *s))
            .collect();
        want.sort_by_key(|(mem, s)| (*s, *mem));
        assert_eq!(
            walk(|| z.by_score(&name).unwrap(), false, page),
            want,
            "zset {name}"
        );
        let mut rev = want.clone();
        rev.reverse();
        assert_eq!(walk(|| z.by_score(&name).unwrap(), true, page), rev);
        let mut by_member = want.clone();
        by_member.sort();
        assert_eq!(walk(|| z.members(&name).unwrap(), false, page), by_member);
        for (i, (mem, _)) in want.iter().enumerate() {
            assert_eq!(z.rank(&name, mem, 1000).unwrap(), Some(i as u64));
        }
    }
    // buckets
    let b = snap.buckets::<String, i64>("b").unwrap();
    for kn in 0..2u8 {
        for (i, w) in WIDTHS.iter().enumerate() {
            let want: Vec<facet::Bucket<i64>> = m
                .buckets
                .iter()
                .filter(|((k, wi, _), _)| *k == part(kn) && *wi == i as u8)
                .map(|((_, _, start), (count, sum, min, max))| facet::Bucket {
                    start: *start,
                    count: *count,
                    sum: *sum,
                    min: *min,
                    max: *max,
                })
                .collect();
            assert_eq!(walk(|| b.range(&part(kn), *w).unwrap(), false, page), want);
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 24, max_shrink_iters: 200, .. ProptestConfig::default() })]

    #[test]
    fn the_engine_matches_the_model(
        batches in proptest::collection::vec(proptest::collection::vec(op(), 1..6), 1..24),
        page in 1usize..5,
    ) {
        let d = tmp();
        let s = store(d.path());
        let (mut p, _) = s.register(&model_decl()).unwrap();
        let mut rx = p.subscribe();
        let mut model = Model::default();
        let mut next = 1;
        for batch in &batches {
            // Control ops happen between batches.
            if let Some(Op::Reopen) = batch.first() {
                drop(p);
                let (q, reg) = s.register(&model_decl()).unwrap();
                prop_assert_eq!(reg.rebuilt, facet::Rebuilt::No);
                prop_assert_eq!(reg.checkpoint.next, next - 1);
                p = q;
                rx = p.subscribe();
            }
            if let Some(Op::Rebuild) = batch.first() {
                p.rebuild().unwrap();
                model = Model::default();
                let s = rx.try_recv().unwrap();
                prop_assert!(s.collections.iter().all(|c| c.changed == Changed::All));
            }
            let aborts = batch.iter().any(|o| matches!(o, Op::Abort));
            let before = model.clone();
            let mut changed = BTreeMap::new();
            let ops: Vec<&Op> = batch.iter().filter(|o| !matches!(o, Op::Reopen | Op::Rebuild)).collect();
            let r = p.apply::<_, Error>(cp(next), |w| {
                for op in &ops {
                    if matches!(op, Op::Abort) {
                        return Err(Error::Validation("abort".into()));
                    }
                    apply_engine(w, op)?;
                    apply_model(&mut model, op, &mut changed);
                }
                Ok(())
            });
            if aborts {
                prop_assert!(r.is_err());
                model = before;
                prop_assert!(rx.try_recv().is_err(), "an aborted batch publishes nothing");
            } else {
                r.unwrap();
                next += 1;
                let summary = rx.try_recv().unwrap();
                let got: BTreeMap<&str, BTreeSet<Vec<u8>>> = summary
                    .collections
                    .iter()
                    .map(|c| {
                        let keys = match &c.changed {
                            Changed::Keys(k) => k.iter().map(|b| b.to_vec()).collect(),
                            Changed::All => panic!("all"),
                        };
                        (c.name.as_str(), keys)
                    })
                    .collect();
                let want: BTreeMap<&str, BTreeSet<Vec<u8>>> = changed.into_iter().filter(|(_, v)| !v.is_empty()).collect();
                prop_assert_eq!(got, want);
            }
            check(&p, &model, page);
        }
    }
}
