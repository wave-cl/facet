//! Reads by collection name with dynamic keys and values: what a query
//! service uses when it does not know the application's types.

use std::collections::BTreeMap;

use redb::ReadableTableMetadata;

use crate::error::{Error, Result};
use crate::keyenc::{
    KeyKind, KeyPart, KeyTuple, decode_part, decode_parts, decode_parts_nullable, encode_i64,
    prefix_end,
};
use crate::prim::buckets::Bucket;
use crate::prim::counter::Num;
use crate::prim::table;
use crate::prim::{find, table_name};
use crate::reader::{Cursor, Page, RawScan, Snapshot};
use crate::schema::{CollectionDecl, CollectionKind, NumKind, ScoreKind, Width};
use crate::value::Value;
use crate::vbin;

/// How a collection's keys look, part by part, so a key given as text
/// can be parsed.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct KeyShape {
    /// The parts' kinds.
    pub kinds: Vec<KeyKind>,
    /// Which parts may be null.
    pub nullable: Vec<bool>,
    /// The parts' names where there are any (table columns), else
    /// `"0"`, `"1"`, …
    pub names: Vec<String>,
}

impl KeyShape {
    fn of(kinds: &[KeyKind]) -> KeyShape {
        KeyShape {
            kinds: kinds.to_vec(),
            nullable: vec![false; kinds.len()],
            names: (0..kinds.len()).map(|i| i.to_string()).collect(),
        }
    }

    fn with_names(kinds: &[KeyKind], names: &[String]) -> KeyShape {
        KeyShape {
            kinds: kinds.to_vec(),
            nullable: vec![false; kinds.len()],
            names: names.to_vec(),
        }
    }

    /// Checks a tuple against the leading parts of this shape and
    /// encodes it.
    pub fn encode(&self, t: &KeyTuple) -> Result<Vec<u8>> {
        if t.0.len() > self.kinds.len() {
            return Err(Error::Key(format!(
                "{} key parts given, the key has {}",
                t.0.len(),
                self.kinds.len()
            )));
        }
        let mut out = Vec::new();
        for (i, p) in t.0.iter().enumerate() {
            if p.kind() != self.kinds[i] {
                return Err(Error::Key(format!(
                    "part {} ({}) is a {}, not a {}",
                    i,
                    self.names[i],
                    p.kind(),
                    self.kinds[i]
                )));
            }
            if self.nullable[i] {
                p.encode_nullable(&mut out);
            } else {
                if matches!(p, KeyPart::Null(_)) {
                    return Err(Error::Key(format!(
                        "part {} ({}) cannot be null",
                        i, self.names[i]
                    )));
                }
                p.encode(&mut out);
            }
        }
        Ok(out)
    }

    /// Decodes a whole key.
    pub fn decode(&self, bytes: &[u8]) -> Result<KeyTuple> {
        let (t, used) = decode_parts_nullable(&self.kinds, &self.nullable, bytes)?;
        if used != bytes.len() {
            return Err(Error::Key(format!(
                "{} trailing bytes after the key",
                bytes.len() - used
            )));
        }
        Ok(t)
    }
}

/// A scan given dynamically.
#[derive(Debug, Clone, Default)]
pub struct DynScan {
    /// Leading parts every key must share.
    pub prefix: Option<KeyTuple>,
    /// Keys at or above these leading parts.
    pub from: Option<KeyTuple>,
    /// Keys at or below these leading parts (every key starting with
    /// them included).
    pub to: Option<KeyTuple>,
    /// Continue after this cursor.
    pub after: Option<Cursor>,
    /// Descending.
    pub reverse: bool,
    /// At most this many (0 = all).
    pub limit: usize,
}

/// One collection, read dynamically.
pub struct DynR<'s> {
    s: &'s Snapshot,
    decl: CollectionDecl,
    table: String,
}

impl Snapshot {
    /// The collection `name`, read dynamically.
    pub fn collection(&self, name: &str) -> Result<DynR<'_>> {
        let decl = find(&self.decl, name)?.clone();
        Ok(DynR {
            s: self,
            table: table_name(name),
            decl,
        })
    }
}

/// A cursor must start with the scan's base and decode as one of its
/// keys; anything else is `InvalidCursor`.
fn check_cursor(
    after: Option<&Cursor>,
    base: &[u8],
    decode: impl FnOnce(&[u8]) -> Result<()>,
) -> Result<Option<Vec<u8>>> {
    let Some(c) = after else {
        return Ok(None);
    };
    if !c.0.starts_with(base) {
        return Err(Error::InvalidCursor(
            "the cursor is not from this scan".into(),
        ));
    }
    decode(&c.0[base.len()..])
        .map_err(|e| Error::InvalidCursor(format!("the cursor does not decode: {e}")))?;
    Ok(Some(c.0.clone()))
}

fn num_value(kind: NumKind, bytes: &[u8]) -> Result<Value> {
    Ok(match kind {
        NumKind::Int => Value::Int(i64::decode(bytes)?),
        NumKind::Decimal => Value::Decimal(rust_decimal::Decimal::decode(bytes)?),
    })
}

fn bucket_value(kind: NumKind, start: i64, bytes: &[u8]) -> Result<Value> {
    let mut m = BTreeMap::new();
    m.insert(
        "start".to_string(),
        Value::Timestamp(crate::value::Timestamp(start)),
    );
    match kind {
        NumKind::Int => {
            let b: Bucket<i64> = Bucket::decode_bytes(start, bytes)?;
            m.insert("count".into(), Value::Uint(b.count));
            m.insert("sum".into(), Value::Int(b.sum));
            m.insert("min".into(), Value::Int(b.min));
            m.insert("max".into(), Value::Int(b.max));
        }
        NumKind::Decimal => {
            let b: Bucket<rust_decimal::Decimal> = Bucket::decode_bytes(start, bytes)?;
            m.insert("count".into(), Value::Uint(b.count));
            m.insert("sum".into(), Value::Decimal(b.sum));
            m.insert("min".into(), Value::Decimal(b.min));
            m.insert("max".into(), Value::Decimal(b.max));
        }
    }
    Ok(Value::Map(m))
}

fn score_value(kind: ScoreKind, bytes: &[u8]) -> Result<Value> {
    let (p, used) = decode_part(kind.key_kind(), bytes)?;
    if used != bytes.len() {
        return Err(Error::Key("trailing bytes after a score".into()));
    }
    Ok(p.to_value())
}

impl DynR<'_> {
    /// The declaration.
    pub fn decl(&self) -> &CollectionDecl {
        &self.decl
    }

    /// The kind's name.
    pub fn kind(&self) -> &'static str {
        self.decl.kind.name()
    }

    /// The shape of the keys `get` and `scan` take: the register has
    /// none; a list's is its partition followed by `seq` (int); a
    /// sorted set's is its partition followed by its member; a table's
    /// is its primary key; buckets' is their key.
    pub fn key_shape(&self) -> KeyShape {
        KeyShape::for_decl(&self.decl)
    }
}

impl KeyShape {
    /// The shape of a collection's keys (see [`DynR::key_shape`]).
    pub fn for_decl(decl: &CollectionDecl) -> KeyShape {
        match &decl.kind {
            CollectionKind::Register { .. } => KeyShape::of(&[]),
            CollectionKind::Counter { key, .. } => KeyShape::of(&key.0),
            CollectionKind::Set { member } => KeyShape::of(&member.0),
            CollectionKind::Map { key, .. } => KeyShape::of(&key.0),
            CollectionKind::List { partition, .. } => {
                let mut kinds = partition.0.clone();
                kinds.push(KeyKind::Int);
                let mut names: Vec<String> =
                    (0..partition.0.len()).map(|i| i.to_string()).collect();
                names.push("seq".into());
                KeyShape::with_names(&kinds, &names)
            }
            CollectionKind::Table {
                columns,
                primary_key,
                ..
            } => {
                let kinds: Vec<KeyKind> = primary_key
                    .iter()
                    .map(|n| {
                        columns
                            .iter()
                            .find(|c| &c.name == n)
                            .and_then(|c| c.ty.key_kind())
                            .expect("validated")
                    })
                    .collect();
                KeyShape::with_names(&kinds, primary_key)
            }
            CollectionKind::SortedSet {
                partition, member, ..
            } => {
                let mut kinds = partition.0.clone();
                kinds.extend(member.0.iter().copied());
                KeyShape::of(&kinds)
            }
            CollectionKind::Buckets { key, .. } => KeyShape::of(&key.0),
        }
    }

    /// The shape of a secondary index's key of a table declaration.
    pub fn for_index(decl: &CollectionDecl, index: &str) -> Result<KeyShape> {
        let CollectionKind::Table {
            columns, indexes, ..
        } = &decl.kind
        else {
            return Err(Error::KindMismatch {
                name: decl.name.clone(),
                declared: decl.kind.name().into(),
                asked: "table".into(),
            });
        };
        let ix = indexes
            .iter()
            .find(|i| i.name == index)
            .ok_or_else(|| Error::NotDeclared(format!("{}.{index}", decl.name)))?;
        let cols: Vec<&crate::schema::ColumnDecl> = ix
            .columns
            .iter()
            .map(|n| columns.iter().find(|c| &c.name == n).expect("validated"))
            .collect();
        Ok(KeyShape {
            kinds: cols
                .iter()
                .map(|c| c.ty.key_kind().expect("validated"))
                .collect(),
            nullable: cols.iter().map(|c| c.nullable).collect(),
            names: ix.columns.clone(),
        })
    }
}

impl DynR<'_> {
    /// The shape of a secondary index's key.
    pub fn index_shape(&self, index: &str) -> Result<KeyShape> {
        KeyShape::for_index(&self.decl, index)
    }

    /// The secondary indexes' names (tables only).
    pub fn indexes(&self) -> Vec<String> {
        match &self.decl.kind {
            CollectionKind::Table { indexes, .. } => {
                indexes.iter().map(|i| i.name.clone()).collect()
            }
            _ => Vec::new(),
        }
    }

    /// The number of entries (rows, members, counters, elements over
    /// every partition, buckets over every width).
    pub fn len(&self) -> Result<u64> {
        Ok(match self.s.raw_table(&self.table)? {
            Some(t) => t.len()?,
            None => 0,
        })
    }

    /// Whether there are no entries.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// The value stored under a raw encoded key, decoded for this kind.
    fn value_of(&self, key: &[u8], bytes: &[u8]) -> Result<Value> {
        Ok(match &self.decl.kind {
            CollectionKind::Register { .. }
            | CollectionKind::Map { .. }
            | CollectionKind::List { .. } => vbin::decode(bytes)?,
            CollectionKind::Counter { num, .. } => num_value(*num, bytes)?,
            CollectionKind::Set { .. } => Value::Bool(true),
            CollectionKind::Table { .. } => {
                let meta = table::open_meta(&self.decl)?;
                let values = meta.values_from(key, bytes)?;
                Value::Map(
                    meta.columns
                        .iter()
                        .zip(values)
                        .map(|(c, v)| (c.name.clone(), v))
                        .collect(),
                )
            }
            CollectionKind::SortedSet { score, .. } => score_value(*score, bytes)?,
            CollectionKind::Buckets { num, .. } => {
                // The start is the last 8 bytes of the key.
                let n = key.len();
                let raw: [u8; 8] = key[n - 8..].try_into().unwrap();
                let start = (u64::from_be_bytes(raw) ^ (1u64 << 63)) as i64;
                bucket_value(*num, start, bytes)?
            }
        })
    }

    /// The value at a whole key (every part), if any. Buckets take
    /// [`DynR::bucket`] instead.
    pub fn get(&self, key: &KeyTuple) -> Result<Option<Value>> {
        if matches!(self.decl.kind, CollectionKind::Buckets { .. }) {
            return Err(Error::Key(
                "buckets are read by key, width and window".into(),
            ));
        }
        let shape = self.key_shape();
        if key.0.len() != shape.kinds.len() {
            return Err(Error::Key(format!(
                "a whole key has {} parts, {} given",
                shape.kinds.len(),
                key.0.len()
            )));
        }
        let k = shape.encode(key)?;
        let Some(t) = self.s.raw_table(&self.table)? else {
            return Ok(None);
        };
        match t.get(k.as_slice())? {
            Some(g) => Ok(Some(self.value_of(&k, g.value())?)),
            None => Ok(None),
        }
    }

    fn run_scan(
        &self,
        table: &str,
        shape: &KeyShape,
        base: &[u8],
        q: &DynScan,
    ) -> Result<Page<(KeyTuple, Value)>> {
        let mut raw = RawScan {
            prefix: base.to_vec(),
            ..Default::default()
        };
        let join = |t: &KeyTuple| -> Result<Vec<u8>> {
            let mut v = base.to_vec();
            v.extend(shape.encode(t)?);
            Ok(v)
        };
        if let Some(p) = &q.prefix {
            raw.prefix = join(p)?;
        }
        if let Some(f) = &q.from {
            raw.from = Some(join(f)?);
        }
        if let Some(t) = &q.to {
            raw.to_exclusive = prefix_end(&join(t)?);
        }
        raw.after = check_cursor(q.after.as_ref(), base, |rest| {
            shape.decode(rest).map(|_| ())
        })?;
        raw.reverse = q.reverse;
        raw.limit = q.limit;
        let (items, next) = raw.run(self.s.raw_table(table)?.as_deref())?;
        let mut out = Vec::with_capacity(items.len());
        for (k, v) in items {
            let key = shape.decode(&k[base.len()..])?;
            out.push((key, self.value_of(&k, &v)?));
        }
        Ok(Page {
            items: out,
            next,
            checkpoint: self.s.checkpoint(),
        })
    }

    /// Entries in key order (the register has one, under the empty
    /// key). Buckets take [`DynR::buckets`] instead.
    pub fn scan(&self, q: &DynScan) -> Result<Page<(KeyTuple, Value)>> {
        if matches!(self.decl.kind, CollectionKind::Buckets { .. }) {
            return Err(Error::Key(
                "buckets are read by key, width and window".into(),
            ));
        }
        let shape = self.key_shape();
        self.run_scan(&self.table.clone(), &shape, &[], q)
    }

    /// Rows in the order of a secondary index; keys are the index's.
    pub fn lookup(&self, index: &str, q: &DynScan) -> Result<Page<(KeyTuple, Value)>> {
        let shape = self.index_shape(index)?;
        let CollectionKind::Table { indexes, .. } = &self.decl.kind else {
            unreachable!()
        };
        let unique = indexes
            .iter()
            .find(|i| i.name == index)
            .expect("checked")
            .unique;
        let table = format!("{}:i:{index}", self.table);
        let meta = table::open_meta(&self.decl)?;
        let mut raw = RawScan::default();
        if let Some(p) = &q.prefix {
            raw.prefix = shape.encode(p)?;
        }
        if let Some(f) = &q.from {
            raw.from = Some(shape.encode(f)?);
        }
        if let Some(t) = &q.to {
            raw.to_exclusive = prefix_end(&shape.encode(t)?);
        }
        raw.after = check_cursor(q.after.as_ref(), &[], |rest| {
            decode_parts_nullable(&shape.kinds, &shape.nullable, rest).map(|_| ())
        })?;
        raw.reverse = q.reverse;
        raw.limit = q.limit;
        let (items, next) = raw.run(self.s.raw_table(&table)?.as_deref())?;
        let rows = self.s.raw_table(&self.table)?;
        let mut out = Vec::with_capacity(items.len());
        for (k, v) in items {
            let (ikey, used) = decode_parts_nullable(&shape.kinds, &shape.nullable, &k)?;
            let pk: &[u8] = if unique { &v } else { &k[used..] };
            let rows = rows.as_ref().ok_or_else(|| Error::Corrupt {
                file: self.decl.name.clone(),
                reason: "an index entry without a table".into(),
            })?;
            let g = rows.get(pk)?.ok_or_else(|| Error::Corrupt {
                file: self.decl.name.clone(),
                reason: "an index entry points at a missing row".into(),
            })?;
            let values = meta.values_from(pk, g.value())?;
            let row = Value::Map(
                meta.columns
                    .iter()
                    .zip(values)
                    .map(|(c, v)| (c.name.clone(), v))
                    .collect(),
            );
            out.push((ikey, row));
        }
        Ok(Page {
            items: out,
            next,
            checkpoint: self.s.checkpoint(),
        })
    }

    /// A sorted set's members of one partition in score order (ties by
    /// member), as `(member, score)`, between `min` and `max` inclusive.
    pub fn by_score(
        &self,
        partition: &KeyTuple,
        min: Option<&KeyPart>,
        max: Option<&KeyPart>,
        after: Option<&Cursor>,
        reverse: bool,
        limit: usize,
    ) -> Result<Page<(KeyTuple, Value)>> {
        let CollectionKind::SortedSet {
            partition: pk,
            member,
            score,
        } = &self.decl.kind
        else {
            return Err(Error::KindMismatch {
                name: self.decl.name.clone(),
                declared: self.kind().into(),
                asked: "sorted set".into(),
            });
        };
        let pshape = KeyShape::of(&pk.0);
        if partition.0.len() != pk.0.len() {
            return Err(Error::Key(format!(
                "the partition has {} parts",
                pk.0.len()
            )));
        }
        let base = pshape.encode(partition)?;
        let skind = score.key_kind();
        let check = |p: &KeyPart| -> Result<Vec<u8>> {
            if p.kind() != skind {
                return Err(Error::Key(format!(
                    "a score is a {skind}, not a {}",
                    p.kind()
                )));
            }
            let mut v = base.clone();
            p.encode(&mut v);
            Ok(v)
        };
        let mut raw = RawScan {
            prefix: base.clone(),
            ..Default::default()
        };
        if let Some(m) = min {
            raw.from = Some(check(m)?);
        }
        if let Some(m) = max {
            raw.to_exclusive = prefix_end(&check(m)?);
        }
        raw.after = check_cursor(after, &base, |rest| {
            let (_, used) = decode_part(skind, rest)?;
            decode_parts(&member.0, &rest[used..]).map(|_| ())
        })?;
        raw.reverse = reverse;
        raw.limit = limit;
        let table = format!("{}:s", self.table);
        let (items, next) = raw.run(self.s.raw_table(&table)?.as_deref())?;
        let mut out = Vec::with_capacity(items.len());
        for (k, _) in items {
            let rest = &k[base.len()..];
            let (s, used) = decode_part(skind, rest)?;
            let (m, used2) = decode_parts(&member.0, &rest[used..])?;
            if used + used2 != rest.len() {
                return Err(Error::Key("trailing bytes after a score index key".into()));
            }
            out.push((m, s.to_value()));
        }
        Ok(Page {
            items: out,
            next,
            checkpoint: self.s.checkpoint(),
        })
    }

    /// One bucket.
    pub fn bucket(&self, key: &KeyTuple, width: Width, start: i64) -> Result<Option<Value>> {
        let page = self.buckets(key, width, Some(start), Some(start), None, false, 1)?;
        Ok(page.items.into_iter().next())
    }

    /// The buckets of one key and width in time order, windows starting
    /// between `from` and `to` inclusive.
    #[allow(clippy::too_many_arguments)]
    pub fn buckets(
        &self,
        key: &KeyTuple,
        width: Width,
        from: Option<i64>,
        to: Option<i64>,
        after: Option<&Cursor>,
        reverse: bool,
        limit: usize,
    ) -> Result<Page<Value>> {
        let CollectionKind::Buckets {
            key: kshape,
            widths,
            ..
        } = &self.decl.kind
        else {
            return Err(Error::KindMismatch {
                name: self.decl.name.clone(),
                declared: self.kind().into(),
                asked: "buckets".into(),
            });
        };
        let shape = KeyShape::of(&kshape.0);
        if key.0.len() != kshape.0.len() {
            return Err(Error::Key(format!("the key has {} parts", kshape.0.len())));
        }
        let wi = widths
            .iter()
            .position(|w| w.nanos() == width.nanos())
            .ok_or_else(|| Error::Validation(format!("width {} is not declared", width.label())))?;
        let mut base = shape.encode(key)?;
        base.push(wi as u8);
        let bound = |start: i64| {
            let mut v = base.clone();
            encode_i64(start, &mut v);
            v
        };
        let mut raw = RawScan {
            prefix: base.clone(),
            ..Default::default()
        };
        if let Some(f) = from {
            raw.from = Some(bound(f));
        }
        if let Some(t) = to {
            raw.to = Some(bound(t));
        }
        raw.after = check_cursor(after, &base, |rest| {
            if rest.len() != 8 {
                return Err(Error::InvalidCursor("not a bucket cursor".into()));
            }
            Ok(())
        })?;
        raw.reverse = reverse;
        raw.limit = limit;
        let (items, next) = raw.run(self.s.raw_table(&self.table)?.as_deref())?;
        let mut out = Vec::with_capacity(items.len());
        for (k, v) in items {
            out.push(self.value_of(&k, &v)?);
        }
        Ok(Page {
            items: out,
            next,
            checkpoint: self.s.checkpoint(),
        })
    }
}
