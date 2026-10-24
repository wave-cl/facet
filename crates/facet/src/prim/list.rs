//! Lists: ordered values per partition; the empty partition tuple makes
//! one list. Elements are addressed by a sequence number that never
//! changes: pushes at the back count up, pushes at the front count down,
//! so a removed element leaves a hole and the order of the rest stands.
//!
//! Table `c:<name>`: `enc(partition) ‖ seq (i64, sign-flipped) ->
//! vbin(value)`; table `c:<name>:m`: `enc(partition) -> first_seq i64 ‖
//! next_seq i64 ‖ len u64`.

use std::marker::PhantomData;

use redb::ReadableTable;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::{Error, Result};
use crate::keyenc::{KeyType, encode_i64, prefix_end};
use crate::limits::check_elements;
use crate::prim::{
    Scan, check_kinds, decode_value, encode_key, encode_value, find, mismatch, table_name,
};
use crate::reader::Snapshot;
use crate::schema::{CollectionKind, ValueSchema};
use crate::writer::Writer;

fn open<P: KeyType>(decl: &crate::schema::ProjectionDecl, name: &str) -> Result<ValueSchema> {
    let c = find(decl, name)?;
    match &c.kind {
        CollectionKind::List { partition, value } => {
            check_kinds(name, "list", "partition", &partition.0, &P::kinds())?;
            Ok(value.clone())
        }
        other => Err(mismatch(name, other.name(), "list")),
    }
}

fn meta_table(collection: &str) -> String {
    format!("c:{collection}:m")
}

#[derive(Debug, Clone, Copy, Default)]
struct Meta {
    first: i64,
    next: i64,
    len: u64,
}

impl Meta {
    fn encode(&self) -> [u8; 24] {
        let mut out = [0u8; 24];
        out[..8].copy_from_slice(&self.first.to_be_bytes());
        out[8..16].copy_from_slice(&self.next.to_be_bytes());
        out[16..].copy_from_slice(&self.len.to_be_bytes());
        out
    }
    fn decode(b: &[u8]) -> Result<Meta> {
        if b.len() != 24 {
            return Err(Error::Value("list meta bytes".into()));
        }
        Ok(Meta {
            first: i64::from_be_bytes(b[..8].try_into().unwrap()),
            next: i64::from_be_bytes(b[8..16].try_into().unwrap()),
            len: u64::from_be_bytes(b[16..].try_into().unwrap()),
        })
    }
}

fn element_key(partition: &[u8], seq: i64) -> Vec<u8> {
    let mut k = partition.to_vec();
    encode_i64(seq, &mut k);
    k
}

/// Lists inside a batch.
pub struct ListW<'w, 't, P, V> {
    w: &'w mut Writer<'t>,
    name: String,
    table: String,
    meta: String,
    schema: ValueSchema,
    _p: PhantomData<(P, V)>,
}

impl<'w, 't, P: KeyType, V> ListW<'w, 't, P, V> {
    pub(crate) fn open(w: &'w mut Writer<'t>, name: &str) -> Result<Self> {
        let schema = open::<P>(w.decl, name)?;
        Ok(ListW {
            w,
            name: name.to_owned(),
            table: table_name(name),
            meta: meta_table(name),
            schema,
            _p: PhantomData,
        })
    }

    fn read_meta(&mut self, partition: &[u8]) -> Result<Meta> {
        let t = self.w.raw_table(&self.meta)?;
        match t.get(partition)? {
            Some(g) => Meta::decode(g.value()),
            None => Ok(Meta::default()),
        }
    }

    fn write_meta(&mut self, partition: &[u8], m: Meta) -> Result<()> {
        let t = self.w.raw_table(&self.meta)?;
        if m.len == 0 && m.first == 0 && m.next == 0 {
            t.remove(partition)?;
        } else {
            t.insert(partition, m.encode().as_slice())?;
        }
        Ok(())
    }

    fn push(&mut self, partition: &[u8], v: &V, front: bool) -> Result<i64>
    where
        V: Serialize,
    {
        let val = encode_value(&self.schema, v)?;
        let mut m = self.read_meta(partition)?;
        let seq = if front {
            m.first = m
                .first
                .checked_sub(1)
                .ok_or_else(|| Error::Overflow(format!("list {}: front sequence", self.name)))?;
            m.first
        } else {
            let s = m.next;
            m.next = m
                .next
                .checked_add(1)
                .ok_or_else(|| Error::Overflow(format!("list {}: back sequence", self.name)))?;
            s
        };
        m.len += 1;
        let key = element_key(partition, seq);
        self.w
            .raw_table(&self.table)?
            .insert(key.as_slice(), val.as_slice())?;
        self.write_meta(partition, m)?;
        self.w.changes.touch(&self.name, partition);
        self.w.changes.touch(&self.name, &key);
        Ok(seq)
    }

    /// Appends `v`; returns its sequence number.
    pub fn push_back(&mut self, p: &P, v: &V) -> Result<i64>
    where
        V: Serialize,
    {
        let partition = encode_key(p)?;
        self.push(&partition, v, false)
    }

    /// Prepends `v`; returns its sequence number.
    pub fn push_front(&mut self, p: &P, v: &V) -> Result<i64>
    where
        V: Serialize,
    {
        let partition = encode_key(p)?;
        self.push(&partition, v, true)
    }

    /// Appends every value, in order.
    pub fn push_many<'v>(&mut self, p: &P, values: impl IntoIterator<Item = &'v V>) -> Result<u64>
    where
        V: Serialize + 'v,
    {
        let partition = encode_key(p)?;
        let values: Vec<&V> = values.into_iter().collect();
        check_elements(values.len())?;
        for v in &values {
            self.push(&partition, v, false)?;
        }
        Ok(values.len() as u64)
    }

    /// The element at `seq`, as the batch sees it.
    pub fn get(&mut self, p: &P, seq: i64) -> Result<Option<V>>
    where
        V: DeserializeOwned,
    {
        let key = element_key(&encode_key(p)?, seq);
        let t = self.w.raw_table(&self.table)?;
        match t.get(key.as_slice())? {
            Some(g) => Ok(Some(decode_value(g.value())?)),
            None => Ok(None),
        }
    }

    /// Replaces the element at `seq`; `false` when there is none.
    pub fn set(&mut self, p: &P, seq: i64, v: &V) -> Result<bool>
    where
        V: Serialize,
    {
        let key = element_key(&encode_key(p)?, seq);
        let val = encode_value(&self.schema, v)?;
        let t = self.w.raw_table(&self.table)?;
        if t.get(key.as_slice())?.is_none() {
            return Ok(false);
        }
        t.insert(key.as_slice(), val.as_slice())?;
        self.w.changes.touch(&self.name, &key);
        Ok(true)
    }

    /// Removes the element at `seq`; `true` when there was one.
    pub fn remove(&mut self, p: &P, seq: i64) -> Result<bool> {
        let partition = encode_key(p)?;
        let key = element_key(&partition, seq);
        let had = self
            .w
            .raw_table(&self.table)?
            .remove(key.as_slice())?
            .is_some();
        if had {
            let mut m = self.read_meta(&partition)?;
            m.len -= 1;
            self.write_meta(&partition, m)?;
            self.w.changes.touch(&self.name, &partition);
            self.w.changes.touch(&self.name, &key);
        }
        Ok(had)
    }

    /// The number of elements in the partition.
    pub fn len(&mut self, p: &P) -> Result<u64> {
        let partition = encode_key(p)?;
        Ok(self.read_meta(&partition)?.len)
    }

    /// Drops the oldest elements until `keep` remain; returns how many
    /// went.
    pub fn truncate_front(&mut self, p: &P, keep: u64) -> Result<u64> {
        self.truncate(p, keep, true)
    }

    /// Drops the newest elements until `keep` remain; returns how many
    /// went.
    pub fn truncate_back(&mut self, p: &P, keep: u64) -> Result<u64> {
        self.truncate(p, keep, false)
    }

    fn truncate(&mut self, p: &P, keep: u64, from_front: bool) -> Result<u64> {
        let partition = encode_key(p)?;
        let mut m = self.read_meta(&partition)?;
        if m.len <= keep {
            return Ok(0);
        }
        let drop_n = m.len - keep;
        let keys: Vec<Vec<u8>> = {
            let t = self.w.raw_table(&self.table)?;
            let end = prefix_end(&partition);
            let range = t.range::<&[u8]>((
                std::ops::Bound::Included(partition.as_slice()),
                end.as_deref()
                    .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
            ))?;
            let mut keys = Vec::with_capacity(drop_n as usize);
            if from_front {
                for e in range {
                    if keys.len() as u64 == drop_n {
                        break;
                    }
                    keys.push(e?.0.value().to_vec());
                }
            } else {
                for e in range.rev() {
                    if keys.len() as u64 == drop_n {
                        break;
                    }
                    keys.push(e?.0.value().to_vec());
                }
            }
            keys
        };
        let t = self.w.raw_table(&self.table)?;
        for k in &keys {
            t.remove(k.as_slice())?;
        }
        m.len -= keys.len() as u64;
        self.write_meta(&partition, m)?;
        self.w.changes.touch(&self.name, &partition);
        for k in &keys {
            self.w.changes.touch(&self.name, k);
        }
        Ok(keys.len() as u64)
    }

    /// Removes every element of the partition; returns how many there
    /// were. The next push starts numbering at zero again.
    pub fn clear(&mut self, p: &P) -> Result<u64> {
        let partition = encode_key(p)?;
        let m = self.read_meta(&partition)?;
        if m.len == 0 && m.first == 0 && m.next == 0 {
            return Ok(0);
        }
        let end = prefix_end(&partition);
        self.w.raw_table(&self.table)?.retain_in::<&[u8], _>(
            (
                std::ops::Bound::Included(partition.as_slice()),
                end.as_deref()
                    .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
            ),
            |_, _| false,
        )?;
        self.w.raw_table(&self.meta)?.remove(partition.as_slice())?;
        self.w.changes.touch(&self.name, &partition);
        Ok(m.len)
    }
}

/// Lists in a snapshot.
pub struct ListR<'s, P, V> {
    s: &'s Snapshot,
    table: String,
    meta: String,
    _p: PhantomData<(P, V)>,
}

impl<'s, P: KeyType, V> ListR<'s, P, V> {
    pub(crate) fn open(s: &'s Snapshot, name: &str) -> Result<Self> {
        open::<P>(&s.decl, name)?;
        Ok(ListR {
            s,
            table: table_name(name),
            meta: meta_table(name),
            _p: PhantomData,
        })
    }

    /// The number of elements in the partition.
    pub fn len(&self, p: &P) -> Result<u64> {
        let partition = encode_key(p)?;
        let Some(t) = self.s.raw_table(&self.meta)? else {
            return Ok(0);
        };
        match t.get(partition.as_slice())? {
            Some(g) => Ok(Meta::decode(g.value())?.len),
            None => Ok(0),
        }
    }

    /// Whether the partition is empty.
    pub fn is_empty(&self, p: &P) -> Result<bool> {
        Ok(self.len(p)? == 0)
    }

    /// The element at `seq`.
    pub fn get(&self, p: &P, seq: i64) -> Result<Option<V>>
    where
        V: DeserializeOwned,
    {
        let key = element_key(&encode_key(p)?, seq);
        let Some(t) = self.s.raw_table(&self.table)? else {
            return Ok(None);
        };
        match t.get(key.as_slice())? {
            Some(g) => Ok(Some(decode_value(g.value())?)),
            None => Ok(None),
        }
    }

    /// A scan over one partition in sequence order; `(seq, value)`
    /// items. The newest `n` are `.reverse().limit(n)`.
    pub fn range(&self, p: &P) -> Result<Scan<'s, (i64,), (i64, V)>>
    where
        V: DeserializeOwned,
    {
        let partition = encode_key(p)?;
        let base_len = partition.len();
        Ok(Scan::new(
            self.s,
            self.table.clone(),
            partition,
            Box::new(move |k, v| {
                let (seq,) = <(i64,)>::from_key(&k[base_len..])?;
                Ok((seq, decode_value(v)?))
            }),
        ))
    }

    /// The first element, if any.
    pub fn first(&self, p: &P) -> Result<Option<(i64, V)>>
    where
        V: DeserializeOwned,
    {
        Ok(self.range(p)?.limit(1).run()?.items.pop())
    }

    /// The last element, if any.
    pub fn last(&self, p: &P) -> Result<Option<(i64, V)>>
    where
        V: DeserializeOwned,
    {
        Ok(self.range(p)?.reverse().limit(1).run()?.items.pop())
    }
}
