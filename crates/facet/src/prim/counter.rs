//! Counters: numbers by key; the empty key tuple makes one counter.
//!
//! Table `c:<name>`: `enc(key) -> i64 big-endian | the 16 bytes of a
//! decimal`.

use std::marker::PhantomData;

use redb::{ReadableTable, ReadableTableMetadata};
use rust_decimal::Decimal;

use crate::error::{Error, Result};
use crate::keyenc::KeyType;
use crate::prim::{Scan, check_kinds, encode_key, find, mismatch, table_name};
use crate::reader::Snapshot;
use crate::schema::{CollectionKind, NumKind};
use crate::value::Value;
use crate::writer::Writer;

/// A number a counter or bucket holds: `i64` or `Decimal`.
pub trait Num: Copy + PartialOrd + std::fmt::Debug + Send + Sync + 'static {
    /// The declared kind this type matches.
    const KIND: NumKind;
    /// Zero.
    fn zero() -> Self;
    /// The stored bytes.
    fn encode(&self) -> Vec<u8>;
    /// From the stored bytes.
    fn decode(bytes: &[u8]) -> Result<Self>;
    /// `self + other`, or `None` on overflow.
    fn checked_add(self, other: Self) -> Option<Self>;
    /// As a [`Value`].
    fn to_value(&self) -> Value;
}

impl Num for i64 {
    const KIND: NumKind = NumKind::Int;
    fn zero() -> Self {
        0
    }
    fn encode(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
    fn decode(bytes: &[u8]) -> Result<Self> {
        let raw: [u8; 8] = bytes
            .try_into()
            .map_err(|_| Error::Value("int bytes".into()))?;
        Ok(i64::from_be_bytes(raw))
    }
    fn checked_add(self, other: Self) -> Option<Self> {
        i64::checked_add(self, other)
    }
    fn to_value(&self) -> Value {
        Value::Int(*self)
    }
}

impl Num for Decimal {
    const KIND: NumKind = NumKind::Decimal;
    fn zero() -> Self {
        Decimal::ZERO
    }
    fn encode(&self) -> Vec<u8> {
        self.serialize().to_vec()
    }
    fn decode(bytes: &[u8]) -> Result<Self> {
        let raw: [u8; 16] = bytes
            .try_into()
            .map_err(|_| Error::Value("decimal bytes".into()))?;
        Ok(Decimal::deserialize(raw))
    }
    fn checked_add(self, other: Self) -> Option<Self> {
        Decimal::checked_add(self, other)
    }
    fn to_value(&self) -> Value {
        Value::Decimal(*self)
    }
}

fn open<K: KeyType, N: Num>(decl: &crate::schema::ProjectionDecl, name: &str) -> Result<()> {
    let c = find(decl, name)?;
    match &c.kind {
        CollectionKind::Counter { key, num } => {
            check_kinds(name, "counter", "key", &key.0, &K::kinds())?;
            if *num != N::KIND {
                return Err(mismatch(
                    name,
                    format!("counter of {num:?}"),
                    format!("counter of {:?}", N::KIND),
                ));
            }
            Ok(())
        }
        other => Err(mismatch(name, other.name(), "counter")),
    }
}

/// Counters inside a batch.
pub struct CounterW<'w, 't, K, N> {
    w: &'w mut Writer<'t>,
    name: String,
    table: String,
    _k: PhantomData<(K, N)>,
}

impl<'w, 't, K: KeyType, N: Num> CounterW<'w, 't, K, N> {
    pub(crate) fn open(w: &'w mut Writer<'t>, name: &str) -> Result<Self> {
        open::<K, N>(w.decl, name)?;
        Ok(CounterW {
            w,
            name: name.to_owned(),
            table: table_name(name),
            _k: PhantomData,
        })
    }

    /// The counter at `k`, as the batch sees it (`None` when never set).
    pub fn get(&mut self, k: &K) -> Result<Option<N>> {
        let key = encode_key(k)?;
        let t = self.w.raw_table(&self.table)?;
        match t.get(key.as_slice())? {
            Some(g) => Ok(Some(N::decode(g.value())?)),
            None => Ok(None),
        }
    }

    /// Adds `by` and returns the new value; an overflow is an error and
    /// fails the batch.
    pub fn add(&mut self, k: &K, by: N) -> Result<N> {
        let key = encode_key(k)?;
        let t = self.w.raw_table(&self.table)?;
        let current = match t.get(key.as_slice())? {
            Some(g) => N::decode(g.value())?,
            None => N::zero(),
        };
        let next = current.checked_add(by).ok_or_else(|| {
            Error::Overflow(format!("counter {}: {current:?} + {by:?}", self.name))
        })?;
        t.insert(key.as_slice(), N::encode(&next).as_slice())?;
        self.w.changes.touch(&self.name, &key);
        Ok(next)
    }

    /// Sets the counter.
    pub fn set(&mut self, k: &K, v: N) -> Result<()> {
        let key = encode_key(k)?;
        self.w
            .raw_table(&self.table)?
            .insert(key.as_slice(), N::encode(&v).as_slice())?;
        self.w.changes.touch(&self.name, &key);
        Ok(())
    }

    /// Removes the counter; `true` when it existed.
    pub fn remove(&mut self, k: &K) -> Result<bool> {
        let key = encode_key(k)?;
        let had = self
            .w
            .raw_table(&self.table)?
            .remove(key.as_slice())?
            .is_some();
        if had {
            self.w.changes.touch(&self.name, &key);
        }
        Ok(had)
    }
}

/// Counters in a snapshot.
pub struct CounterR<'s, K, N> {
    s: &'s Snapshot,
    table: String,
    _k: PhantomData<(K, N)>,
}

impl<'s, K: KeyType, N: Num> CounterR<'s, K, N> {
    pub(crate) fn open(s: &'s Snapshot, name: &str) -> Result<Self> {
        open::<K, N>(&s.decl, name)?;
        Ok(CounterR {
            s,
            table: table_name(name),
            _k: PhantomData,
        })
    }

    /// The counter at `k` (`None` when never set).
    pub fn get(&self, k: &K) -> Result<Option<N>> {
        let key = encode_key(k)?;
        let Some(t) = self.s.raw_table(&self.table)? else {
            return Ok(None);
        };
        match t.get(key.as_slice())? {
            Some(g) => Ok(Some(N::decode(g.value())?)),
            None => Ok(None),
        }
    }

    /// The number of counters.
    pub fn len(&self) -> Result<u64> {
        Ok(match self.s.raw_table(&self.table)? {
            Some(t) => t.len()?,
            None => 0,
        })
    }

    /// Whether there are no counters.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// A scan over the counters in key order.
    pub fn scan(&self) -> Scan<'s, K, (K, N)> {
        Scan::new(
            self.s,
            self.table.clone(),
            Vec::new(),
            Box::new(|k, v| Ok((K::from_key(k)?, N::decode(v)?))),
        )
    }
}
