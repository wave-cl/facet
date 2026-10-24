//! A map: values by key, one entry per key.
//!
//! Table `c:<name>`: `enc(key) -> vbin(value)`.

use std::marker::PhantomData;

use redb::{ReadableTable, ReadableTableMetadata};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::Result;
use crate::keyenc::KeyType;
use crate::prim::{
    Scan, check_kinds, decode_value, encode_key, encode_value, find, mismatch, table_name,
};
use crate::reader::Snapshot;
use crate::schema::{CollectionKind, ValueSchema};
use crate::vbin;
use crate::writer::Writer;

fn open<K: KeyType>(decl: &crate::schema::ProjectionDecl, name: &str) -> Result<ValueSchema> {
    let c = find(decl, name)?;
    match &c.kind {
        CollectionKind::Map { key, value } => {
            check_kinds(name, "map", "key", &key.0, &K::kinds())?;
            Ok(value.clone())
        }
        other => Err(mismatch(name, other.name(), "map")),
    }
}

/// A map inside a batch.
pub struct MapW<'w, 't, K, V> {
    w: &'w mut Writer<'t>,
    name: String,
    table: String,
    schema: ValueSchema,
    _k: PhantomData<(K, V)>,
}

impl<'w, 't, K: KeyType, V> MapW<'w, 't, K, V> {
    pub(crate) fn open(w: &'w mut Writer<'t>, name: &str) -> Result<Self> {
        let schema = open::<K>(w.decl, name)?;
        Ok(MapW {
            w,
            name: name.to_owned(),
            table: table_name(name),
            schema,
            _k: PhantomData,
        })
    }
}

impl<K: KeyType, V> MapW<'_, '_, K, V> {
    /// Sets the value at `k`.
    pub fn put(&mut self, k: &K, v: &V) -> Result<()>
    where
        V: Serialize,
    {
        let key = encode_key(k)?;
        let val = encode_value(&self.schema, v)?;
        self.w
            .raw_table(&self.table)?
            .insert(key.as_slice(), val.as_slice())?;
        self.w.changes.touch(&self.name, &key);
        Ok(())
    }

    /// The value at `k`, as the batch sees it.
    pub fn get(&mut self, k: &K) -> Result<Option<V>>
    where
        V: DeserializeOwned,
    {
        let key = encode_key(k)?;
        let t = self.w.raw_table(&self.table)?;
        let Some(g) = t.get(key.as_slice())? else {
            return Ok(None);
        };
        Ok(Some(decode_value(g.value())?))
    }

    /// Removes `k`; `true` when it was present.
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

    /// Reads, maps and writes back: `None` from `f` removes the key.
    pub fn update(&mut self, k: &K, f: impl FnOnce(Option<V>) -> Option<V>) -> Result<Option<V>>
    where
        V: Serialize + DeserializeOwned,
    {
        let current = self.get(k)?;
        match f(current) {
            Some(v) => {
                self.put(k, &v)?;
                Ok(Some(v))
            }
            None => {
                self.remove(k)?;
                Ok(None)
            }
        }
    }

    /// Whether `k` is present.
    pub fn contains(&mut self, k: &K) -> Result<bool> {
        let key = encode_key(k)?;
        Ok(self
            .w
            .raw_table(&self.table)?
            .get(key.as_slice())?
            .is_some())
    }
}

/// A map in a snapshot.
pub struct MapR<'s, K, V> {
    s: &'s Snapshot,
    table: String,
    _k: PhantomData<(K, V)>,
}

impl<'s, K: KeyType, V> MapR<'s, K, V> {
    pub(crate) fn open(s: &'s Snapshot, name: &str) -> Result<Self> {
        open::<K>(&s.decl, name)?;
        Ok(MapR {
            s,
            table: table_name(name),
            _k: PhantomData,
        })
    }

    /// The number of entries.
    pub fn len(&self) -> Result<u64> {
        Ok(match self.s.raw_table(&self.table)? {
            Some(t) => t.len()?,
            None => 0,
        })
    }

    /// Whether the map is empty.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// A scan over the whole map; narrow it with the builder.
    pub fn scan(&self) -> Scan<'s, K, (K, V)>
    where
        V: DeserializeOwned,
    {
        Scan::new(
            self.s,
            self.table.clone(),
            Vec::new(),
            Box::new(|k, v| Ok((K::from_key(k)?, decode_value(v)?))),
        )
    }

    /// A scan yielding raw [`crate::Value`]s.
    pub fn scan_values(&self) -> Scan<'s, K, (K, crate::Value)> {
        Scan::new(
            self.s,
            self.table.clone(),
            Vec::new(),
            Box::new(|k, v| Ok((K::from_key(k)?, vbin::decode(v)?))),
        )
    }

    /// The value at `k`.
    pub fn get(&self, k: &K) -> Result<Option<V>>
    where
        V: DeserializeOwned,
    {
        let key = encode_key(k)?;
        let Some(t) = self.s.raw_table(&self.table)? else {
            return Ok(None);
        };
        let Some(g) = t.get(key.as_slice())? else {
            return Ok(None);
        };
        Ok(Some(decode_value(g.value())?))
    }

    /// The raw [`crate::Value`] at `k`.
    pub fn get_value(&self, k: &K) -> Result<Option<crate::Value>> {
        let key = encode_key(k)?;
        let Some(t) = self.s.raw_table(&self.table)? else {
            return Ok(None);
        };
        let Some(g) = t.get(key.as_slice())? else {
            return Ok(None);
        };
        Ok(Some(vbin::decode(g.value())?))
    }

    /// Whether `k` is present.
    pub fn contains(&self, k: &K) -> Result<bool> {
        let key = encode_key(k)?;
        let Some(t) = self.s.raw_table(&self.table)? else {
            return Ok(false);
        };
        Ok(t.get(key.as_slice())?.is_some())
    }
}
