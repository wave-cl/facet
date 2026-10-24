//! A map: values by key, one entry per key.

use std::marker::PhantomData;

use redb::{ReadableTable, ReadableTableMetadata};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::{Error, Result};
use crate::keyenc::KeyType;
use crate::limits::{check_key_len, check_value_len};
use crate::prim::{find, kinds_str, mismatch, table_name};
use crate::reader::{Cursor, Page, RawScan, Snapshot};
use crate::schema::{CollectionKind, ValueSchema, check_value};
use crate::value::{from_value, to_value};
use crate::vbin;
use crate::writer::Writer;

fn open<K: KeyType>(decl: &crate::schema::ProjectionDecl, name: &str) -> Result<ValueSchema> {
    let c = find(decl, name)?;
    match &c.kind {
        CollectionKind::Map { key, value } => {
            let asked = K::kinds();
            if asked != key.0 {
                return Err(mismatch(
                    name,
                    format!("map keyed by {}", kinds_str(&key.0)),
                    format!("map keyed by {}", kinds_str(&asked)),
                ));
            }
            Ok(value.clone())
        }
        other => Err(mismatch(name, other.name(), "map")),
    }
}

fn encode_key<K: KeyType>(k: &K) -> Result<Vec<u8>> {
    let bytes = k.to_key();
    check_key_len(bytes.len())?;
    Ok(bytes)
}

fn encode_value<V: Serialize>(schema: &ValueSchema, v: &V) -> Result<Vec<u8>> {
    let bytes = vbin::encode(&check_value(schema, to_value(v)?)?);
    check_value_len(bytes.len())?;
    Ok(bytes)
}

fn decode_value<V: DeserializeOwned>(bytes: &[u8]) -> Result<V> {
    from_value(vbin::decode(bytes)?)
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
            .table(&self.table)?
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
        let t = self.w.table(&self.table)?;
        let Some(g) = t.get(key.as_slice())? else {
            return Ok(None);
        };
        Ok(Some(decode_value(g.value())?))
    }

    /// Removes `k`; `true` when it was present.
    pub fn remove(&mut self, k: &K) -> Result<bool> {
        let key = encode_key(k)?;
        let had = self.w.table(&self.table)?.remove(key.as_slice())?.is_some();
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
        Ok(self.w.table(&self.table)?.get(key.as_slice())?.is_some())
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
        Ok(match self.s.table(&self.table)? {
            Some(t) => t.len()?,
            None => 0,
        })
    }

    /// Whether the map is empty.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// A scan over the whole map; narrow it with the builder.
    pub fn scan(&self) -> MapScan<'_, 's, K, V> {
        MapScan {
            r: self,
            raw: RawScan::default(),
            err: None,
        }
    }

    /// Counts the entries under a key prefix, stopping at `max` (the
    /// flag says whether it stopped).
    pub fn count_prefix<P: KeyType>(&self, prefix: &P, max: u64) -> Result<(u64, bool)> {
        check_prefix::<K, P>()?;
        let raw = RawScan {
            prefix: prefix.to_key(),
            limit: usize::try_from(max).unwrap_or(usize::MAX),
            ..Default::default()
        };
        let (items, next) = raw.run(self.s.table(&self.table)?.as_deref())?;
        Ok((items.len() as u64, next.is_some()))
    }
}

fn check_prefix<K: KeyType, P: KeyType>() -> Result<()> {
    let k = K::kinds();
    let p = P::kinds();
    if p.len() > k.len() || k[..p.len()] != p[..] {
        return Err(Error::Key(format!(
            "{} is not a prefix of the key {}",
            kinds_str(&p),
            kinds_str(&k)
        )));
    }
    Ok(())
}

impl<K: KeyType, V> MapR<'_, K, V> {
    /// The value at `k`.
    pub fn get(&self, k: &K) -> Result<Option<V>>
    where
        V: DeserializeOwned,
    {
        let key = encode_key(k)?;
        let Some(t) = self.s.table(&self.table)? else {
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
        let Some(t) = self.s.table(&self.table)? else {
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
        let Some(t) = self.s.table(&self.table)? else {
            return Ok(false);
        };
        Ok(t.get(key.as_slice())?.is_some())
    }
}

/// A scan over a map, built then run.
pub struct MapScan<'r, 's, K, V> {
    r: &'r MapR<'s, K, V>,
    raw: RawScan,
    err: Option<Error>,
}

impl<K: KeyType, V: DeserializeOwned> MapScan<'_, '_, K, V> {
    /// Only keys whose leading parts equal `prefix`.
    pub fn prefix<P: KeyType>(mut self, prefix: &P) -> Self {
        if let Err(e) = check_prefix::<K, P>() {
            self.err = Some(e);
        }
        self.raw.prefix = prefix.to_key();
        self
    }

    /// Only keys at or above `k`.
    pub fn from(mut self, k: &K) -> Self {
        self.raw.from = Some(k.to_key());
        self
    }

    /// Only keys at or below `k`.
    pub fn to(mut self, k: &K) -> Self {
        self.raw.to = Some(k.to_key());
        self
    }

    /// Continues after the key a previous page ended with.
    pub fn after(mut self, cursor: &Cursor) -> Self {
        self.raw.after = Some(cursor.0.clone());
        self
    }

    /// Descending order.
    pub fn reverse(mut self) -> Self {
        self.raw.reverse = true;
        self
    }

    /// At most `n` entries (0 = all).
    pub fn limit(mut self, n: usize) -> Self {
        self.raw.limit = n;
        self
    }

    /// Runs the scan.
    pub fn run(self) -> Result<Page<(K, V)>> {
        if let Some(e) = self.err {
            return Err(e);
        }
        let (items, next) = self.raw.run(self.r.s.table(&self.r.table)?.as_deref())?;
        let mut out = Vec::with_capacity(items.len());
        for (k, v) in items {
            out.push((K::from_key(&k)?, decode_value(&v)?));
        }
        Ok(Page {
            items: out,
            next,
            checkpoint: self.r.s.checkpoint(),
        })
    }
}
