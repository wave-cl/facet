//! A register: one value.

use std::marker::PhantomData;

use redb::ReadableTable;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::Result;
use crate::limits::check_value_len;
use crate::prim::{find, mismatch, table_name};
use crate::reader::Snapshot;
use crate::schema::{CollectionKind, ValueSchema, check_value};
use crate::value::{from_value, to_value};
use crate::vbin;
use crate::writer::Writer;

fn open(decl: &crate::schema::ProjectionDecl, name: &str) -> Result<ValueSchema> {
    let c = find(decl, name)?;
    match &c.kind {
        CollectionKind::Register { value } => Ok(value.clone()),
        other => Err(mismatch(name, other.name(), "register")),
    }
}

/// A register inside a batch.
pub struct RegisterW<'w, 't, V> {
    w: &'w mut Writer<'t>,
    name: String,
    table: String,
    schema: ValueSchema,
    _v: PhantomData<V>,
}

impl<'w, 't, V> RegisterW<'w, 't, V> {
    pub(crate) fn open(w: &'w mut Writer<'t>, name: &str) -> Result<Self> {
        let schema = open(w.decl, name)?;
        Ok(RegisterW {
            w,
            name: name.to_owned(),
            table: table_name(name),
            schema,
            _v: PhantomData,
        })
    }
}

impl<V> RegisterW<'_, '_, V> {
    /// Sets the value.
    pub fn set(&mut self, v: &V) -> Result<()>
    where
        V: Serialize,
    {
        let bytes = vbin::encode(&check_value(&self.schema, to_value(v)?)?);
        check_value_len(bytes.len())?;
        self.w
            .raw_table(&self.table)?
            .insert(&b""[..], bytes.as_slice())?;
        self.w.changes.touch(&self.name, b"");
        Ok(())
    }

    /// The value, as the batch sees it.
    pub fn get(&mut self) -> Result<Option<V>>
    where
        V: DeserializeOwned,
    {
        let t = self.w.raw_table(&self.table)?;
        let Some(g) = t.get(&b""[..])? else {
            return Ok(None);
        };
        let v = vbin::decode(g.value())?;
        Ok(Some(from_value(v)?))
    }

    /// Clears the value; `true` when there was one.
    pub fn clear(&mut self) -> Result<bool> {
        let had = self.w.raw_table(&self.table)?.remove(&b""[..])?.is_some();
        if had {
            self.w.changes.touch(&self.name, b"");
        }
        Ok(had)
    }
}

/// A register in a snapshot.
pub struct RegisterR<'s, V> {
    s: &'s Snapshot,
    table: String,
    _v: PhantomData<V>,
}

impl<'s, V> RegisterR<'s, V> {
    pub(crate) fn open(s: &'s Snapshot, name: &str) -> Result<Self> {
        open(&s.decl, name)?;
        Ok(RegisterR {
            s,
            table: table_name(name),
            _v: PhantomData,
        })
    }
}

impl<V> RegisterR<'_, V> {
    /// The value.
    pub fn get(&self) -> Result<Option<V>>
    where
        V: DeserializeOwned,
    {
        let Some(t) = self.s.raw_table(&self.table)? else {
            return Ok(None);
        };
        let Some(g) = t.get(&b""[..])? else {
            return Ok(None);
        };
        let v = vbin::decode(g.value())?;
        Ok(Some(from_value(v)?))
    }

    /// The raw [`crate::Value`].
    pub fn get_value(&self) -> Result<Option<crate::Value>> {
        let Some(t) = self.s.raw_table(&self.table)? else {
            return Ok(None);
        };
        let Some(g) = t.get(&b""[..])? else {
            return Ok(None);
        };
        Ok(Some(vbin::decode(g.value())?))
    }
}
