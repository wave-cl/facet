//! A set: members, present or not.
//!
//! Table `c:<name>`: `enc(member) -> ()`.

use std::marker::PhantomData;

use redb::{ReadableTable, ReadableTableMetadata};

use crate::error::Result;
use crate::keyenc::KeyType;
use crate::prim::{Scan, check_kinds, encode_key, find, mismatch, table_name};
use crate::reader::Snapshot;
use crate::schema::CollectionKind;
use crate::writer::Writer;

fn open<M: KeyType>(decl: &crate::schema::ProjectionDecl, name: &str) -> Result<()> {
    let c = find(decl, name)?;
    match &c.kind {
        CollectionKind::Set { member } => {
            check_kinds(name, "set", "member", &member.0, &M::kinds())
        }
        other => Err(mismatch(name, other.name(), "set")),
    }
}

/// A set inside a batch.
pub struct SetW<'w, 't, M> {
    w: &'w mut Writer<'t>,
    name: String,
    table: String,
    _m: PhantomData<M>,
}

impl<'w, 't, M: KeyType> SetW<'w, 't, M> {
    pub(crate) fn open(w: &'w mut Writer<'t>, name: &str) -> Result<Self> {
        open::<M>(w.decl, name)?;
        Ok(SetW {
            w,
            name: name.to_owned(),
            table: table_name(name),
            _m: PhantomData,
        })
    }

    /// Adds `m`; `true` when it was not there.
    pub fn insert(&mut self, m: &M) -> Result<bool> {
        let key = encode_key(m)?;
        let was_new = self
            .w
            .raw_table(&self.table)?
            .insert(key.as_slice(), &b""[..])?
            .is_none();
        if was_new {
            self.w.changes.touch(&self.name, &key);
        }
        Ok(was_new)
    }

    /// Removes `m`; `true` when it was there.
    pub fn remove(&mut self, m: &M) -> Result<bool> {
        let key = encode_key(m)?;
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

    /// Whether `m` is a member, as the batch sees it.
    pub fn contains(&mut self, m: &M) -> Result<bool> {
        let key = encode_key(m)?;
        Ok(self
            .w
            .raw_table(&self.table)?
            .get(key.as_slice())?
            .is_some())
    }
}

/// A set in a snapshot.
pub struct SetR<'s, M> {
    s: &'s Snapshot,
    table: String,
    _m: PhantomData<M>,
}

impl<'s, M: KeyType> SetR<'s, M> {
    pub(crate) fn open(s: &'s Snapshot, name: &str) -> Result<Self> {
        open::<M>(&s.decl, name)?;
        Ok(SetR {
            s,
            table: table_name(name),
            _m: PhantomData,
        })
    }

    /// Whether `m` is a member.
    pub fn contains(&self, m: &M) -> Result<bool> {
        let key = encode_key(m)?;
        let Some(t) = self.s.raw_table(&self.table)? else {
            return Ok(false);
        };
        Ok(t.get(key.as_slice())?.is_some())
    }

    /// The number of members.
    pub fn len(&self) -> Result<u64> {
        Ok(match self.s.raw_table(&self.table)? {
            Some(t) => t.len()?,
            None => 0,
        })
    }

    /// Whether the set is empty.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// A scan over the members in key order.
    pub fn scan(&self) -> Scan<'s, M, M> {
        Scan::new(
            self.s,
            self.table.clone(),
            Vec::new(),
            Box::new(|k, _| M::from_key(k)),
        )
    }
}
