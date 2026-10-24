//! The write side of a batch: typed handles over the transaction's
//! tables, with change tracking.

use std::collections::HashMap;

use redb::{Table, TableDefinition, WriteTransaction};

use crate::error::Result;
use crate::prim;
use crate::schema::ProjectionDecl;
use crate::watch::ChangeTracker;

pub(crate) type RawTable<'t> = Table<'t, &'static [u8], &'static [u8]>;

/// The handle a batch writes through. Reads see the batch's own earlier
/// writes.
pub struct Writer<'t> {
    txn: &'t WriteTransaction,
    pub(crate) decl: &'t ProjectionDecl,
    tables: HashMap<String, RawTable<'t>>,
    pub(crate) changes: ChangeTracker,
}

impl<'t> Writer<'t> {
    pub(crate) fn new(
        txn: &'t WriteTransaction,
        decl: &'t ProjectionDecl,
        changes: ChangeTracker,
    ) -> Self {
        Writer {
            txn,
            decl,
            tables: HashMap::new(),
            changes,
        }
    }

    /// Closes the tables and hands back the change tracker.
    pub(crate) fn finish(self) -> ChangeTracker {
        drop(self.tables);
        self.changes
    }

    /// The transaction's table of that name, opened once per batch.
    pub(crate) fn table(&mut self, name: &str) -> Result<&mut RawTable<'t>> {
        if !self.tables.contains_key(name) {
            let t = self
                .txn
                .open_table(TableDefinition::<&[u8], &[u8]>::new(name))?;
            self.tables.insert(name.to_owned(), t);
        }
        Ok(self.tables.get_mut(name).unwrap())
    }

    /// The declaration.
    pub fn decl(&self) -> &ProjectionDecl {
        self.decl
    }

    /// The register collection `name`.
    pub fn register<V>(&mut self, name: &str) -> Result<prim::register::RegisterW<'_, 't, V>> {
        prim::register::RegisterW::open(self, name)
    }

    /// The map collection `name` keyed by `K`.
    pub fn map<K: crate::KeyType, V>(
        &mut self,
        name: &str,
    ) -> Result<prim::map::MapW<'_, 't, K, V>> {
        prim::map::MapW::open(self, name)
    }
}
