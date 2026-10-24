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
    pub(crate) fn raw_table(&mut self, name: &str) -> Result<&mut RawTable<'t>> {
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

    /// The set collection `name` with members `M`.
    pub fn set<M: crate::KeyType>(&mut self, name: &str) -> Result<prim::set::SetW<'_, 't, M>> {
        prim::set::SetW::open(self, name)
    }

    /// The counter collection `name` keyed by `K` holding `N`.
    pub fn counter<K: crate::KeyType, N: prim::Num>(
        &mut self,
        name: &str,
    ) -> Result<prim::counter::CounterW<'_, 't, K, N>> {
        prim::counter::CounterW::open(self, name)
    }

    /// The list collection `name` partitioned by `P`.
    pub fn list<P: crate::KeyType, V>(
        &mut self,
        name: &str,
    ) -> Result<prim::list::ListW<'_, 't, P, V>> {
        prim::list::ListW::open(self, name)
    }

    /// The table collection `name` with primary key `K` and rows `R`.
    pub fn table<K: crate::KeyType, R>(
        &mut self,
        name: &str,
    ) -> Result<prim::table::TableW<'_, 't, K, R>> {
        prim::table::TableW::open(self, name)
    }

    /// The sorted set collection `name` partitioned by `P`, members `M`,
    /// scores `S`.
    pub fn sorted_set<P: crate::KeyType, M: crate::KeyType, S: prim::Score>(
        &mut self,
        name: &str,
    ) -> Result<prim::sorted_set::SortedSetW<'_, 't, P, M, S>> {
        prim::sorted_set::SortedSetW::open(self, name)
    }

    /// The buckets collection `name` keyed by `K` summing `N`.
    pub fn buckets<K: crate::KeyType, N: prim::Num>(
        &mut self,
        name: &str,
    ) -> Result<prim::buckets::BucketsW<'_, 't, K, N>> {
        prim::buckets::BucketsW::open(self, name)
    }
}
