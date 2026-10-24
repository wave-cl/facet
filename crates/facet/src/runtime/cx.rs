//! What a handler sees: the event and the projection's collections,
//! inside the batch's transaction.

use chron_client::Event;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use super::collections::*;
use super::error::HandlerError;
use super::projection::Codec;
use crate::error::Result;
use crate::keyenc::KeyType;
use crate::prim::{self, Num, Score};
use crate::writer::Writer;

/// The handler context: the event being applied and write access to the
/// projection's collections. Reads see this batch's earlier writes.
pub struct Cx<'c, 't> {
    w: &'c mut Writer<'t>,
    event: &'c Event,
    codec: Codec,
}

impl<'c, 't> Cx<'c, 't> {
    pub(crate) fn new(w: &'c mut Writer<'t>, event: &'c Event, codec: Codec) -> Self {
        Cx { w, event, codec }
    }

    /// The event.
    pub fn event(&self) -> &Event {
        self.event
    }

    /// The event's global position.
    pub fn position(&self) -> u64 {
        self.event.position
    }

    /// The event's commit time, unix nanoseconds.
    pub fn recorded_at(&self) -> i64 {
        self.event.recorded_at
    }

    /// The event's aggregate, `None` for a free event.
    pub fn aggregate(&self) -> Option<Uuid> {
        self.event.aggregate
    }

    /// The payload decoded with the projection's codec.
    pub fn decode<T: DeserializeOwned>(&self) -> std::result::Result<T, HandlerError> {
        self.decode_with(self.codec, self.event)
    }

    pub(crate) fn decode_with<T: DeserializeOwned>(
        &self,
        codec: Codec,
        ev: &Event,
    ) -> std::result::Result<T, HandlerError> {
        match codec {
            Codec::Json => serde_json::from_slice(&ev.payload)
                .map_err(|e| HandlerError::Decode(format!("position {}: {e}", ev.position))),
        }
    }

    /// The raw writer, for collections named by string.
    pub fn writer(&mut self) -> &mut Writer<'t> {
        self.w
    }

    /// The register.
    pub fn register<V>(
        &mut self,
        h: &RegisterHandle<V>,
    ) -> Result<prim::register::RegisterW<'_, 't, V>> {
        self.w.register(h.name())
    }

    /// The counters.
    pub fn counter<K: KeyType, N: Num>(
        &mut self,
        h: &CounterHandle<K, N>,
    ) -> Result<prim::counter::CounterW<'_, 't, K, N>> {
        self.w.counter(h.name())
    }

    /// The set.
    pub fn set<M: KeyType>(&mut self, h: &SetHandle<M>) -> Result<prim::set::SetW<'_, 't, M>> {
        self.w.set(h.name())
    }

    /// The map.
    pub fn map<K: KeyType, V>(
        &mut self,
        h: &MapHandle<K, V>,
    ) -> Result<prim::map::MapW<'_, 't, K, V>> {
        self.w.map(h.name())
    }

    /// The lists.
    pub fn list<P: KeyType, V>(
        &mut self,
        h: &ListHandle<P, V>,
    ) -> Result<prim::list::ListW<'_, 't, P, V>> {
        self.w.list(h.name())
    }

    /// The table.
    pub fn table<K: KeyType, R>(
        &mut self,
        h: &TableHandle<K, R>,
    ) -> Result<prim::table::TableW<'_, 't, K, R>> {
        self.w.table(h.name())
    }

    /// The sorted sets.
    pub fn sorted_set<P: KeyType, M: KeyType, S: Score>(
        &mut self,
        h: &SortedSetHandle<P, M, S>,
    ) -> Result<prim::sorted_set::SortedSetW<'_, 't, P, M, S>> {
        self.w.sorted_set(h.name())
    }

    /// The buckets.
    pub fn buckets<K: KeyType, N: Num>(
        &mut self,
        h: &BucketsHandle<K, N>,
    ) -> Result<prim::buckets::BucketsW<'_, 't, K, N>> {
        self.w.buckets(h.name())
    }
}
