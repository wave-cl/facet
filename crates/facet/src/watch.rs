//! What a committed batch changed, for live queries. Kept in memory
//! only: a watcher that falls behind re-reads from a snapshot.

use bytes::Bytes;

use crate::meta::Checkpoint;

/// The keys one batch touched in one collection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Changed {
    /// These encoded keys, sorted and without duplicates.
    Keys(Vec<Bytes>),
    /// Too many keys to list, or the whole collection was cleared or
    /// rebuilt.
    All,
}

/// One collection's changes in a batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionChange {
    /// The collection.
    pub name: String,
    /// What changed.
    pub changed: Changed,
}

/// Everything one committed batch changed, with the checkpoint it
/// committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchSummary {
    /// The checkpoint the batch committed.
    pub checkpoint: Checkpoint,
    /// The collections touched, in name order.
    pub collections: Vec<CollectionChange>,
}

impl BatchSummary {
    /// The changes to one collection, if it was touched.
    pub fn collection(&self, name: &str) -> Option<&Changed> {
        self.collections
            .iter()
            .find(|c| c.name == name)
            .map(|c| &c.changed)
    }
}

/// Collects changed keys while a batch runs.
pub(crate) struct ChangeTracker {
    max_keys: usize,
    by: std::collections::BTreeMap<String, Tracked>,
}

enum Tracked {
    Keys(std::collections::BTreeSet<Bytes>),
    All,
}

impl ChangeTracker {
    pub(crate) fn new(max_keys: usize) -> Self {
        ChangeTracker {
            max_keys,
            by: Default::default(),
        }
    }

    pub(crate) fn touch(&mut self, collection: &str, key: &[u8]) {
        match self.by.get_mut(collection) {
            Some(Tracked::All) => {}
            Some(Tracked::Keys(set)) => {
                set.insert(Bytes::copy_from_slice(key));
                if set.len() > self.max_keys {
                    self.by.insert(collection.to_owned(), Tracked::All);
                }
            }
            None => {
                let mut set = std::collections::BTreeSet::new();
                set.insert(Bytes::copy_from_slice(key));
                self.by.insert(collection.to_owned(), Tracked::Keys(set));
            }
        }
    }

    pub(crate) fn touch_all(&mut self, collection: &str) {
        self.by.insert(collection.to_owned(), Tracked::All);
    }

    pub(crate) fn finish(self, checkpoint: Checkpoint) -> BatchSummary {
        BatchSummary {
            checkpoint,
            collections: self
                .by
                .into_iter()
                .map(|(name, t)| CollectionChange {
                    name,
                    changed: match t {
                        Tracked::Keys(set) => Changed::Keys(set.into_iter().collect()),
                        Tracked::All => Changed::All,
                    },
                })
                .collect(),
        }
    }
}
