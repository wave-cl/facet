//! Declaring collections and the typed handles that name them.

use std::marker::PhantomData;
use std::sync::Arc;

use crate::error::Error;
use crate::keyenc::KeyType;
use crate::prim::{Num, Score};
use crate::schema::{
    CollectionDecl, CollectionKind, ColumnDecl, IndexDecl, KeySchema, TypeSchema, ValueSchema,
    Width,
};

/// Collects a projection's collection declarations.
#[derive(Default)]
pub struct Decl {
    collections: Vec<CollectionDecl>,
    errors: Vec<Error>,
}

macro_rules! handle {
    ($(#[$doc:meta])* $name:ident < $($g:ident),* >) => {
        $(#[$doc])*
        pub struct $name<$($g),*> {
            name: Arc<str>,
            _t: PhantomData<fn() -> ($($g,)*)>,
        }
        impl<$($g),*> Clone for $name<$($g),*> {
            fn clone(&self) -> Self {
                $name { name: self.name.clone(), _t: PhantomData }
            }
        }
        impl<$($g),*> $name<$($g),*> {
            /// The collection's name.
            pub fn name(&self) -> &str {
                &self.name
            }
            fn new(name: &str) -> Self {
                $name { name: Arc::from(name), _t: PhantomData }
            }
        }
    };
}

handle!(
    /// Names a register collection.
    RegisterHandle<V>
);
handle!(
    /// Names a counter collection.
    CounterHandle<K, N>
);
handle!(
    /// Names a set collection.
    SetHandle<M>
);
handle!(
    /// Names a map collection.
    MapHandle<K, V>
);
handle!(
    /// Names a list collection.
    ListHandle<P, V>
);
handle!(
    /// Names a table collection.
    TableHandle<K, R>
);
handle!(
    /// Names a sorted set collection.
    SortedSetHandle<P, M, S>
);
handle!(
    /// Names a buckets collection.
    BucketsHandle<K, N>
);

impl Decl {
    fn push(&mut self, name: &str, kind: CollectionKind) {
        if name.starts_with("__") {
            self.errors.push(Error::Declaration(format!(
                "collection {name:?}: names starting with __ are reserved"
            )));
        }
        self.collections.push(CollectionDecl {
            name: name.to_owned(),
            kind,
        });
    }

    pub(crate) fn take_errors(&mut self) -> Vec<Error> {
        std::mem::take(&mut self.errors)
    }

    pub(crate) fn into_collections(self) -> Vec<CollectionDecl> {
        self.collections
    }

    /// A register holding any value.
    pub fn register<V>(&mut self, name: &str) -> RegisterHandle<V> {
        self.push(
            name,
            CollectionKind::Register {
                value: ValueSchema::Any,
            },
        );
        RegisterHandle::new(name)
    }

    /// A register whose value is checked against a type.
    pub fn register_typed<V>(&mut self, name: &str, ty: TypeSchema) -> RegisterHandle<V> {
        self.push(
            name,
            CollectionKind::Register {
                value: ValueSchema::Typed(ty),
            },
        );
        RegisterHandle::new(name)
    }

    /// Counters keyed by `K` (`()` for one counter) holding `N`.
    pub fn counter<K: KeyType, N: Num>(&mut self, name: &str) -> CounterHandle<K, N> {
        self.push(
            name,
            CollectionKind::Counter {
                key: KeySchema(K::kinds()),
                num: N::KIND,
            },
        );
        CounterHandle::new(name)
    }

    /// A set of `M`.
    pub fn set<M: KeyType>(&mut self, name: &str) -> SetHandle<M> {
        self.push(
            name,
            CollectionKind::Set {
                member: KeySchema(M::kinds()),
            },
        );
        SetHandle::new(name)
    }

    /// A map from `K` to any value.
    pub fn map<K: KeyType, V>(&mut self, name: &str) -> MapHandle<K, V> {
        self.push(
            name,
            CollectionKind::Map {
                key: KeySchema(K::kinds()),
                value: ValueSchema::Any,
            },
        );
        MapHandle::new(name)
    }

    /// A map from `K` to values checked against a type.
    pub fn map_typed<K: KeyType, V>(&mut self, name: &str, ty: TypeSchema) -> MapHandle<K, V> {
        self.push(
            name,
            CollectionKind::Map {
                key: KeySchema(K::kinds()),
                value: ValueSchema::Typed(ty),
            },
        );
        MapHandle::new(name)
    }

    /// Lists partitioned by `P` (`()` for one list) of any value.
    pub fn list<P: KeyType, V>(&mut self, name: &str) -> ListHandle<P, V> {
        self.push(
            name,
            CollectionKind::List {
                partition: KeySchema(P::kinds()),
                value: ValueSchema::Any,
            },
        );
        ListHandle::new(name)
    }

    /// Lists whose elements are checked against a type.
    pub fn list_typed<P: KeyType, V>(&mut self, name: &str, ty: TypeSchema) -> ListHandle<P, V> {
        self.push(
            name,
            CollectionKind::List {
                partition: KeySchema(P::kinds()),
                value: ValueSchema::Typed(ty),
            },
        );
        ListHandle::new(name)
    }

    /// A table with primary key `K` and rows `R`; declare its columns on
    /// the builder.
    pub fn table<K: KeyType, R>(&mut self, name: &str) -> TableBuilder<'_, K, R> {
        TableBuilder {
            decl: self,
            name: name.to_owned(),
            columns: Vec::new(),
            primary_key: Vec::new(),
            indexes: Vec::new(),
            _t: PhantomData,
        }
    }

    /// Sorted sets partitioned by `P` (`()` for one), members `M`, scores
    /// `S`.
    pub fn sorted_set<P: KeyType, M: KeyType, S: Score>(
        &mut self,
        name: &str,
    ) -> SortedSetHandle<P, M, S> {
        self.push(
            name,
            CollectionKind::SortedSet {
                partition: KeySchema(P::kinds()),
                member: KeySchema(M::kinds()),
                score: S::KIND,
            },
        );
        SortedSetHandle::new(name)
    }

    /// Buckets keyed by `K` (`()` for one series) summing `N`, kept for
    /// each width.
    pub fn buckets<K: KeyType, N: Num>(
        &mut self,
        name: &str,
        widths: impl IntoIterator<Item = Width>,
    ) -> BucketsHandle<K, N> {
        self.push(
            name,
            CollectionKind::Buckets {
                key: KeySchema(K::kinds()),
                num: N::KIND,
                widths: widths.into_iter().collect(),
            },
        );
        BucketsHandle::new(name)
    }
}

/// Declares a table's columns, primary key and indexes.
pub struct TableBuilder<'d, K, R> {
    decl: &'d mut Decl,
    name: String,
    columns: Vec<ColumnDecl>,
    primary_key: Vec<String>,
    indexes: Vec<IndexDecl>,
    _t: PhantomData<fn() -> (K, R)>,
}

impl<K: KeyType, R> TableBuilder<'_, K, R> {
    /// A column that is never null.
    pub fn column(mut self, name: &str, ty: TypeSchema) -> Self {
        self.columns.push(ColumnDecl {
            name: name.to_owned(),
            ty,
            nullable: false,
        });
        self
    }

    /// A column that may be null.
    pub fn nullable(mut self, name: &str, ty: TypeSchema) -> Self {
        self.columns.push(ColumnDecl {
            name: name.to_owned(),
            ty,
            nullable: true,
        });
        self
    }

    /// The primary key columns, in order; their types must match `K`.
    pub fn primary_key<'a>(mut self, columns: impl IntoIterator<Item = &'a str>) -> Self {
        self.primary_key = columns.into_iter().map(str::to_owned).collect();
        self
    }

    /// A secondary index over columns, in order.
    pub fn index<'a>(mut self, name: &str, columns: impl IntoIterator<Item = &'a str>) -> Self {
        self.indexes.push(IndexDecl {
            name: name.to_owned(),
            columns: columns.into_iter().map(str::to_owned).collect(),
            unique: false,
        });
        self
    }

    /// A unique secondary index.
    pub fn unique_index<'a>(
        mut self,
        name: &str,
        columns: impl IntoIterator<Item = &'a str>,
    ) -> Self {
        self.indexes.push(IndexDecl {
            name: name.to_owned(),
            columns: columns.into_iter().map(str::to_owned).collect(),
            unique: true,
        });
        self
    }

    /// Declares the table.
    pub fn build(self) -> TableHandle<K, R> {
        let pk_kinds: Vec<_> = self
            .primary_key
            .iter()
            .filter_map(|n| self.columns.iter().find(|c| &c.name == n))
            .filter_map(|c| c.ty.key_kind())
            .collect();
        if pk_kinds != K::kinds() {
            self.decl.errors.push(Error::Declaration(format!(
                "table {}: the primary key columns do not match the key type {:?}",
                self.name,
                K::kinds()
            )));
        }
        let name = self.name.clone();
        self.decl.push(
            &name,
            CollectionKind::Table {
                columns: self.columns,
                primary_key: self.primary_key,
                indexes: self.indexes,
            },
        );
        TableHandle::new(&name)
    }
}
