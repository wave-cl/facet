//! A projection as the application writes it.

use std::collections::BTreeSet;
use std::sync::Arc;

use bytes::Bytes;
use chron_client::Event;
use serde::de::DeserializeOwned;

use super::collections::Decl;
use super::cx::Cx;
use super::deadletter;
use super::error::HandlerError;
use crate::error::{Error, Result};
use crate::schema::{CollectionDecl, ProjectionDecl};

/// Where a projection's events come from.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum Source {
    /// Every event of the chron database.
    All,
    /// The events carrying one tag.
    Tag(Bytes),
    /// The members of a chron stream the operator defined.
    Stream(String),
    /// Derived from the declared event types: one type is its tag;
    /// several are the chron stream `facet-<projection>` over those tags,
    /// which the runtime defines.
    #[default]
    Auto,
}

impl Source {
    /// `Tag` from anything bytes-like.
    pub fn tag(t: impl Into<Bytes>) -> Source {
        Source::Tag(t.into())
    }
    /// `Stream` from a name.
    pub fn stream(name: impl Into<String>) -> Source {
        Source::Stream(name.into())
    }
}

/// What happens when a handler fails on an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OnError {
    /// The projection stops (`FAILED`) with its checkpoint before the
    /// event; `resume` retries it.
    #[default]
    Fail,
    /// The event is recorded in the projection's dead letters and
    /// skipped; the batch commits without it.
    Skip,
}

/// How payloads are decoded for typed handlers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Codec {
    /// JSON through serde.
    #[default]
    Json,
}

/// What to do when the stream `facet-<projection>` exists with other
/// tags than the projection declares.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StreamPolicy {
    /// Fail the projection and say so.
    #[default]
    Refuse,
    /// Delete and define it again (ending other subscribers on it).
    Recreate,
}

pub(crate) type HandlerFn =
    Arc<dyn Fn(&mut Cx<'_, '_>, &Event) -> std::result::Result<(), HandlerError> + Send + Sync>;

pub(crate) struct Handler {
    /// The tag the handler runs for; `None` runs for every event.
    pub event_type: Option<Bytes>,
    pub f: HandlerFn,
}

/// A projection: a source, collections and handlers. Built with
/// [`Projection::new`].
pub struct Projection {
    name: String,
    version: String,
    source: Source,
    on_error: OnError,
    codec: Codec,
    stream_policy: StreamPolicy,
    collections: Vec<CollectionDecl>,
    handlers: Vec<Handler>,
}

impl std::fmt::Debug for Projection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Projection")
            .field("name", &self.name)
            .field("version", &self.version)
            .field("source", &self.source)
            .field("handlers", &self.handlers.len())
            .finish()
    }
}

impl Projection {
    /// Starts a projection of that name (`[a-z0-9_-]`, at most 48 bytes).
    #[allow(clippy::new_ret_no_self)]
    pub fn new(name: impl Into<String>) -> ProjectionBuilder {
        ProjectionBuilder {
            name: name.into(),
            version: "1".into(),
            source: Source::Auto,
            on_error: OnError::Fail,
            codec: Codec::Json,
            stream_policy: StreamPolicy::Refuse,
            decl: Decl::default(),
            handlers: Vec::new(),
            errors: Vec::new(),
        }
    }

    /// The name.
    pub fn name(&self) -> &str {
        &self.name
    }
    /// The version string the application set.
    pub fn version(&self) -> &str {
        &self.version
    }
    /// The source as declared.
    pub fn source(&self) -> &Source {
        &self.source
    }
    /// The error policy.
    pub fn on_error(&self) -> OnError {
        self.on_error
    }
    /// The payload codec.
    pub fn codec(&self) -> Codec {
        self.codec
    }
    /// The stream policy.
    pub fn stream_policy(&self) -> StreamPolicy {
        self.stream_policy
    }

    /// The event types (tags) the typed handlers declared, sorted.
    pub fn event_types(&self) -> Vec<Bytes> {
        let set: BTreeSet<Bytes> = self
            .handlers
            .iter()
            .filter_map(|h| h.event_type.clone())
            .collect();
        set.into_iter().collect()
    }

    /// The source the runtime subscribes to.
    pub fn resolved_source(&self) -> chron_client::Source {
        match &self.source {
            Source::All => chron_client::Source::All,
            Source::Tag(t) => chron_client::Source::Tag(t.clone()),
            Source::Stream(s) => chron_client::Source::Stream(s.clone()),
            Source::Auto => {
                let types = self.event_types();
                if types.len() == 1 {
                    chron_client::Source::Tag(types[0].clone())
                } else {
                    chron_client::Source::Stream(self.auto_stream_name())
                }
            }
        }
    }

    /// The name of the chron stream the runtime defines for an `Auto`
    /// source over several event types.
    pub fn auto_stream_name(&self) -> String {
        format!("facet-{}", self.name)
    }

    /// The declaration handed to the engine: the collections, the dead
    /// letter table, and a version that also covers the source, the
    /// event types and the codec, so a change in any of them rebuilds.
    pub fn decl(&self) -> ProjectionDecl {
        let mut collections = self.collections.clone();
        collections.push(deadletter::decl());
        ProjectionDecl {
            name: self.name.clone(),
            version: self.effective_version(),
            collections,
        }
    }

    fn effective_version(&self) -> String {
        let mut h = crc32fast::Hasher::new();
        let src = match &self.source {
            Source::All => "all".to_string(),
            Source::Tag(t) => format!("tag:{}", crate::value::base64_encode(t)),
            Source::Stream(s) => format!("stream:{s}"),
            Source::Auto => "auto".to_string(),
        };
        h.update(src.as_bytes());
        h.update(b"|");
        for t in self.event_types() {
            h.update(&(t.len() as u32).to_be_bytes());
            h.update(&t);
        }
        h.update(b"|");
        h.update(format!("{:?}", self.codec).as_bytes());
        format!("{}+{:08x}", self.version, h.finalize())
    }

    pub(crate) fn handlers_for<'a>(
        &'a self,
        ev: &'a Event,
    ) -> impl Iterator<Item = &'a Handler> + 'a {
        self.handlers.iter().filter(move |h| match &h.event_type {
            None => true,
            Some(t) => ev.has_tag(t),
        })
    }
}

/// Builds a [`Projection`].
pub struct ProjectionBuilder {
    name: String,
    version: String,
    source: Source,
    on_error: OnError,
    codec: Codec,
    stream_policy: StreamPolicy,
    decl: Decl,
    handlers: Vec<Handler>,
    errors: Vec<Error>,
}

impl ProjectionBuilder {
    /// A version string; change it to rebuild the projection from
    /// nothing. Default `"1"`.
    pub fn version(mut self, v: impl Into<String>) -> Self {
        self.version = v.into();
        self
    }

    /// The source. Default [`Source::Auto`].
    pub fn source(mut self, s: Source) -> Self {
        self.source = s;
        self
    }

    /// The error policy. Default [`OnError::Fail`].
    pub fn on_error(mut self, p: OnError) -> Self {
        self.on_error = p;
        self
    }

    /// The payload codec. Default [`Codec::Json`].
    pub fn codec(mut self, c: Codec) -> Self {
        self.codec = c;
        self
    }

    /// The stream policy for an `Auto` source. Default
    /// [`StreamPolicy::Refuse`].
    pub fn stream_policy(mut self, p: StreamPolicy) -> Self {
        self.stream_policy = p;
        self
    }

    /// Declares the collections; the closure returns the handles the
    /// handlers will use.
    pub fn collections<C>(mut self, f: impl FnOnce(&mut Decl) -> C) -> (Self, C) {
        let handles = f(&mut self.decl);
        (self, handles)
    }

    /// A handler for events carrying `event_type`, with the payload
    /// decoded as `T`.
    pub fn on<T: DeserializeOwned + 'static>(
        mut self,
        event_type: impl Into<Bytes>,
        f: impl Fn(&mut Cx<'_, '_>, T) -> std::result::Result<(), HandlerError> + Send + Sync + 'static,
    ) -> Self {
        let codec = self.codec;
        let event_type = event_type.into();
        self.push_handler(Some(event_type), move |cx, ev| {
            let payload: T = cx.decode_with(codec, ev)?;
            f(cx, payload)
        });
        self
    }

    /// A handler for events carrying `event_type`, given the raw event.
    pub fn on_raw(
        mut self,
        event_type: impl Into<Bytes>,
        f: impl Fn(&mut Cx<'_, '_>, &Event) -> std::result::Result<(), HandlerError>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.push_handler(Some(event_type.into()), f);
        self
    }

    /// A handler for every event of the source (which must then be
    /// explicit).
    pub fn on_any(
        mut self,
        f: impl Fn(&mut Cx<'_, '_>, &Event) -> std::result::Result<(), HandlerError>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.push_handler(None, f);
        self
    }

    fn push_handler(
        &mut self,
        event_type: Option<Bytes>,
        f: impl Fn(&mut Cx<'_, '_>, &Event) -> std::result::Result<(), HandlerError>
        + Send
        + Sync
        + 'static,
    ) {
        if let Some(t) = &event_type {
            if t.is_empty() || t.len() > 255 {
                self.errors.push(Error::Declaration(format!(
                    "projection {}: an event type must be 1 to 255 bytes",
                    self.name
                )));
            }
            if self
                .handlers
                .iter()
                .any(|h| h.event_type.as_ref() == Some(t))
            {
                self.errors.push(Error::Declaration(format!(
                    "projection {}: two handlers for event type {}",
                    self.name,
                    String::from_utf8_lossy(t)
                )));
            }
        }
        let wrapped: HandlerFn = Arc::new(move |cx, ev| {
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(cx, ev))) {
                Ok(r) => r,
                Err(p) => Err(HandlerError::Panic(panic_message(&p))),
            }
        });
        self.handlers.push(Handler {
            event_type,
            f: wrapped,
        });
    }

    /// Checks the projection and builds it.
    pub fn build(mut self) -> Result<Projection> {
        self.errors.extend(self.decl.take_errors());
        if let Some(e) = self.errors.into_iter().next() {
            return Err(e);
        }
        if self.handlers.is_empty() {
            return Err(Error::Declaration(format!(
                "projection {}: no handlers",
                self.name
            )));
        }
        let any = self.handlers.iter().any(|h| h.event_type.is_none());
        if any && self.source == Source::Auto {
            return Err(Error::Declaration(format!(
                "projection {}: on_any needs an explicit source",
                self.name
            )));
        }
        let p = Projection {
            name: self.name,
            version: self.version,
            source: self.source,
            on_error: self.on_error,
            codec: self.codec,
            stream_policy: self.stream_policy,
            collections: self.decl.into_collections(),
            handlers: self.handlers,
        };
        if p.event_types().is_empty() && p.source == Source::Auto {
            return Err(Error::Declaration(format!(
                "projection {}: no event types to derive a source from",
                p.name
            )));
        }
        p.decl().validate()?;
        Ok(p)
    }
}

pub(crate) fn panic_message(p: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = p.downcast_ref::<&str>() {
        (*s).to_owned()
    } else if let Some(s) = p.downcast_ref::<String>() {
        s.clone()
    } else {
        "panic".to_owned()
    }
}
