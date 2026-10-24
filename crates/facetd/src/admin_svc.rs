//! `facet.v1.Admin`: the runtime and its projections.

use std::sync::Arc;

use base64::Engine;
use facet::{DEAD_LETTERS, DeadLetter, KeyShape, Projection, Rebuilt};
use facet_proto::v1 as pb;
use facet_proto::v1::admin_server::Admin;
use tonic::{Request, Response, Status};

use crate::Ctx;
use crate::codec::{runtime_status, status};

/// The service.
pub struct AdminService {
    ctx: Arc<Ctx>,
}

impl AdminService {
    pub(crate) fn new(ctx: Arc<Ctx>) -> Self {
        AdminService { ctx }
    }
}

/// The wire state.
pub fn state_wire(s: facet::State) -> pb::State {
    match s {
        facet::State::Starting => pb::State::Starting,
        facet::State::CatchingUp => pb::State::CatchingUp,
        facet::State::Live => pb::State::Live,
        facet::State::Rebuilding => pb::State::Rebuilding,
        facet::State::Failed => pb::State::Failed,
        facet::State::Paused => pb::State::Paused,
        facet::State::Stopped => pb::State::Stopped,
    }
}

/// `all`, `tag:<text or base64>` or `stream:<name>`.
pub fn source_label(s: &chron_client::Source) -> String {
    match s {
        chron_client::Source::All => "all".into(),
        chron_client::Source::Tag(t) => format!("tag:{}", tag_text(t)),
        chron_client::Source::Stream(n) => format!("stream:{n}"),
    }
}

fn tag_text(t: &[u8]) -> String {
    match std::str::from_utf8(t) {
        Ok(s) => s.to_owned(),
        Err(_) => format!(
            "base64:{}",
            base64::engine::general_purpose::STANDARD.encode(t)
        ),
    }
}

fn rebuilt_label(r: Rebuilt) -> &'static str {
    match r {
        Rebuilt::No => "no",
        Rebuilt::Fresh => "fresh",
        Rebuilt::DeclChanged => "declaration_changed",
        Rebuilt::VersionChanged => "version_changed",
        Rebuilt::FormatChanged => "format_changed",
        Rebuilt::DatabaseChanged => "database_changed",
    }
}

/// A registered projection's status on the wire.
pub(crate) fn projection_wire(
    def: &Projection,
    s: &facet::Status,
    chron_head: u64,
) -> pb::ProjectionStatus {
    let decl = def.decl();
    let collections = decl
        .collections
        .iter()
        .map(|c| pb::CollectionInfo {
            name: c.name.clone(),
            kind: c.kind.name().into(),
            key_shape: serde_json::to_string(&KeyShape::for_decl(c)).unwrap_or_default(),
            decl: serde_json::to_string(&c.kind).unwrap_or_default(),
            indexes: match &c.kind {
                facet::CollectionKind::Table { indexes, .. } => {
                    indexes.iter().map(|i| i.name.clone()).collect()
                }
                _ => Vec::new(),
            },
        })
        .collect();
    let head = chron_head.max(s.checkpoint.next);
    pb::ProjectionStatus {
        name: def.name().into(),
        version: def.version().into(),
        state: state_wire(s.state) as i32,
        checkpoint: s.checkpoint.next,
        chron_head: head,
        lag: head.saturating_sub(s.checkpoint.next),
        error: s.error.clone().unwrap_or_default(),
        source: source_label(&def.resolved_source()),
        event_types: def.event_types().iter().map(|t| tag_text(t)).collect(),
        collections,
        dead_letters: s.dead_letters,
        batches: s.batches,
        events: s.events,
        last_batch_unix_nanos: s.last_batch_at.and_then(|t| {
            t.duration_since(std::time::UNIX_EPOCH)
                .ok()
                .map(|d| d.as_nanos() as i64)
        }),
        on_error: match def.on_error() {
            facet::OnError::Fail => "fail".into(),
            facet::OnError::Skip => "skip".into(),
        },
        rebuilt: rebuilt_label(s.rebuilt).into(),
        reconnects: s.reconnects,
    }
}

fn orphan_wire(info: &facet::ProjectionInfo) -> pb::ProjectionStatus {
    pb::ProjectionStatus {
        name: info.name.clone(),
        version: info.version.clone(),
        state: pb::State::Orphaned as i32,
        checkpoint: info.checkpoint.next,
        error: "in the store but not registered by the application; drop it or register it again"
            .into(),
        ..Default::default()
    }
}

impl AdminService {
    fn registered(&self, name: &str) -> Option<pb::ProjectionStatus> {
        let def = self.ctx.handle.definition(name)?;
        let s = self.ctx.handle.status(name)?;
        Some(projection_wire(&def, &s, self.ctx.handle.chron_head()))
    }

    async fn orphans(&self) -> Result<Vec<facet::ProjectionInfo>, Status> {
        let store = self.ctx.runtime.store().clone();
        let names = self.ctx.handle.names();
        tokio::task::spawn_blocking(move || store.list())
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map(|list| {
                list.into_iter()
                    .filter(|i| !names.contains(&i.name))
                    .collect()
            })
            .map_err(status)
    }

    async fn one(&self, name: &str) -> Result<pb::ProjectionStatus, Status> {
        if let Some(p) = self.registered(name) {
            return Ok(p);
        }
        self.orphans()
            .await?
            .into_iter()
            .find(|i| i.name == name)
            .map(|i| orphan_wire(&i))
            .ok_or_else(|| Status::not_found(format!("no projection {name:?}")))
    }

    fn need_registered(&self, name: &str) -> Result<(), Status> {
        if self.ctx.handle.definition(name).is_none() {
            return Err(Status::not_found(format!(
                "no projection {name:?} is registered"
            )));
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl Admin for AdminService {
    async fn status(
        &self,
        _: Request<pb::StatusRequest>,
    ) -> Result<Response<pb::StatusResponse>, Status> {
        let statuses = self.ctx.handle.statuses();
        let count = |st: facet::State| statuses.iter().filter(|s| s.state == st).count() as u32;
        Ok(Response::new(pb::StatusResponse {
            version: env!("CARGO_PKG_VERSION").into(),
            uptime_secs: self.ctx.started.elapsed().as_secs(),
            store_path: self.ctx.runtime.store().dir().display().to_string(),
            chron_db_id: self.ctx.handle.db_id().hyphenated().to_string(),
            chron_head: self.ctx.handle.chron_head(),
            fatal: self.ctx.runtime.fatal().unwrap_or_default(),
            starting: count(facet::State::Starting),
            catching_up: count(facet::State::CatchingUp),
            live: count(facet::State::Live),
            rebuilding: count(facet::State::Rebuilding),
            failed: count(facet::State::Failed),
            paused: count(facet::State::Paused),
            sources: self
                .ctx
                .handle
                .sources()
                .into_iter()
                .map(|s| pb::SourceStatus {
                    source: source_label(&s.source),
                    connected: s.connected,
                    reconnects: s.reconnects,
                    error: s.error.unwrap_or_default(),
                    buffered: s.buffered as u64,
                })
                .collect(),
        }))
    }

    async fn list_projections(
        &self,
        _: Request<pb::ListProjectionsRequest>,
    ) -> Result<Response<pb::ListProjectionsResponse>, Status> {
        let mut projections: Vec<pb::ProjectionStatus> = self
            .ctx
            .handle
            .names()
            .iter()
            .filter_map(|n| self.registered(n))
            .collect();
        projections.extend(self.orphans().await?.iter().map(orphan_wire));
        Ok(Response::new(pb::ListProjectionsResponse { projections }))
    }

    async fn get_projection(
        &self,
        req: Request<pb::ProjectionRequest>,
    ) -> Result<Response<pb::ProjectionStatus>, Status> {
        Ok(Response::new(self.one(&req.into_inner().name).await?))
    }

    async fn dead_letters(
        &self,
        req: Request<pb::DeadLettersRequest>,
    ) -> Result<Response<pb::DeadLettersResponse>, Status> {
        let r = req.into_inner();
        let data = self.ctx.handle.projection(&r.name).ok_or_else(|| {
            Status::not_found(format!("no projection {:?} is registered", r.name))
        })?;
        let limit = if r.limit == 0 {
            100
        } else {
            r.limit.min(self.ctx.opts.max_page)
        } as usize;
        let rows: Vec<DeadLetter> =
            tokio::task::spawn_blocking(move || -> Result<Vec<DeadLetter>, Status> {
                let snap = data.snapshot().map_err(status)?;
                let t = snap
                    .table::<u64, DeadLetter>(DEAD_LETTERS)
                    .map_err(status)?;
                let mut sc = t.scan().limit(limit);
                if r.after > 0 {
                    sc = sc.from(&(r.after + 1));
                }
                Ok(sc.run().map_err(status)?.items)
            })
            .await
            .map_err(|e| Status::internal(e.to_string()))??;
        let next = rows.last().map(|d| d.position).unwrap_or(0);
        Ok(Response::new(pb::DeadLettersResponse {
            dead_letters: rows
                .into_iter()
                .map(|d| pb::DeadLetter {
                    position: d.position,
                    aggregate: d.aggregate.map(|a| a.as_bytes().to_vec()),
                    index: d.index,
                    tags: d.tags.iter().map(|t| t.to_vec()).collect(),
                    error: d.error,
                    at_unix_nanos: d.at.0,
                })
                .collect(),
            next,
        }))
    }

    async fn rebuild(
        &self,
        req: Request<pb::ProjectionRequest>,
    ) -> Result<Response<pb::ProjectionStatus>, Status> {
        self.ctx.require_admin(req.metadata())?;
        let name = req.into_inner().name;
        self.need_registered(&name)?;
        self.ctx
            .runtime
            .rebuild(&name)
            .await
            .map_err(runtime_status)?;
        Ok(Response::new(self.one(&name).await?))
    }

    async fn pause(
        &self,
        req: Request<pb::ProjectionRequest>,
    ) -> Result<Response<pb::ProjectionStatus>, Status> {
        self.ctx.require_admin(req.metadata())?;
        let name = req.into_inner().name;
        self.need_registered(&name)?;
        self.ctx
            .runtime
            .pause(&name)
            .await
            .map_err(runtime_status)?;
        Ok(Response::new(self.one(&name).await?))
    }

    async fn resume(
        &self,
        req: Request<pb::ProjectionRequest>,
    ) -> Result<Response<pb::ProjectionStatus>, Status> {
        self.ctx.require_admin(req.metadata())?;
        let name = req.into_inner().name;
        self.need_registered(&name)?;
        self.ctx
            .runtime
            .resume(&name)
            .await
            .map_err(runtime_status)?;
        Ok(Response::new(self.one(&name).await?))
    }

    async fn drop_projection(
        &self,
        req: Request<pb::ProjectionRequest>,
    ) -> Result<Response<pb::DropResponse>, Status> {
        self.ctx.require_admin(req.metadata())?;
        let name = req.into_inner().name;
        if self.ctx.handle.definition(&name).is_some() {
            return Err(Status::failed_precondition(format!(
                "projection {name} is registered by the application; remove it there first"
            )));
        }
        let store = self.ctx.runtime.store().clone();
        let existed = tokio::task::spawn_blocking(move || store.drop_projection(&name))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(status)?;
        Ok(Response::new(pb::DropResponse { existed }))
    }
}
