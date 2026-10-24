//! `facet.v1.Query`: reads of the projections.

use std::sync::Arc;
use std::time::Duration;

use facet::keyenc::KeyTuple;
use facet::{Cursor, DynScan, KeyShape, ProjectionData, Width};
use facet_proto::v1 as pb;
use facet_proto::v1::query_server::Query;
use facet_proto::{CHRON_HEAD_HEADER, DEFAULT_WAIT_MS, MAX_WAIT_MS};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::codec::{insert, runtime_status, status};
use crate::keys;
use crate::{Ctx, admin_svc};

/// The service.
pub struct QueryService {
    ctx: Arc<Ctx>,
}

impl QueryService {
    pub(crate) fn new(ctx: Arc<Ctx>) -> Self {
        QueryService { ctx }
    }

    /// Finds the projection, refuses a failed one, waits for `at_least`.
    async fn prepare(
        &self,
        projection: &str,
        at: Option<pb::ReadAt>,
    ) -> Result<(ProjectionData, facet::Status), Status> {
        let s = self
            .ctx
            .handle
            .status(projection)
            .ok_or_else(|| Status::not_found(format!("no projection {projection:?}")))?;
        if s.state == facet::State::Failed {
            return Err(Status::failed_precondition(format!(
                "projection {projection} is failed: {}",
                s.error.as_deref().unwrap_or("no error recorded")
            )));
        }
        if let Some(at) = at
            && let Some(p) = at.at_least
        {
            let wait = at.wait_ms.unwrap_or(DEFAULT_WAIT_MS).min(MAX_WAIT_MS);
            if let Err(e) = self
                .ctx
                .handle
                .wait_for(projection, p, Duration::from_millis(u64::from(wait)))
                .await
            {
                let mut st = runtime_status(e);
                insert(&mut st, CHRON_HEAD_HEADER, self.ctx.handle.chron_head());
                return Err(st);
            }
        }
        let data = self
            .ctx
            .handle
            .projection(projection)
            .ok_or_else(|| Status::not_found(format!("no projection {projection:?}")))?;
        Ok((data, s))
    }

    fn seen(&self, checkpoint: u64, s: &facet::Status) -> pb::Seen {
        pb::Seen {
            checkpoint,
            state: admin_svc::state_wire(s.state) as i32,
            chron_head: self.ctx.handle.chron_head().max(checkpoint),
        }
    }

    fn limit(&self, n: u32) -> usize {
        if n == 0 {
            self.ctx.opts.default_page as usize
        } else {
            n.min(self.ctx.opts.max_page) as usize
        }
    }
}

fn cursor(after: &[u8]) -> Option<Cursor> {
    (!after.is_empty()).then(|| Cursor::from_bytes(after.to_vec()))
}

fn entries(items: Vec<(KeyTuple, facet::Value)>) -> Vec<pb::Entry> {
    items
        .into_iter()
        .map(|(k, v)| pb::Entry {
            key: keys::key_json(&k),
            value: keys::value_json(&v),
        })
        .collect()
}

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, Status> + Send + 'static,
) -> Result<T, Status> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| Status::internal(format!("the read panicked: {e}")))?
}

#[tonic::async_trait]
impl Query for QueryService {
    async fn get(&self, req: Request<pb::GetRequest>) -> Result<Response<pb::GetResponse>, Status> {
        let r = req.into_inner();
        let (data, s) = self.prepare(&r.projection, r.at).await?;
        let (found, value, checkpoint) = blocking(move || {
            let snap = data.snapshot().map_err(status)?;
            let coll = snap.collection(&r.collection).map_err(status)?;
            let key = keys::parse(&coll.key_shape(), &r.key, true, "key")?;
            let v = coll.get(&key).map_err(status)?;
            Ok((
                v.is_some(),
                v.map(|v| keys::value_json(&v)).unwrap_or_default(),
                snap.checkpoint().next,
            ))
        })
        .await?;
        Ok(Response::new(pb::GetResponse {
            found,
            value,
            seen: Some(self.seen(checkpoint, &s)),
        }))
    }

    async fn scan(
        &self,
        req: Request<pb::ScanRequest>,
    ) -> Result<Response<pb::ScanResponse>, Status> {
        let r = req.into_inner();
        let (data, s) = self.prepare(&r.projection, r.at).await?;
        let limit = self.limit(r.limit);
        let (page, checkpoint) = blocking(move || {
            let snap = data.snapshot().map_err(status)?;
            let coll = snap.collection(&r.collection).map_err(status)?;
            let shape = coll.key_shape();
            let q = DynScan {
                prefix: keys::parse_opt(&shape, &r.prefix, "prefix")?,
                from: keys::parse_opt(&shape, &r.from, "from")?,
                to: keys::parse_opt(&shape, &r.to, "to")?,
                after: cursor(&r.after),
                reverse: r.reverse,
                limit,
            };
            let page = coll.scan(&q).map_err(status)?;
            Ok((page, snap.checkpoint().next))
        })
        .await?;
        Ok(Response::new(pb::ScanResponse {
            entries: entries(page.items),
            next: page.next.map(|c| c.as_bytes().to_vec()).unwrap_or_default(),
            seen: Some(self.seen(checkpoint, &s)),
        }))
    }

    async fn lookup(
        &self,
        req: Request<pb::LookupRequest>,
    ) -> Result<Response<pb::ScanResponse>, Status> {
        let r = req.into_inner();
        let (data, s) = self.prepare(&r.projection, r.at).await?;
        let limit = self.limit(r.limit);
        let (page, checkpoint) = blocking(move || {
            let snap = data.snapshot().map_err(status)?;
            let coll = snap.collection(&r.table).map_err(status)?;
            let shape = coll.index_shape(&r.index).map_err(status)?;
            let q = DynScan {
                prefix: keys::parse_opt(&shape, &r.prefix, "prefix")?,
                from: keys::parse_opt(&shape, &r.from, "from")?,
                to: keys::parse_opt(&shape, &r.to, "to")?,
                after: cursor(&r.after),
                reverse: r.reverse,
                limit,
            };
            let page = coll.lookup(&r.index, &q).map_err(status)?;
            Ok((page, snap.checkpoint().next))
        })
        .await?;
        Ok(Response::new(pb::ScanResponse {
            entries: entries(page.items),
            next: page.next.map(|c| c.as_bytes().to_vec()).unwrap_or_default(),
            seen: Some(self.seen(checkpoint, &s)),
        }))
    }

    async fn range(
        &self,
        req: Request<pb::RangeRequest>,
    ) -> Result<Response<pb::ScanResponse>, Status> {
        let r = req.into_inner();
        let (data, s) = self.prepare(&r.projection, r.at).await?;
        let limit = self.limit(r.limit);
        let (page, checkpoint) = blocking(move || {
            let snap = data.snapshot().map_err(status)?;
            let coll = snap.collection(&r.collection).map_err(status)?;
            let facet::CollectionKind::SortedSet {
                partition, score, ..
            } = &coll.decl().kind
            else {
                return Err(Status::invalid_argument(format!(
                    "{} is a {}, not a sorted set",
                    r.collection,
                    coll.kind()
                )));
            };
            let pshape = KeyShape {
                kinds: partition.0.clone(),
                nullable: vec![false; partition.0.len()],
                names: (0..partition.0.len()).map(|i| i.to_string()).collect(),
            };
            let p = keys::parse(&pshape, &r.partition, true, "partition")?;
            let kind = score.key_kind();
            let bound = |text: &str, what: &str| -> Result<Option<facet::KeyPart>, Status> {
                if text.trim().is_empty() {
                    return Ok(None);
                }
                let j: serde_json::Value = serde_json::from_str(text)
                    .map_err(|e| Status::invalid_argument(format!("{what}: not JSON: {e}")))?;
                keys::part(kind, false, &j, what).map(Some)
            };
            let min = bound(&r.min, "min")?;
            let max = bound(&r.max, "max")?;
            let page = coll
                .by_score(
                    &p,
                    min.as_ref(),
                    max.as_ref(),
                    cursor(&r.after).as_ref(),
                    r.reverse,
                    limit,
                )
                .map_err(status)?;
            Ok((page, snap.checkpoint().next))
        })
        .await?;
        Ok(Response::new(pb::ScanResponse {
            entries: entries(page.items),
            next: page.next.map(|c| c.as_bytes().to_vec()).unwrap_or_default(),
            seen: Some(self.seen(checkpoint, &s)),
        }))
    }

    async fn buckets(
        &self,
        req: Request<pb::BucketsRequest>,
    ) -> Result<Response<pb::ScanResponse>, Status> {
        let r = req.into_inner();
        let (data, s) = self.prepare(&r.projection, r.at).await?;
        let limit = self.limit(r.limit);
        let (page, checkpoint) = blocking(move || {
            let snap = data.snapshot().map_err(status)?;
            let coll = snap.collection(&r.collection).map_err(status)?;
            let facet::CollectionKind::Buckets { key, .. } = &coll.decl().kind else {
                return Err(Status::invalid_argument(format!(
                    "{} is a {}, not buckets",
                    r.collection,
                    coll.kind()
                )));
            };
            let kshape = KeyShape {
                kinds: key.0.clone(),
                nullable: vec![false; key.0.len()],
                names: (0..key.0.len()).map(|i| i.to_string()).collect(),
            };
            let k = keys::parse(&kshape, &r.key, true, "key")?;
            let width = Width::parse(&r.width).map_err(status)?;
            let page = coll
                .buckets(
                    &k,
                    width,
                    r.from,
                    r.to,
                    cursor(&r.after).as_ref(),
                    r.reverse,
                    limit,
                )
                .map_err(status)?;
            Ok((page, snap.checkpoint().next))
        })
        .await?;
        let entries = page
            .items
            .into_iter()
            .map(|v| {
                let start = match &v {
                    facet::Value::Map(m) => m.get("start").cloned().unwrap_or(facet::Value::Null),
                    _ => facet::Value::Null,
                };
                pb::Entry {
                    key: serde_json::Value::Array(vec![start.to_json()]).to_string(),
                    value: keys::value_json(&v),
                }
            })
            .collect();
        Ok(Response::new(pb::ScanResponse {
            entries,
            next: page.next.map(|c| c.as_bytes().to_vec()).unwrap_or_default(),
            seen: Some(self.seen(checkpoint, &s)),
        }))
    }

    type WatchStream = ReceiverStream<Result<pb::Change, Status>>;

    async fn watch(
        &self,
        req: Request<pb::WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let r = req.into_inner();
        let (data, _) = self.prepare(&r.projection, None).await?;
        let decl = data
            .decl()
            .collection(&r.collection)
            .ok_or_else(|| Status::not_found(format!("no collection {:?}", r.collection)))?
            .clone();
        let shape = KeyShape::for_decl(&decl);
        let key = keys::parse_opt(&shape, &r.key, "key")?
            .map(|k| shape.encode(&k))
            .transpose()
            .map_err(status)?;
        let prefix = keys::parse_opt(&shape, &r.prefix, "prefix")?
            .map(|k| shape.encode(&k))
            .transpose()
            .map_err(status)?;
        let mut rx = data.subscribe();
        let (tx, out) = tokio::sync::mpsc::channel(64);
        let collection = r.collection.clone();
        let cancel = self.ctx.cancel.clone();
        self.ctx.metrics.watchers.inc();
        let metrics = self.ctx.metrics.clone();
        tokio::spawn(async move {
            struct Guard(Arc<crate::Metrics>);
            impl Drop for Guard {
                fn drop(&mut self) {
                    self.0.watchers.dec();
                }
            }
            let _g = Guard(metrics);
            loop {
                let summary = tokio::select! {
                    _ = cancel.cancelled() => {
                        let _ = tx.send(Err(Status::unavailable("the daemon is stopping"))).await;
                        return;
                    }
                    _ = tx.closed() => return,
                    s = rx.recv() => s,
                };
                let summary = match summary {
                    Ok(s) => s,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        let mut st =
                            Status::out_of_range("the watch fell behind; re-read and watch again");
                        insert(
                            &mut st,
                            facet_proto::POSITION_HEADER,
                            data.checkpoint().next,
                        );
                        let _ = tx.send(Err(st)).await;
                        return;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                };
                let Some(changed) = summary.collection(&collection) else {
                    continue;
                };
                let change = match changed {
                    facet::Changed::All => pb::Change {
                        checkpoint: summary.checkpoint.next,
                        collection: collection.clone(),
                        keys: Vec::new(),
                        all: true,
                    },
                    facet::Changed::Keys(ks) => {
                        let keys: Vec<String> = ks
                            .iter()
                            .filter(|k| match (&key, &prefix) {
                                (Some(key), _) => k.as_ref() == key.as_slice(),
                                (None, Some(p)) => k.starts_with(p),
                                (None, None) => true,
                            })
                            .filter_map(|k| shape.decode(k).ok())
                            .map(|k| keys::key_json(&k))
                            .collect();
                        if keys.is_empty() {
                            continue;
                        }
                        pb::Change {
                            checkpoint: summary.checkpoint.next,
                            collection: collection.clone(),
                            keys,
                            all: false,
                        }
                    }
                };
                if tx.send(Ok(change)).await.is_err() {
                    return;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(out)))
    }
}
