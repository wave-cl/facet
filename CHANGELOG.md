# Changelog

All notable changes to facet. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the workspace's
crates share one version.

## Unreleased

### facet-db

- The engine: one redb file per projection under a store directory bound
  to a chron database id; order-preserving, prefix-free key encoding
  (bool, int, uint, decimal, string, bytes, uuid, timestamp, tuples); a
  typed `Value` with a compact binary encoding and JSON rendering;
  declarations with a rebuild rule (a changed declaration, version or
  format drops the projection's collections and zeroes its checkpoint);
  `Projection::apply` runs a batch inside one write transaction and
  commits it with the checkpoint, so a crash never leaves rows ahead of
  or behind the checkpoint; snapshots for reads; batch summaries and a
  checkpoint watch for live queries; limits.
- The collections: `register`, `counter` (keyed, `i64` or decimal),
  `set`, `map`, `list` (per partition, sequence-numbered, push at either
  end, remove, truncate, clear), `table` (typed columns, a primary key,
  secondary indexes with nullable columns, unique indexes), `sorted set`
  (per partition, scores `i64`, `u64`, decimal or timestamp; by score,
  top, rank), `buckets` (count, sum, min and max per key and window for
  every declared width). Every read handle scans with a prefix, bounds,
  a direction, a limit and a cursor to page from; typed handles are
  checked against the declaration.
- The SDK (feature `runtime`, on by default): `Projection::new(name)
  .version(..).source(..).on_error(..).collections(|c| ..).on::<T>(tag,
  handler)`, with typed collection handles handed out at declaration
  and a `Cx` giving handlers the event and write access inside the
  batch's transaction; `App` gathers projections.
- The runtime: `Runtime::start(app, options)` connects to chron
  (`connect_any` over the seeds), opens the store bound to the chron
  database id (refusing another one unless `rebind`), registers every
  projection and runs one task each: a resume check (the database id,
  the floor, the confirmed head and a crc32 fingerprint of the last
  applied event, so a replaced or shortened log rebuilds the
  projection), a confirmed subscription from the checkpoint, batches of
  up to 256 events or 4 MiB applied inside one transaction with the
  checkpoint, reconnection with backoff and rediscovery, `pause`,
  `resume` and `rebuild`. `OnError::Skip` reruns a failed batch with
  the failing event recorded in the `__dead` table; `OnError::Fail`
  stops the projection before it. A `Handle` reads snapshots, statuses,
  waits for a position (`wait_for`) and follows batch summaries.
- One subscription per source, shared by every projection on it: a tail
  fills a ring of recent events; a projection whose checkpoint the ring
  no longer reaches back to reads its pages from chron until it catches
  up. On a stream error the tail backs off, looks for the leader again
  and resubscribes from the lowest checkpoint among its projections; a
  cut-back past a projection's checkpoint, a vanished explicit stream,
  or a changed chron database fails the projections it concerns. An
  `Auto` source over several event types is the chron stream
  `facet-<projection>`, which the runtime defines at start (a stream of
  that name with other tags is refused, or recreated under
  `StreamPolicy::Recreate`) and defines again when it is deleted.
  Retention that passed a projection's checkpoint fails it; a rebuild,
  or a projection with nothing applied yet, starts at the floor.
  `Handle::sources` reports every tail.
- Dynamic reads for services: `Snapshot::collection(name)` gives a
  `DynR` over any collection with `KeyTuple` keys and `Value` values
  (`get`, `scan`, `lookup` by index, `by_score`, `buckets`), a
  `KeyShape` describing a collection's or an index's key, and cursors
  checked against the scan they came from.

### facet-proto

- The `facet.v1` contract: `Query` (`Get`, `Scan`, `Lookup`, `Range`,
  `Buckets`, `Watch`) and `Admin` (`Status`, `ListProjections`,
  `GetProjection`, `DeadLetters`, `Rebuild`, `Pause`, `Resume`,
  `DropProjection`), keys and values as JSON text, `ReadAt` for read
  your writes, `Seen` on every read; the generated code is committed.

### facetd

- The daemon as a library an application binary embeds:
  `facetd::serve(app, options)`, `facetd::run(app, cli)` with
  `facetd::Cli` to flatten into the binary's flags (`--data-dir`,
  `--chron`, `--chron-db`, tokens and token files, chron TLS,
  `--listen`, `--metrics-listen`, server TLS and mTLS, `--token`,
  `--admin-token`, `--no-fsync`, `--rebind`, batch sizes, `--log-format`).
  Reads refuse a failed projection with its error, wait for `at_least`
  (504-style `UNAVAILABLE` with the checkpoint when it is not reached),
  page with opaque cursors and default to 100 entries; `Watch` streams
  each batch's changed keys, filtered by key or prefix, and ends with
  `OUT_OF_RANGE` when the watcher fell behind. Orphaned projections
  (in the store, not registered) are listed and can be dropped. Health:
  `facet.v1.Query` while serving, `facet.v1.Query.live` while every
  projection is caught up. Prometheus metrics at `--metrics-listen`:
  per-projection checkpoint, lag, state, dead letters, batches, events
  and reconnects, per-source connection and buffer, chron's head, the
  fatal flag, calls by method and code, durations, open streams,
  watchers. `facetd::testing::Fixture` (feature `test-util`) starts a
  chrond and a facetd for tests.
- The HTTP/JSON gateway on `--http-listen` (same tokens and TLS as
  gRPC): `GET /v1/status`, `/v1/projections`, `/v1/projections/{p}`,
  `/v1/projections/{p}/dead-letters`, `POST .../rebuild`, `/pause`,
  `/resume`, `DELETE /v1/projections/{p}`, and per collection `get`,
  `scan`, `lookup`, `range`, `top`, `buckets` and an SSE `watch`; keys
  and cursors as query parameters (cursors base64); errors as
  `{"error", "code", ...}` with the status metadata, a read whose
  `at_least` was not reached answering 504 with the checkpoint;
  `/openapi.json` documents every route (a unit test checks the two
  agree); `facet_http_requests_total{route,status}`.

### facet-cli

- The `facet` binary: `get`, `scan`, `lookup`, `range`, `top`,
  `buckets`, `watch`, `projections [NAME]`, `dead-letters`, `rebuild`,
  `pause`, `resume`, `drop`, `status [--max-lag]` (exit 1 when a
  projection is failed, paused or orphaned, lags past the bound, a
  source is disconnected or the runtime is fatal), `health`; `--addr`,
  `--token`, `--admin-token`, TLS flags, `--json`; exit 1 for a refusal,
  2 for usage or an unreachable daemon.

### facet-client

- A typed async client: `get`, `scan`, `lookup`, `range`, `top`,
  `buckets`, `watch`, `status`, `projections`, `projection`,
  `dead_letters`, `rebuild`, `pause`, `resume`, `drop_projection`,
  `health`; keys and values as `serde_json::Value`; `ClientError`
  with `BoundTimeout` and `WatchLagged` carrying the positions.
