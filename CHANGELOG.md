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
