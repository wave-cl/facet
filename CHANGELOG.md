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
