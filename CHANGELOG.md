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
  or behind the checkpoint; snapshots for reads; the `register` and `map`
  collections with prefix, range and paged scans.
