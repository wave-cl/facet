# facet

A projection database over [chron](https://github.com/wave-cl/chron):
projections are Rust code that consume chron's event streams and keep
sets, maps, lists, tables, sorted sets and time buckets, served over gRPC
and HTTP.

Round 1 is under construction; see `CHANGELOG.md` for what exists.

| Crate (crates.io) | What |
|---|---|
| `facet-db` | the library: engine, SDK and runtime; its Rust name is `facet` (`use facet::Store`) |

The crates.io name `facet` belongs to another crate, so the library is
published as `facet-db` with its library target named `facet`.

## Development

```
cargo fmt --all --check
cargo clippy -p facet-db --no-default-features --all-targets -- -D warnings
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo nextest run --workspace --all-features --no-fail-fast
```
