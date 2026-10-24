# facet

A projection database over [chron](https://github.com/wave-cl/chron).
Projections are Rust code: they consume chron's event streams and keep
registers, counters, sets, maps, lists, tables with secondary indexes,
sorted sets and time buckets, which the daemon serves over gRPC and
HTTP. Every projection's checkpoint commits in the same transaction as
its writes, so a crash, a restart or a chron failover never applies an
event twice or loses one.

| Crate (crates.io) | What |
|---|---|
| `facet-db` | the library: the engine, the projection SDK and the runtime; its Rust name is `facet` (`use facet::Projection`) |
| `facet-proto` | the `facet.v1` gRPC contract and generated code |
| `facetd` | the daemon as a library an application binary embeds: gRPC, HTTP gateway, tokens, TLS, metrics |
| `facet-client` | a typed async client |
| `facet-cli` | the `facet` binary: reads and operations against a daemon |
| `examples/bank-views` | projections over chron's bank example (not published) |

## Install

```
cargo add facet-db                 # the library; code says `use facet::...`
cargo add facetd                   # to serve your projections
cargo add facet-client             # the client; no protoc needed
cargo install facet-cli            # the `facet` binary
```

The crates.io name `facet` belongs to another crate, so the library is
published as `facet-db` with its library target named `facet`. A
project that also depends on that other `facet` renames one of them,
for example `facet = { package = "facet-db", version = "0.1" }`.
Prebuilt `facet` binaries for Linux (x86_64 and aarch64, static musl)
and macOS (Apple silicon) are attached to each
[GitHub Release](https://github.com/wave-cl/facet/releases) with a
`SHA256SUMS` file. There is no `facetd` binary: a daemon without
projections has nothing to serve, so the application is the binary
(see [The daemon](#the-daemon)).

## The SDK

A projection names its source, declares its collections and registers
handlers over chron events. The event type is a tag; a handler's
payload is decoded with serde (JSON).

```rust
use facet::{App, HandlerError, Projection, Source, TypeSchema, Width};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum AccountEvent { Opened, Deposited { amount: i64 }, Withdrawn { amount: i64 } }

#[derive(Serialize, Deserialize)]
struct Txn { id: u64, account: Uuid, amount: i64, at: i64 }

let (b, (balance, top, deposits, txns)) = Projection::new("bank")
    .version("1")
    .source(Source::tag("account"))
    .collections(|c| (
        c.map::<Uuid, i64>("balance"),
        c.sorted_set::<(), Uuid, i64>("top"),
        c.buckets::<(), i64>("deposits", [Width::Hour, Width::Day]),
        c.table::<u64, Txn>("transactions")
            .column("id", TypeSchema::Uint)
            .column("account", TypeSchema::Uuid)
            .column("amount", TypeSchema::Int)
            .column("at", TypeSchema::Timestamp)
            .primary_key(["id"])
            .index("by_account", ["account", "at"])
            .build(),
    ));
let bank = b
    .on::<AccountEvent>("account", move |cx, ev| {
        let id = cx.aggregate().ok_or("an account event without an aggregate")?;
        let delta = match ev {
            AccountEvent::Opened => return Ok(()),
            AccountEvent::Deposited { amount } => amount,
            AccountEvent::Withdrawn { amount } => -amount,
        };
        let now = cx.map(&balance)?.update(&id, |b| Some(b.unwrap_or(0) + delta))?.unwrap();
        cx.sorted_set(&top)?.put(&(), &id, now)?;
        if delta > 0 {
            cx.buckets(&deposits)?.add(&(), cx.recorded_at(), delta)?;
        }
        cx.table(&txns)?.upsert(&Txn { id: cx.position(), account: id, amount: delta, at: cx.recorded_at() })?;
        Ok::<(), HandlerError>(())
    })
    .build()?;
let app = App::new().projection(bank);
```

- **Collections** are declared once; the handles that come out of the
  closure are how handlers name them, so nothing is stringly typed.
  Keys are tuples of `bool`, `i64`, `u64`, `Decimal`, `String`,
  `Bytes`, `Uuid` and `Timestamp` (unix nanoseconds), encoded
  order-preserving and prefix-free, so a prefix of the leading parts
  scans. Values are anything serde can serialize; a table's columns are
  typed and validated on every write.
- **Sources**: `Source::tag(t)`, `Source::stream(name)` (a chron
  stream the operator defined), `Source::All`, or `Source::Auto` (the
  default): one declared event type is its tag; several are the chron
  stream `facet-<projection>` over those tags, which the runtime
  defines itself. A `StreamPolicy` says what to do when that stream
  already exists over other tags (refuse, or recreate it).
- **Handlers** run inside the batch's transaction: a read sees the
  batch's earlier writes. A handler that returns an error or panics
  fails the projection (`OnError::Fail`, the default: the checkpoint
  stays before the event and `resume` retries it) or skips the event
  into the projection's `__dead` table (`OnError::Skip`).
- **Rebuilds**: a changed declaration or `version` drops the
  projection's collections and replays from the start (or the floor);
  so do an operator's `rebuild`, a chron log that was replaced or cut
  shorter under the projection, and a store bound to another chron
  database when started with `rebind`.

## Guarantees

- A batch of events, the writes it makes and the checkpoint after it
  commit in one redb transaction. After a crash, rows and checkpoint
  agree; an event is applied once.
- Subscriptions and reads use chron's *confirmed* consistency: nothing a
  projection applied can be cut back by a chron failover. A checkpoint
  also carries a fingerprint of the last applied event; a log replaced
  underneath (a restore) rebuilds the projection.
- Reads come from one snapshot and report the checkpoint they saw; a
  read may ask to wait until a position was applied (read your writes,
  given a chron commit's `next`).

## Limits

| Limit | Value |
|---|---|
| encoded key | 1024 bytes |
| encoded value or row | 1 MiB |
| key parts | 16 |
| columns per table | 256 |
| indexes per table | 16 |
| elements per bulk operation | 10 000 |
| collection, column or index name | 64 bytes of `[A-Za-z0-9_]`; names starting with `__` are reserved |
| projection name | 48 bytes of `[a-z0-9_-]` |
| bucket widths per collection | 8 |

Lists keep holes where elements were removed (reads by sequence number
stay O(log n), a walk from the front is O(offset)); a sorted set's
`rank` walks the score index up to a cap. Floats are values, never keys
or scores.

## Layout on disk

```
<data_dir>/<projection>.redb      one redb file per projection
  meta       format, chron db_id, name, version, declaration, checkpoint
  c:<name>   one table per collection (tables add c:<name>:i:<index>,
             sorted sets c:<name>:s, lists c:<name>:m)
```

Deleting a projection's file (or `facet drop` for one the application
no longer registers) is the only maintenance; everything is rebuilt
from chron.

## The daemon

`facetd` is a library. An application binary embeds it:

```rust
#[derive(clap::Parser)]
struct Args {
    #[command(flatten)]
    daemon: facetd::Cli,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = <Args as clap::Parser>::parse();
    facetd::run(app(), args.daemon).await
}
```

```
my-views --data-dir ./facet --chron http://127.0.0.1:4242 \
  --listen 127.0.0.1:4343 --http-listen 127.0.0.1:4344 --metrics-listen 127.0.0.1:9101
```

`--chron` takes every address of a chron group, comma separated; the
leader is found among them and found again after a failover. Tokens:
`--token` (every call), `--admin-token` (rebuild, pause, resume, drop),
`--chron-token` / `--chron-admin-token` (what is sent to chron; the
admin one defines streams). TLS: `--tls-cert`/`--tls-key`
(`--tls-client-ca` for mTLS) on the daemon, `--chron-ca`/`--chron-cert`/
`--chron-key` towards chron. `--no-fsync` acknowledges batches without
a sync (a process crash is survived, a power loss replays from the last
durable batch); `--rebind` rebuilds files bound to another chron
database. Health: `facet.v1.Query` while serving, `facet.v1.Query.live`
only while every projection is live or caught up. Metrics (Prometheus
text at `--metrics-listen`): per projection `facet_projection_checkpoint`,
`_lag_events`, `_state`, `_dead_letters`, `_batches_total`,
`_events_total`, `_reconnects_total`; per source `facet_source_connected`,
`_buffered`, `_reconnects_total`; `facet_chron_head`, `facet_fatal`,
`facet_rpc_calls_total`, `facet_rpc_duration_seconds`,
`facet_streams_open`, `facet_watchers`, `facet_http_requests_total`.

### The HTTP gateway

The same calls as gRPC, for clients without it, on `--http-listen` with
the same tokens and TLS. Keys are JSON (an array of parts, or one bare
part), cursors base64, errors `{"error", "code"}` plus the checkpoint
or position the daemon knows. `GET /openapi.json` documents every route.

```
curl -s 'http://127.0.0.1:4344/v1/projections/bank/balance/get?key="<uuid>"'
curl -s 'http://127.0.0.1:4344/v1/projections/bank/top/top?limit=3'
curl -s 'http://127.0.0.1:4344/v1/projections/bank/transactions/lookup?index=by_account&prefix=["<uuid>"]'
curl -s 'http://127.0.0.1:4344/v1/projections/bank/deposits/buckets?width=hour'
curl -sN 'http://127.0.0.1:4344/v1/projections/bank/balance/watch?key="<uuid>"'   # SSE
curl -s -X POST http://127.0.0.1:4344/v1/projections/bank/rebuild
```

## The client crate

```rust
let c = facet_client::Client::connect("http://127.0.0.1:4343", Default::default()).await?;
let (balance, seen) = c.get("bank", "balance", Some(&json!(id)), ReadAt::at_least(committed.next)).await?;
let top = c.top("bank", "top", None, 10).await?;
let mut changes = c.watch("bank", "balance", Some(&json!(id)), None).await?;
```

`get`, `scan`, `lookup`, `range`, `top`, `buckets`, `watch`; `status`,
`projections`, `projection`, `dead_letters`, `rebuild`, `pause`,
`resume`, `drop_projection`, `health`. A read whose `at_least` was not
reached in time is `ClientError::BoundTimeout`; a watch that fell
behind ends with `WatchLagged { position }`: re-read, then watch again.

## The CLI

```
facet get bank balance <uuid>                 facet scan bank balance --limit 100 --all
facet lookup bank transactions --index by_account --prefix <uuid>
facet top bank top --limit 10                 facet buckets bank deposits --width hour
facet watch bank balance --key <uuid>
facet projections [bank]                      facet dead-letters bank
facet rebuild bank | pause bank | resume bank | drop old
facet status [--max-lag 1000]                 facet health [--service facet.v1.Query.live]
```

`--addr` (`FACET_ADDR`, default `http://127.0.0.1:4343`), `--token`,
`--admin-token`, `--tls-ca`/`--tls-cert`/`--tls-key`/`--tls-domain`,
`--json` for one JSON document per line. Exit 0; 1 for a refusal or a
degraded `status`; 2 for a usage error or an unreachable daemon.

## Operations

See [`docs/operations.md`](docs/operations.md): what a failed
projection means and how to resume or rebuild it, dead letters,
retention that passed a checkpoint, a chron database that changed, a
projection the application no longer registers, tokens, metrics,
upgrading.

## Known limitations

- A handler reads only its own projection's collections; projections do
  not see each other (each rebuilds on its own).
- A rebuild runs in place: until it catches up, reads answer from the
  partial data and say `rebuilding` in `seen`. Pass `at_least` for a
  read that must be complete.
- One facetd follows one chron database; run one per database.
- A changed declaration rebuilds the whole projection, not just the
  collection that changed.
- Live queries report the keys a batch changed, not the values; the
  watcher reads them. A watcher that falls more than 64 batches behind
  is told to re-read.
- No cursor on chron's side: the projection's checkpoint is the only
  position, so chron's cursor lag metrics do not apply.

## Development

```
cargo fmt --all --check
cargo clippy -p facet-db --no-default-features --all-targets -- -D warnings
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo nextest run --workspace --all-features --no-fail-fast
cargo package --workspace --locked
FACET_PROTO_REGEN=1 cargo build -p facet-proto     # after editing the .proto (needs protoc)
```

CI runs the same on every push; the `proto` job checks the committed
generated code matches the `.proto`.

## Releasing

`scripts/release.sh X.Y.Z` bumps the workspace version, moves the
changelog's Unreleased section, runs the gate, commits and tags. Push
main, then the tag; the release workflow builds the binaries,
publishes the crates (with the repository secret `CARGO_REGISTRY_TOKEN`)
and creates the GitHub Release with the changelog section as notes.
