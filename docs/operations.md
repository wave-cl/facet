# Operating facet

Runbooks for a facetd and its projections. `$FACET` is a
`http://host:4343`-style address; the admin token travels in
`FACET_ADMIN_TOKEN` (or `--admin-token`). `facet status` exits 0 when
every projection is live or catching up and every source connected, 1
when degraded (a failed, paused or orphaned projection, lag above
`--max-lag`, a source disconnected, a fatal error), 2 when unreachable.

## Reading the status

```
facet --addr $FACET status
facet --addr $FACET projections bank
```

A projection is `starting` (checking where to resume), `catching_up`
(applying what chron already holds), `live`, `rebuilding`, `failed`,
`paused`, or `orphaned` (in the store but no longer registered by the
application). `lag` is chron's confirmed head minus the checkpoint.
`reconnects` counts the times the source's subscription was opened
again; a source that stays disconnected shows its last error.

## A projection is failed

Its error names the event and the handler's message, for example
`event 1234: unlucky`. The checkpoint stands before that event; reads
are refused with the error (`FAILED_PRECONDITION`, HTTP 409) so a
client does not act on stale data without knowing.

- If the handler was fixed (a new binary), restart: the projection
  resumes at its checkpoint and retries the event.
- If the event is bad and the projection may skip it, set
  `OnError::Skip` on the projection (a restart; the error policy does
  not rebuild) and look at `facet dead-letters bank` afterwards.
- `facet resume bank` retries the event with the same code; it fails
  the same way unless the data changed.
- `facet rebuild bank` drops the collections and replays from the
  start, applying everything again with the current code.

Other failures the error names: retention passed the checkpoint (see
below), the chron database changed (see below), an explicit stream the
projection follows was deleted (define it again, then `resume`), a
confirmed position was cut back (should not happen; `rebuild`).

## Dead letters

`facet dead-letters bank` lists the events a `Skip` projection skipped,
with the position, tags and error. They live in the projection's
`__dead` table and go away on a rebuild. Fix the handler and `facet
rebuild bank` to apply them.

## Retention passed a checkpoint

chron's `retain` moved its floor past a projection's checkpoint while
the projection was down or paused. The projection fails naming the
floor: what it missed is gone from chron. `facet rebuild bank` starts
again from the floor (so does a fresh projection); the collections then
reflect the events from the floor on.

## The chron database changed

The store is bound to the id of the chron database it was built from.
If `--chron` now points at another database (a fresh chron, a
different group), the daemon refuses to start with `WrongDatabase`; a
running one notices (`facet status` shows `fatal`) and fails its
projections. Either point it back, or start with `--rebind` to rebuild
every projection from the new database.

A log that was replaced (a restore of chron) or cut shorter under a
running projection is noticed by the checkpoint's fingerprint and
rebuilds that projection by itself.

## A projection the application no longer registers

Its file stays in the store and it is listed as `orphaned`. `facet
drop old` deletes the file (admin). Registering it again later starts
it from the file if the declaration still matches, else rebuilds it.

## Pausing

`facet pause bank` stops applying (the checkpoint stands; reads still
answer, with `paused` in `seen`); `facet resume bank` continues.

## Streams facet defines

A projection with an `Auto` source over several event types follows the
chron stream `facet-<projection>`, which the daemon defines with its
chron admin token at start and again if it is deleted. If a stream of
that name exists over other tags the projection fails and says so;
delete the stream on chron, or set `StreamPolicy::Recreate`.

## Tokens and TLS

`--token` gates every call; `--admin-token` additionally gates
rebuild, pause, resume and drop (the admin token also reads). Rotate by
restarting with the new value; `--token-file` variants read them from
files. Serve TLS with `--tls-cert`/`--tls-key`, require client
certificates with `--tls-client-ca`. Towards chron: `--chron-token`,
`--chron-admin-token`, `--chron-ca`, `--chron-cert`/`--chron-key`.

## Metrics

`--metrics-listen` serves Prometheus text with no token: bind it to a
private interface. Alert on `facet_projection_state{state="failed"} ==
1`, `facet_projection_lag_events` growing, `facet_source_connected ==
0` for longer than a reconnect, `facet_fatal == 1`.

## Upgrading

1. Stop the application binary, install the new one, start it. Files
   whose engine format changed are rebuilt (the status says
   `format_changed`); otherwise projections resume at their
   checkpoints.
2. A projection whose declaration or `version` changed is rebuilt from
   the start. Expect a catch-up; reads answer from the partial data
   with `rebuilding` in `seen` meanwhile.
3. chron must be 0.7 or later (streams, confirmed reads).
