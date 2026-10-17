# Follower replay after restore

[Issue 12](https://github.com/russellromney/haqlite/issues/12) needs coordinated
changes in haqlite, hadb and walrust. The haqlite change cannot ship alone against
the original dependency revisions.

## Restore and replay contract

The coordinator must initialize the follower's public replay position from the
position actually restored. It must do this before starting replay or promotion
monitoring. A remote head, SQLite change counter or first available incremental
is not evidence of locally applied data.

The SQLite replicator retains a receipt for each local database:

- the last applied sequence
- the source generation: snapshot, incremental or external base
- the checksum expected by the next changeset
- the highest accepted leader epoch

Snapshot and external-base anchors use the database-image checksum.
Incremental anchors use the HADBP chain checksum. A snapshot's HADBP envelope
checksum is not its database-image checksum.

Live replay checks contiguous sequences, key/header agreement and checksum
lineage before applying the downloaded bytes. It includes new snapshots because
snapshots also consume sequence numbers. Deleting or overwriting an old remote
anchor does not erase the locally retained receipt.

Restore, repeated incremental pulls and promotion catch-up use the same owner.
The existing explicit fork-acknowledgement option remains opt-in. Normal replay
still rejects gaps, forks and epochs below the accepted high-water mark.

A follower may join before the first snapshot exists. It can start replay once
it actually restores that snapshot. Incrementals without a restorable base are
an error, not permission to infer a cursor.

## Dependency coordination and migration

The changes were developed from these revisions:

- haqlite: `db6db54b555a56c7352a5c9d57f1b998da05d199`
- hadb: `c5773c1970463b2be4bf0d8187e544667faf1069`
- walrust: `e556bd5b7679851baa0efd6ea80838405f08e7a5`

hadb adds `Replicator::pull_with_position` and `Replicator::pull_incremental`.
Position-aware implementations must override the former and retain their own
engine-specific verification state. Unsupported incremental replay fails closed.
The coordinator uses the restored position for followers and clients.

`FollowerBehavior::catchup_on_promotion` now receives the replicator, so the
SQLite implementation can use that same state during promotion. Other follower
implementations need to accept the added argument.

walrust adds `ReplayCursor`, `ReplayGeneration`, `ReplayPolicy`,
`restore_with_cursor` and `pull_incremental_with_cursor`. Its existing
sequence-only restore methods remain available.

`SqliteFollowerBehavior::new()` no longer accepts storage. Storage access,
verification and cursor ownership now belong to the replicator.

Merge and publish the dependency changes, then update the dependency references
before shipping haqlite. No dependency references or lockfile versions were
changed here; local validation uses explicit Cargo patch overrides.

## Native regression

`tests/follower_replay.rs` uses the public HaQLite builder, SQL reads and replay
position with real SQLite and the existing in-memory storage and lease fixtures.
It needs no S3 service, Node bindings or production test hooks.

The original baseline restored snapshot 3 and published incremental 4. The native
regression observed the follower remain at 0 and log `expected seq 1, found 4`.
It also checks repeated replay, a later snapshot generation, late initial
snapshots, promotion catch-up and rejection followed by recovery for invalid
sequences, lineage and epochs.

Run it with a Cargo config pointing to the coordinated dependency checkouts:

```sh
RUSTC_WRAPPER= cargo test -p haqlite --test follower_replay --config "$CONFIG"
```

This native proof does not replace live S3-compatible integration validation.
