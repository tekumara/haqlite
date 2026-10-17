# Upstream dependency upgrade

This upgrade is not ready to merge. The base library builds and its test harness passes, but tiered replication still has safety failures and deadlocks. Do not treat a successful build as proof that tiered failover works.

The work is independent of Node bindings PR #13 and follower-cursor issue #12. Neither is implemented or declared fixed here.

The existing worktree branch is `chore/haqlite-upstream-dependency-upgrade`, with managed target `main`. Starting HEAD, local `main` and `origin/main` all matched `db6db54b555a56c7352a5c9d57f1b998da05d199` before editing. Implementation stayed in that worktree. No commits, pushes, PRs or issue/comment changes were made.

## Source revisions

These full revisions matched upstream `main` during validation on 17 October 2026:

| Repository | Package version | Git revision |
| --- | --- | --- |
| hadb | 0.5.0 | `3142b3d6062a7aa5a8a1cb211ee38b647fe0f48b` |
| walrust | `walrust-core` 0.7.0 | `1c0656e2ce896a38fdf064a27402f6a8deb1f5aa` |
| turbolite | 0.5.0 | `6a4f60741bbfd4243331bd837227fb264bdaa349` |
| hrana-server | 0.1.0 | `a3e554aedd6313175a367485bd2718a0e74ff1cc` |

Direct sibling dependencies share root workspace declarations. The Rust dependency name `walrust` now aliases package `walrust-core`. The standalone fuzz workspace uses the same Hrana revision as the root package.

There are no developer-local sibling patches or external target directories. Both lockfiles are updated. The remaining path dependencies point inside this repository.

### Older transitive storage crates remain

Turbolite main still requires crates.io `hadb-storage` 0.4.1 and `hadb-storage-local` 0.4.0. These are not upgraded by the Git pins.

`haqlite-turbolite` names its compatibility dependency `hadb-storage-04`. Its `TurboliteStorage` adapter forwards every operation to the current 0.5 backend, including CAS, pagination, range and batch operations. It preserves backend errors and identity rather than substituting trait defaults.

The root crates.io patches unify walrust's published 0.5 dependencies with the pinned hadb Git crates: `hadb-storage`, `hadb-io` and `hadb-changeset`. They cannot replace Turbolite's incompatible 0.4 trait.

Turbolite's optional legacy `wal` feature is disabled. Haqlite still ships WAL through its own direct, pinned walrust integration. Turbolite's `zstd` feature remains enabled.

Inspect the resolved boundary with:

```sh
cargo tree --locked --workspace --all-features -i hadb-storage@0.4.1
cargo tree --locked --workspace --all-features -i hadb-storage@0.5.0
cargo metadata --locked --format-version 1
cargo metadata --locked --manifest-path fuzz/Cargo.toml --format-version 1
```

## API adaptations and safety boundaries

The port makes these changes:

- retains walrust's full `PullCursor` between follower polls and CLI replication iterations
- resolves owned streams through the active lineage in `state.json`
- reconstructs an incremental cursor from its exact changeset, not the next object's claimed predecessor
- reconstructs a snapshot cursor from that remote snapshot's full image hash, which differs from its HADBP payload checksum
- rejects missing cursor evidence, sequence gaps, checksum breaks and stale leader epochs
- uses opaque native restore results and lease-guarded `resume_owned_after_restore` before registering restored leaders
- checks the active lease fence before replacing restored local files and throughout resumed publication
- propagates failures from native `remove` instead of ignoring them
- distinguishes a published manifest base from an already-applied delta when preparing tiered replay
- handles flagged empty-page end-page-count markers as truncation metadata, not SQLite pages
- checks marker and TLM_DELTA envelope page counts agree
- materializes the exact Turbolite manifest to implement the current snapshot-source checksum contract
- uses an explicit manifest replay sequence when its cursor epoch is populated, not SQLite's unrelated change counter
- uses SQLite online backup and conditional upload for one-shot CLI snapshots, including DELETE-mode databases

The legacy `with_allow_fork_bridge` setting never disables native walrust sequence or checksum verification.

The new regression tests use real SQLite, native codecs and temporary storage. They cover initial snapshots, restored-leader shipping, lease-loss rejection, owned-lineage follower replay, fork rejection, snapshot cursor checksums, storage CAS and pagination, and page-marker truncation with SQLite integrity checking. Existing epoch, cursor and fork tests remain enabled.

## Unresolved tiered blockers

### Manifest adoption deadlocks under the replay gate

The pinned Turbolite `TurboliteVfs::set_manifest` now acquires `replay_gate.write()` internally. Haqlite already holds this write gate across materialization, manifest installation and replay finalization. Calling `set_manifest_bytes` from that critical section deadlocks.

A native process stack confirmed the wait in `set_manifest` during `TurboliteWalReplicator::restore_from_manifest`. The affected paths include follower base adoption and single-writer client opening.

Turbolite exposes `finalize_assuming_external_write`, but no corresponding manifest-installation API for callers holding the gate. A safe upstream API is needed to retain atomic adoption. Releasing the outer lock between materialization and installation would expose torn snapshots, so this port does not do that.

These final serial targets were terminated after 45 seconds:

- `mode_role_bail`, blocked at `singlewriter_client_opens_as_read_only_consumer`
- `singlewriter_failover`, blocked at `follower_apply_is_atomic_under_missing_page_group_race`

Their remaining tests were not reached. Earlier targeted runs also hung. Increasing failover fixture TTLs above hadb 0.5's two-second clock-skew margin did not resolve this lock conflict.

### Failed final sync can be acknowledged by a retry

`flotilla_regression::continuous_close_can_retry_after_failed_final_sync` fails because `close()` returns success while the injected WAL storage failure is still active.

The pinned native `sync_wal_fenced_delta` reads and advances WAL checksum-chain bookkeeping before publishing. Its publication-error path does not restore all that bookkeeping. This is a dependency-boundary retry concern; the native high-level replicator keeps that state private. The test remains enabled and failing. No success result should be taken as a durability guarantee for this case.

### A reopened tiered leader's write is missing

`flotilla_regression::continuous_leader_publishes_replayed_base_before_new_writes` fails because the third node cannot find the second leader's row. This remains unresolved. The failure is not attributed solely to an upstream bug without a separate isolated proof.

The final serial Flotilla run reports 14 passed, 2 failed and one existing ignored stress test. Earlier parallel runs also tripped latency and file-descriptor assertions; those assertions passed in the final serial run.

An exploratory 512-byte-page version of the new import-and-truncate fixture produced a malformed SQLite cache image. The retained integrity regression uses the default 4096-byte pages. Smaller-page import/replay remains unresolved; the passing regression does not establish its safety.

## Issue 12 remains separate

The generic hadb follower interface still exposes only a sequence position. Starting it at zero after a nonzero restore remains an invalid handshake.

The new follower regression confirms that zero fails closed. Supplying the actual restored sequence permits native checked replay, and a forged tail is rejected without applying its pages. This is a boundary regression, not a fix for the coordinator's post-restore cursor initialization.

## Validation results

Final validation on macOS arm64 used Rust 1.99.0:

| Check | Result |
| --- | --- |
| Workspace locked check, all targets and all features | passed |
| Root package locked tests, all features, including doctests | 225 harness passes, no failures |
| Tiered library tests | 43 passed |
| Standalone fuzz locked check, all targets | passed; fuzzing itself was not run |
| Workspace library Clippy, all features | passed with warnings; not warning-free |
| Formatting and whitespace checks | passed |
| Full workspace runtime tests | not green |

The root harness count includes an environment-gated NATS test that returns early without `NATS_URL`. It is not evidence of live NATS coverage.

The tiered runtime sweep built all-feature test executables, ran targets serially, and enforced the stated 45-second limit per target. Completed local targets included `builder_explicit`, `continuous_bootstrap`, `cursor_handshake`, `property_execution`, `static_call_list`, `test_manifest_store` and `turbolite_manifest_round_trip`.

External-service validation did not run successfully:

- 35 legacy S3 cases failed on missing `TIERED_TEST_BUCKET`, across `e2e_modes`, `lease_split_brain`, `mode_f_s3primary`, `mode_i_production`, `mode_matrix`, `shared_mode` and `turbolite_split_brain`
- some bootstrap and object-store cursor tests return early when that variable is absent; harness success is not service-backed validation
- live S3-compatible storage, NATS and HTTP service deployments were not provisioned or validated
- the existing ignored file-descriptor stress test, cross-platform builds and fuzz execution were not run

## Reproduce the checks

Use a recent Rust toolchain. The current lockfiles include AWS dependencies requiring Rust 1.91.1; older haqlite toolchain assumptions are no longer sufficient. Native bindgen also needs libclang.

On the validation machine, the native build needed these environment settings. They are not committed as machine-specific Cargo configuration:

```sh
export LIBCLANG_PATH=/Library/Developer/CommandLineTools/usr/lib
export DYLD_FALLBACK_LIBRARY_PATH=/Library/Developer/CommandLineTools/usr/lib
```

Run the portable checks from the repository root:

```sh
cargo check --locked --workspace --all-targets --all-features
cargo test --locked -p haqlite --all-features --no-fail-fast
cargo test --locked -p haqlite-turbolite --all-features --lib
cargo check --locked --manifest-path fuzz/Cargo.toml --all-targets
cargo clippy --locked --workspace --all-features --lib
cargo fmt --all --check
git diff --check
```

Reproduce the local tiered blockers separately. The last two commands need an external timeout or manual termination because they deadlock:

```sh
cargo test --locked -p haqlite-turbolite --test flotilla_regression -- --test-threads=1
cargo test --locked -p haqlite-turbolite --test mode_role_bail singlewriter_client_opens_as_read_only_consumer -- --nocapture
cargo test --locked -p haqlite-turbolite --test singlewriter_failover follower_apply_is_atomic_under_missing_page_group_race -- --nocapture
```

The unbounded full command is `cargo test --locked --workspace --all-features --no-fail-fast`. It is expected to fail or hang for the reasons above; it is not a passing acceptance check.
