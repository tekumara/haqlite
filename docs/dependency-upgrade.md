# Sibling dependency upgrade

This checkout now selects the patched `tekumara` forks and applies the lock-aware manifest integration. The default-feature workspace passes its local runtime sweep, including the previously failing Flotilla and deadlocking adoption tests. Live service validation and smaller-page import/replay safety remain incomplete; this is not full acceptance.

The work is independent of Node bindings PR #13 and follower-cursor issue #12. Neither is implemented or declared fixed here.

The repairs are published in [walrust PR 47](https://github.com/russellromney/walrust/pull/47) and [Turbolite PR 47](https://github.com/russellromney/turbolite/pull/47). The selected dependencies now contain those repairs. No temporary Git-source overrides are needed for this checkout.

The existing worktree branch is `chore/haqlite-upstream-dependency-upgrade`, with managed target `main`. Starting HEAD, local `main` and `origin/main` all matched `db6db54b555a56c7352a5c9d57f1b998da05d199` before editing. The original upgrade was committed as `9b20551b12bafe9bcbf10f696a7a6972b8e5998d` and patch preparation as `40cb39b44e0c9de6772abed40ad7061ca1311a53`. The upstream PRs were prepared in separately requested app-managed worktrees. Haqlite's fork-pin update stays in this existing worktree.

## Source revisions

The fork branch heads were verified on 17 October 2026. Cargo pins full commits, not moving branches. Hadb and Hrana retain the original verified upstream revisions.

| Repository | Package version | Git revision |
| --- | --- | --- |
| russellromney/hadb | 0.5.0 | `3142b3d6062a7aa5a8a1cb211ee38b647fe0f48b` |
| tekumara/walrust | `walrust-core` 0.7.0 | `1d59c45d3c944b0d606989d07e3e013b175b0b3e` |
| tekumara/turbolite | 0.5.0 | `d2b48f32282584d5713eb20d8ac5206a3314fb46` |
| russellromney/hrana-server | 0.1.0 | `a3e554aedd6313175a367485bd2718a0e74ff1cc` |

Direct sibling dependencies share root workspace declarations. The Rust dependency name `walrust` now aliases package `walrust-core`. The standalone fuzz workspace uses the same Hrana revision as the root package.

There are no developer-local sibling patches or external target directories. Both lockfiles are updated. The remaining path dependencies point inside this repository.

### Older transitive storage crates remain

The pinned Turbolite fork still requires crates.io `hadb-storage` 0.4.1 and `hadb-storage-local` 0.4.0. These are not upgraded by the Git pins.

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
- passes the existing replay write guard to manifest installation, without releasing the atomic adoption gate

The legacy `with_allow_fork_bridge` setting never disables native walrust sequence or checksum verification.

The new regression tests use real SQLite, native codecs and temporary storage. They cover initial snapshots, restored-leader shipping, lease-loss rejection, owned-lineage follower replay, fork rejection, snapshot cursor checksums, storage CAS and pagination, and page-marker truncation with SQLite integrity checking. Existing epoch, cursor and fork tests remain enabled.

## Repaired local tiered failures

### Manifest adoption retains the held replay gate

The original Turbolite setter reacquired the non-reentrant replay write lock already held by haqlite. Both client opening and atomic follower adoption deadlocked.

The fork adds `set_manifest_bytes_under_replay_write(bytes, guard)`, which validates that the supplied guard belongs to this exact VFS. Haqlite now uses it at the 4 installation calls already inside its write gate. Materialization, replay finalization and cursor handling keep their existing protection. The ordinary setter retains its no-op fast path and checks the version floor under the gate before adoption.

With the selected Git commits, all 6 `mode_role_bail` cases complete. All 11 `singlewriter_failover` harness cases also complete, including atomic adoption and concurrent-reader consistency. Four object-storage cases return early without `TIERED_TEST_BUCKET`; they do not establish live S3 coverage.

### Failed final sync remains retryable

The original fenced publication path advanced speculative WAL-reader checksums without rolling them back after an upload failure. A retry could incorrectly report no pending frames and allow `close()` to succeed while storage was still failing.

The walrust fork restores the offset, generation, salt and checksum seed unless publication succeeds. The existing `continuous_close_can_retry_after_failed_final_sync` regression now passes without weakening its injected-failure assertion.

### Reopened leaders retain their live WAL

The original empty-WAL journal-mode probe opened and closed SQLite's default VFS. That connection could unlink an empty WAL still owned by Turbolite. Subsequent writes reached an open but unlinked file that walrust could no longer discover by pathname.

The walrust fork reads the SQLite header without opening a database connection. The existing `continuous_leader_publishes_replayed_base_before_new_writes` regression now passes: the third node reads the second leader's row. The full local Flotilla target reports 16 passed and one existing ignored stress test.

## Smaller-page import remains unresolved

An exploratory 512-byte-page version of the import-and-truncate fixture produced a malformed SQLite cache image. The retained integrity regression uses the default 4096-byte pages. The passing regression does not establish smaller-page import/replay safety.

## Issue 12 remains separate

The generic hadb follower interface still exposes only a sequence position. Starting it at zero after a nonzero restore remains an invalid handshake.

The new follower regression confirms that zero fails closed. Supplying the actual restored sequence permits native checked replay, and a forged tail is rejected without applying its pages. This is a boundary regression, not a fix for the coordinator's post-restore cursor initialization.

## Validation results

Validation of the selected fork Git commits on macOS arm64 used Rust 1.99.0. These checks ran in this checkout without temporary Cargo Git-source overrides. The existing hadb crates.io patches remain part of the normal manifest configuration.

| Check | Result |
| --- | --- |
| Workspace locked check, all targets and all features | passed |
| Full default-feature workspace runtime sweep, serial | 331 harness passes, no failures, one existing ignored stress test |
| Root package locked tests, all features, including doctests | 225 harness passes, no failures |
| Tiered library tests, all features | 43 passed |
| Standalone fuzz locked check, all targets | passed; fuzzing itself was not run |
| Workspace library Clippy, all features | passed with warnings; not warning-free |
| Formatting and whitespace checks | passed |
| Full all-feature workspace runtime acceptance with external services | unvalidated |

The root harness count includes an environment-gated NATS test that returns early without `NATS_URL`. It is not evidence of live NATS coverage.

The default-feature sweep now completes every target, including `flotilla_regression`, `mode_role_bail` and `singlewriter_failover`. No test assertions were changed for the fork-pin update. Locked metadata confirms both repaired crates come from the specified fork Git commits, not local path replacements. Lockfile changes only replace the intended Git source entries.

External-service validation remains incomplete:

- the original all-feature sweep had 35 legacy S3 failures from missing `TIERED_TEST_BUCKET`, across `e2e_modes`, `lease_split_brain`, `mode_f_s3primary`, `mode_i_production`, `mode_matrix`, `shared_mode` and `turbolite_split_brain`; those cases have not been rerun with service credentials
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
cargo test --locked --workspace --no-fail-fast -- --test-threads=1
cargo test --locked -p haqlite --all-features --no-fail-fast
cargo test --locked -p haqlite-turbolite --all-features --lib
cargo check --locked --manifest-path fuzz/Cargo.toml --all-targets
cargo clippy --locked --workspace --all-features --lib
cargo fmt --all --check
git diff --check
```

Reproduce the repaired local tiered targets separately. All 3 now complete with the selected forks:

```sh
cargo test --locked -p haqlite-turbolite --test flotilla_regression -- --test-threads=1
cargo test --locked -p haqlite-turbolite --test mode_role_bail singlewriter_client_opens_as_read_only_consumer -- --nocapture
cargo test --locked -p haqlite-turbolite --test singlewriter_failover follower_apply_is_atomic_under_missing_page_group_race -- --nocapture
```

Full all-feature runtime acceptance still needs external fixtures. `cargo test --locked --workspace --all-features --no-fail-fast` is not a passing acceptance check without those services. The passing default-feature sweep does not establish live service, smaller-page, ignored stress-test or cross-platform coverage.
