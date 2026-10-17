# Proposed upstream safety fixes

These patches repair both Flotilla safety failures and the manifest-adoption deadlocks in isolated validation copies. They are not applied to this checkout's selected dependencies. Cargo manifests and lockfiles still select the committed upstream Git pins.

The dependency upgrade was committed as `9b20551b12bafe9bcbf10f696a7a6972b8e5998d`. This patch set follows the request to prepare upstream patches rather than vendor dependency overrides.

## Patch bases and application order

| Patch | Repository | Exact base |
| --- | --- | --- |
| [walrust retry and WAL ownership](walrust-retry-state.patch) | walrust | `1c0656e2ce896a38fdf064a27402f6a8deb1f5aa` |
| [Turbolite manifest locking](turbolite-manifest-lock.patch) | turbolite | `6a4f60741bbfd4243331bd837227fb264bdaa349` |
| [haqlite manifest adoption](haqlite-manifest-adoption.patch) | haqlite | `9b20551b12bafe9bcbf10f696a7a6972b8e5998d` |

Apply each patch at its repository root. `git apply --check /path/to/the.patch` verifies compatibility first. The two dependency patches are independent. The haqlite patch requires the new Turbolite API and must not be applied against the unchanged Git pin.

Before using these fixes in this checkout:

1. Land the dependency patches upstream.
2. Pin the resulting full upstream commit SHAs in haqlite.
3. Apply the haqlite integration patch.
4. Rerun locked builds and runtime tests against those actual Git revisions.

No upstream repository was edited, committed or pushed during this work. Source copies, temporary overrides and validation builds stayed under this worktree's ignored `target/upstream-fixes/` directory.

## Walrust fixes

### Roll back speculative reader state on failed publication

`read_next_wal_batch` advances the running WAL checksum before the fenced envelope is published. The old error path left that checksum advanced while keeping the old byte offset. A retry then read those bytes with the wrong checksum seed and could report no pending frames.

The patch adds a private rollback guard around fenced publication. It commits the reader cursor only after successful publication. Error returns and dropped futures restore the offset, generation, salt and checksum seed. The envelope CAS and checksum checks remain enabled.

The new native regression, `failed_fenced_publish_retries_pending_wal_frames`, first publishes a real SQLite WAL batch. It then fails two publication attempts, recovers storage and reconstructs a follower with every expected row.

Before the repair, the second failed-publication attempt incorrectly returned `None`. After the repair, it retries the unpublished frames and reconstructs the correct data.

### Inspect journal mode without opening SQLite

The no-WAL branch opened the base through SQLite's default VFS to query `PRAGMA journal_mode`. Closing that connection could unlink an empty WAL still owned by Turbolite. Later writes went into its open but unlinked file, while walrust looked for a pathname that no longer existed.

The patch reads the SQLite header without opening a database connection. It requires valid SQLite magic and WAL format versions in bytes 18 and 19. Rollback-mode databases are still rejected. The probe does not checkpoint, recover or remove companion files.

The native regression, `empty_external_wal_probe_preserves_the_live_file`, failed before this repair because the empty sync unlinked the live WAL. It passes afterward. The existing Flotilla three-node regression also passes: the third node reads the second leader's write.

## Turbolite and haqlite fixes

Turbolite adds `set_manifest_bytes_under_replay_write(bytes, guard)`. The caller must supply a write guard for that exact VFS's replay gate. A guard from another lock is rejected before mutation.

The ordinary setter retains its unchanged-base fast path, so a no-op publish does not upgrade a reader lock. Actual adoption acquires the write gate and rechecks the version floor. Both entry points share installation logic. Existing stale-version and pending-dirty-group protections remain enabled.

Haqlite switches only the 4 installation calls already inside its replay write gate. It preserves the existing materialization, replay finalization and cursor handling. Installation does not release or reacquire that gate.

The new native API test checks held-lock adoption, wrong-lock rejection and stale-version rejection. Existing haqlite tests verify read-only client opening and atomic follower adoption during missing-page-group and concurrent-reader races.

## Validation

Validation used macOS arm64 and Rust 1.99.0. The selected hadb revision remained `3142b3d6062a7aa5a8a1cb211ee38b647fe0f48b`.

The validation copies used temporary Cargo Git-source patch overrides for the two changed crates. Native walrust tests used the same hadb 0.5 Git patches as haqlite. These overrides were not added to the real workspace.

| Test or check in patched copies | Result |
| --- | --- |
| Native walrust-core library suite | 293 passed |
| Native Turbolite lock-aware installation test | passed |
| Native Turbolite existing manifest tests | 2 passed |
| Native Turbolite replay suite | 35 passed |
| Haqlite Flotilla regression target | 16 passed, one existing ignored stress test |
| Haqlite mode-role target | 6 passed |
| Haqlite single-writer failover target | 11 harness passes |
| Complete haqlite workspace runtime sweep, default features, serial | 331 harness passes, no failures, one existing ignored stress test |
| Copied workspace locked check, all targets and all features | passed |
| Copied workspace library Clippy, all features | passed with warnings |
| Patch application and byte-for-byte source checks | passed |

Four failover harness cases return early without `TIERED_TEST_BUCKET`. They are not live object-storage validation. The other 7 cases include atomic adoption, concurrent-reader consistency, promotion and continued writes. The full workspace sweep used default features, not the legacy S3 runtime feature set. Its aggregate also includes environment-gated tests that return early.

The original committed dependency selection still fails the Flotilla tests and deadlocks during adoption. The repair results above describe patched source copies only. Full native Turbolite coverage, live S3/NATS/HTTP deployments, the ignored stress test and the separate 512-byte-page import/replay problem are not established by these results. Issue #12 remains separate.

### Reproduce in source copies

Create disposable source copies at the exact bases above and apply the matching patches. Use an absolute-path Cargo override file only for the validation copy:

```toml
[patch."https://github.com/russellromney/walrust.git"]
walrust-core = { path = "/absolute/path/to/patched/walrust/crates/walrust-core" }

[patch."https://github.com/russellromney/turbolite.git"]
turbolite = { path = "/absolute/path/to/patched/turbolite" }
```

Resolve the copied lockfile once with those overrides, then use `--locked`. Do not put these overrides in the real workspace.

Run these checks in the patched haqlite copy, supplying its override file with `--config`:

```sh
cargo test --locked --workspace --no-fail-fast -- --test-threads=1
cargo check --locked --workspace --all-targets --all-features
cargo test --locked -p haqlite-turbolite --test flotilla_regression -- --test-threads=1
cargo test --locked -p haqlite-turbolite --test mode_role_bail -- --test-threads=1
cargo test --locked -p haqlite-turbolite --test singlewriter_failover -- --test-threads=1
```

For native walrust tests, supply a separate `native-config.toml` matching haqlite's hadb patches:

```toml
[patch.crates-io]
hadb-storage = { git = "https://github.com/russellromney/hadb.git", rev = "3142b3d6062a7aa5a8a1cb211ee38b647fe0f48b" }
hadb-io = { git = "https://github.com/russellromney/hadb.git", rev = "3142b3d6062a7aa5a8a1cb211ee38b647fe0f48b" }
hadb-changeset = { git = "https://github.com/russellromney/hadb.git", rev = "3142b3d6062a7aa5a8a1cb211ee38b647fe0f48b" }
```

Resolve its copied lockfile with that configuration first. Run the native suites in their patched source copies:

```sh
# walrust repository
cargo test --locked --config /absolute/path/to/native-config.toml -p walrust-core --lib

# Turbolite repository
cargo test --locked -p turbolite --lib manifest_adoption_requires_own_write_guard_and_keeps_version_floor
cargo test --locked -p turbolite --lib tiered::vfs::local_vfs_tests::set_manifest
cargo test --locked -p turbolite --lib tiered::replay::tests
```

Native builds on the validation machine also needed the libclang environment settings described in the [dependency upgrade report](../docs/dependency-upgrade.md).
