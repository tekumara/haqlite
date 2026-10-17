//! Bridge hadb's sequence-only follower position to walrust's checked cursor.
//!
//! An incremental cursor uses its HADBP chain checksum; a snapshot cursor uses
//! the full database hash of that exact remote snapshot. Neither is recovered
//! from the next object's prev_checksum or guessed from a changed local file.

use std::path::Path;

use anyhow::{bail, Context, Result};
use hadb_storage::StorageBackend;
use serde::Deserialize;
use walrust::hadb_changeset::storage::ChangesetKind;
use walrust::PullCursor;

pub(crate) async fn database_namespace(
    storage: &dyn StorageBackend,
    prefix: &str,
    name: &str,
) -> Result<String> {
    #[derive(Deserialize)]
    struct State {
        lineage_id: Option<String>,
    }
    let state_key = format!("{prefix}{name}/state.json");
    let lineage = match storage.get(&state_key).await? {
        Some(bytes) => {
            serde_json::from_slice::<State>(&bytes)
                .with_context(|| format!("invalid replication state at {state_key}"))?
                .lineage_id
        }
        None => None,
    };
    match lineage {
        Some(id) => {
            // A lineage is one key component, never an arbitrary storage path.
            if id.is_empty() || id.contains('/') || id == "." || id == ".." {
                bail!("invalid walrust lineage id in {state_key}");
            }
            Ok(format!("{name}/lineages/{id}"))
        }
        None => Ok(name.to_owned()),
    }
}

pub(crate) async fn cursor_at(
    storage: &dyn StorageBackend,
    prefix: &str,
    name: &str,
    path: &Path,
    seq: u64,
) -> Result<PullCursor> {
    if seq == 0 {
        return Ok(PullCursor {
            seq,
            checksum: walrust::ltx::compute_checksum_from_file(path)?,
        });
    }
    let namespace = database_namespace(storage, prefix, name).await?;
    for generation in [0, 1] {
        let key = walrust::hadb_changeset::storage::format_key(
            prefix,
            &namespace,
            generation,
            seq,
            ChangesetKind::Physical,
        );
        if let Some(bytes) = storage.get(&key).await? {
            let (object_seq, checksum) = if generation == 0 {
                let changeset = walrust::ltx::decode_sqlite_changeset(&bytes)
                    .with_context(|| format!("invalid cursor changeset at {key}"))?;
                (changeset.header.seq, changeset.checksum)
            } else {
                // Snapshot payload integrity is a HADBP checksum, but native
                // restore chains subsequent deltas from the full image hash.
                // Decode the exact remote snapshot, never the local follower
                // which may already contain later pages.
                let temp = tempfile::tempdir()?;
                let snapshot = walrust::ltx::decode_to_db(&bytes, &temp.path().join("snapshot.db"))
                    .with_context(|| format!("invalid cursor snapshot at {key}"))?;
                (snapshot.header.seq, snapshot.checksum)
            };
            if object_seq != seq {
                bail!("cursor changeset header seq mismatch at {key}");
            }
            return Ok(PullCursor { seq, checksum });
        }
    }
    bail!("cannot prove checksum for '{name}' at seq {seq}: applied changeset or snapshot missing")
}
