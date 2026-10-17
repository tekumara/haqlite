//! Explicit hadb-storage 0.5 -> 0.4 boundary for the pinned Turbolite VFS.
//!
//! Forward every operation, including optional batch/range methods: replacing
//! those with trait defaults could bypass backend fencing or error handling.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use hadb_storage::StorageBackend;
use hadb_storage_04::CasResult;

/// Use a current haqlite storage backend with Turbolite's older storage trait.
/// Builders wrap supplied backends automatically; embedders constructing a VFS
/// directly can wrap their backend with this adapter.
pub struct TurboliteStorage(pub Arc<dyn StorageBackend>);

fn cas(result: hadb_storage::CasResult) -> CasResult {
    CasResult {
        success: result.success,
        etag: result.etag,
    }
}

#[async_trait]
impl hadb_storage_04::StorageBackend for TurboliteStorage {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.0.get(key).await
    }
    async fn put(&self, key: &str, data: &[u8]) -> Result<()> {
        self.0.put(key, data).await
    }
    async fn delete(&self, key: &str) -> Result<()> {
        self.0.delete(key).await
    }
    async fn list(&self, prefix: &str, after: Option<&str>) -> Result<Vec<String>> {
        self.0.list(prefix, after).await
    }
    async fn exists(&self, key: &str) -> Result<bool> {
        self.0.exists(key).await
    }
    async fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<CasResult> {
        self.0.put_if_absent(key, data).await.map(cas)
    }
    async fn put_if_match(&self, key: &str, data: &[u8], etag: &str) -> Result<CasResult> {
        self.0.put_if_match(key, data, etag).await.map(cas)
    }
    async fn range_get(&self, key: &str, start: u64, len: u32) -> Result<Option<Vec<u8>>> {
        self.0.range_get(key, start, len).await
    }
    async fn delete_many(&self, keys: &[String]) -> Result<usize> {
        self.0.delete_many(keys).await
    }
    async fn put_many(&self, entries: &[(String, Vec<u8>)]) -> Result<()> {
        self.0.put_many(entries).await
    }
    fn backend_name(&self) -> &str {
        self.0.backend_name()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hadb_storage_04::StorageBackend as _;

    #[tokio::test]
    async fn vfs_storage_preserves_cas_and_pagination() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let backend = Arc::new(hadb_storage_local::LocalStorage::new(tmp.path()));
        let vfs_storage = TurboliteStorage(backend.clone());
        let created = vfs_storage.put_if_absent("pages/a", b"abcdef").await?;
        assert!(created.success);
        let etag = created
            .etag
            .expect("created etag crosses the version boundary");
        assert!(!vfs_storage.put_if_absent("pages/a", b"lost").await?.success);
        assert!(
            !vfs_storage
                .put_if_match("pages/a", b"lost", "stale")
                .await?
                .success
        );
        assert!(
            vfs_storage
                .put_if_match("pages/a", b"replacement", &etag)
                .await?
                .success
        );
        assert_eq!(backend.get("pages/a").await?, Some(b"replacement".to_vec()));
        assert_eq!(
            vfs_storage.range_get("pages/a", 2, 3).await?,
            Some(b"pla".to_vec())
        );
        vfs_storage
            .put_many(&[
                ("pages/b".into(), b"b".to_vec()),
                ("pages/c".into(), b"c".to_vec()),
            ])
            .await?;
        assert_eq!(
            vfs_storage.list("pages/", Some("pages/a")).await?,
            ["pages/b", "pages/c"]
        );
        assert_eq!(
            vfs_storage
                .delete_many(&["pages/a".into(), "pages/b".into()])
                .await?,
            2
        );
        assert!(!backend.exists("pages/a").await?);
        assert!(vfs_storage.exists("pages/c").await?);
        Ok(())
    }
}
