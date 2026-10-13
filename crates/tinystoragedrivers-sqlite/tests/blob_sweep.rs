//! The blob sweep hook reclaims lapsed blobs through `dyn StorageBackend`.

use std::sync::Arc;
use std::time::Duration;

use tinystoragedrivers_core::{Scope, StorageBackend};
use tinystoragedrivers_sqlite::SqliteStorage;

#[tokio::test]
async fn the_backend_sweep_hook_reclaims_only_lapsed_blobs() {
    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path()).unwrap();
    let blobs = storage.for_scope(&Scope::local()).unwrap().blobs().clone();
    blobs
        .put_expiring("old", vec![1], None, Duration::from_millis(20))
        .await
        .unwrap();
    blobs
        .put_expiring("young", vec![2], None, Duration::from_secs(3600))
        .await
        .unwrap();
    blobs.put("forever", vec![3], None).await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;

    let backend: Arc<dyn StorageBackend> = Arc::new(storage);
    assert_eq!(
        backend.sweep_expired_blobs(&Scope::local()).await.unwrap(),
        1
    );
    assert_eq!(
        backend.sweep_expired_blobs(&Scope::local()).await.unwrap(),
        0
    );
    let kept: Vec<String> = blobs
        .list("")
        .await
        .unwrap()
        .into_iter()
        .map(|meta| meta.key)
        .collect();
    assert_eq!(kept, ["forever", "young"]);
}
