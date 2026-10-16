//! The blob step runner over the memory driver.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::schema::{MigrateOptions, Owner, Registry, Schema, ScopeClass, Step};
use crate::{ErrorKind, MemoryStorage, Scope, ScopedStorage, StorageBackend, StorageError};

const LEGACY: &str = "tinyjuice_ccr/";

fn storage() -> ScopedStorage {
    MemoryStorage::new().for_scope(&Scope::local()).unwrap()
}

fn registry(step: Step) -> Registry {
    Registry::compose([Schema::new(Owner::new("juice").unwrap())
        .blobs("juice/ccr/", ScopeClass::Tenant)
        .step(step)])
    .unwrap()
}

fn rename_step(calls: Arc<AtomicUsize>, fail_on: Option<usize>) -> Step {
    let failed = Arc::new(AtomicUsize::new(0));
    Step::blobs(
        "ccr_v2",
        "juice/ccr/",
        1,
        2,
        move |key, content_type, mut bytes| {
            let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
            if fail_on == Some(call) && failed.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(StorageError::backend("injected failure"));
            }
            bytes.reverse();
            let token = key.strip_prefix(LEGACY).unwrap();
            Ok(Some((format!("juice/ccr/{token}"), content_type, bytes)))
        },
    )
    .source_prefix(LEGACY)
}

async fn seed(storage: &ScopedStorage) {
    for (key, bytes, content_type) in [
        ("tinyjuice_ccr/a", b"abc".to_vec(), Some("text/plain")),
        ("tinyjuice_ccr/nested/b", b"de".to_vec(), None),
        ("tinyjuice_ccr/c", b"f".to_vec(), Some("application/json")),
    ] {
        storage.blobs().put(key, bytes, content_type).await.unwrap();
    }
}

#[tokio::test]
async fn renames_blobs_and_resumes_after_a_failure() {
    let storage = storage();
    seed(&storage).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let registry = registry(rename_step(Arc::clone(&calls), Some(2)));
    let options = MigrateOptions::new().batch_size(2);

    registry.migrate(&storage, &options).await.unwrap_err();
    assert_eq!(storage.blobs().list("juice/ccr/").await.unwrap().len(), 1);
    assert_eq!(storage.blobs().list(LEGACY).await.unwrap().len(), 2);
    assert!(registry.check_ready("juice/ccr/").is_err());

    let report = registry.migrate(&storage, &options).await.unwrap();
    assert_eq!(report.ran[0].written, 3);
    registry.check_ready("juice/ccr/").unwrap();
    assert_eq!(storage.blobs().list(LEGACY).await.unwrap().len(), 0);
    let moved = storage
        .blobs()
        .get("juice/ccr/nested/b")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(moved.bytes, b"ed");
    assert_eq!(moved.meta.content_type, None);
    let moved = storage.blobs().get("juice/ccr/a").await.unwrap().unwrap();
    assert_eq!(moved.bytes, b"cba");
    assert_eq!(moved.meta.content_type.as_deref(), Some("text/plain"));

    let before = calls.load(Ordering::SeqCst);
    let again = registry.migrate(&storage, &options).await.unwrap();
    assert_eq!(again.ran.len(), 0);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        before,
        "a finished step does not run again"
    );
}

#[tokio::test]
async fn resumes_over_an_identical_target_and_drops_none() {
    let storage = storage();
    seed(&storage).await;
    // An interrupted earlier run already wrote the target for `a` (reversed).
    storage
        .blobs()
        .put("juice/ccr/a", b"cba".to_vec(), Some("text/plain"))
        .await
        .unwrap();
    let step = Step::blobs("ccr_v2", "juice/ccr/", 1, 2, |key, ct, mut bytes| {
        bytes.reverse();
        Ok((!key.ends_with("/c")).then(|| (key.replace(LEGACY, "juice/ccr/"), ct, bytes)))
    })
    .source_prefix(LEGACY);
    registry(step)
        .migrate(&storage, &MigrateOptions::new())
        .await
        .unwrap();
    assert_eq!(
        storage
            .blobs()
            .get("juice/ccr/a")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"cba"
    );
    assert!(storage.blobs().get("juice/ccr/c").await.unwrap().is_none());
    assert_eq!(storage.blobs().list(LEGACY).await.unwrap().len(), 0);
}

#[tokio::test]
async fn a_target_holding_other_bytes_conflicts_and_keeps_the_source() {
    let storage = storage();
    seed(&storage).await;
    storage
        .blobs()
        .put("juice/ccr/a", b"unrelated".to_vec(), None)
        .await
        .unwrap();
    let error = registry(rename_step(Arc::new(AtomicUsize::new(0)), None))
        .migrate(&storage, &MigrateOptions::new())
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Conflict);
    let kept = storage.blobs().get(&format!("{LEGACY}a")).await.unwrap();
    assert_eq!(kept.unwrap().bytes, b"abc", "the source survives");
    let target = storage.blobs().get("juice/ccr/a").await.unwrap().unwrap();
    assert_eq!(target.bytes, b"unrelated", "the target is not overwritten");
}

#[tokio::test]
async fn a_target_with_another_content_type_conflicts() {
    let storage = storage();
    seed(&storage).await;
    storage
        .blobs()
        .put("juice/ccr/a", b"cba".to_vec(), Some("application/json"))
        .await
        .unwrap();
    let error = registry(rename_step(Arc::new(AtomicUsize::new(0)), None))
        .migrate(&storage, &MigrateOptions::new())
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Conflict);
    assert!(
        storage
            .blobs()
            .head(&format!("{LEGACY}a"))
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn two_sources_mapping_to_one_target_keep_the_second_source() {
    let storage = storage();
    seed(&storage).await;
    let step = Step::blobs("ccr_v2", "juice/ccr/", 1, 2, |_, ct, bytes| {
        Ok(Some(("juice/ccr/same".to_owned(), ct, bytes)))
    })
    .source_prefix(LEGACY);
    let error = registry(step)
        .migrate(&storage, &MigrateOptions::new())
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Conflict);
    // The first source moved; the colliding ones were not discarded.
    assert_eq!(storage.blobs().list(LEGACY).await.unwrap().len(), 2);
    assert!(
        storage
            .blobs()
            .head("juice/ccr/same")
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn refuses_a_target_outside_the_prefix_and_keeps_the_source() {
    let storage = storage();
    seed(&storage).await;
    let step = Step::blobs("ccr_v2", "juice/ccr/", 1, 2, |_, ct, bytes| {
        Ok(Some(("elsewhere/x".to_owned(), ct, bytes)))
    })
    .source_prefix(LEGACY);
    let error = registry(step)
        .migrate(&storage, &MigrateOptions::new())
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert_eq!(storage.blobs().list(LEGACY).await.unwrap().len(), 3);
}
