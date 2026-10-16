//! Crash points of the sealing protocol: blob, then head, then entries.
//!
//! The three steps are separate operations on the handle, so stopping after
//! one is exactly the state a crash there would leave behind.

use serde_json::{Value, json};

use super::*;
use crate::{ErrorKind, SealedLog, StreamEntry, StreamStore, sealed_blob_key};

fn handle() -> (MemoryStorage, MemoryStreams) {
    let storage = MemoryStorage::new();
    let streams = MemoryStreams::new(
        Arc::clone(&storage.db),
        Scope::local(),
        Arc::clone(&storage.clock),
        None,
    );
    (storage, streams)
}

async fn fill(streams: &MemoryStreams, name: &str, count: u64) -> Vec<StreamEntry> {
    let values: Vec<Value> = (0..count).map(|i| json!({"i": i})).collect();
    streams.append_batch(name, values).await.unwrap();
    streams.read_window(name, 0, usize::MAX).await.unwrap()
}

fn blob_exists(storage: &MemoryStorage, name: &str) -> bool {
    let state = storage.db.lock().unwrap();
    let key = (
        Scope::local().as_str().to_owned(),
        sealed_blob_key(name).unwrap(),
    );
    state.blobs.contains_key(&key)
}

fn rows(storage: &MemoryStorage, name: &str) -> usize {
    let state = storage.db.lock().unwrap();
    state
        .streams
        .get(&(Scope::local().as_str().to_owned(), name.to_owned()))
        .map_or(0, |stream| stream.entries.len())
}

#[tokio::test]
async fn a_crash_after_the_blob_write_leaves_the_stream_readable_and_unsealed() {
    let (storage, streams) = handle();
    let before = fill(&streams, "j", 20).await;

    let written = streams.seal_write_blob("j").unwrap();
    assert!(blob_exists(&storage, "j"));

    assert_eq!(
        streams.read_window("j", 0, usize::MAX).await.unwrap(),
        before
    );
    assert_eq!(streams.len("j").await.unwrap(), 20);
    assert_eq!(streams.append("j", json!("more")).await.unwrap(), 20);

    // The blob is now stale; sealing again replaces it and seals everything.
    let outcome = streams.seal("j").await.unwrap();
    assert_eq!((outcome.base, outcome.len), (0, 21));
    assert_ne!(outcome, written);
    assert_eq!(
        streams.read_window("j", 0, usize::MAX).await.unwrap().len(),
        21
    );
    assert_eq!(rows(&storage, "j"), 0);
}

#[tokio::test]
async fn a_crash_after_the_blob_write_resumes_to_the_same_outcome() {
    let (storage, streams) = handle();
    let before = fill(&streams, "j", 20).await;
    let written = streams.seal_write_blob("j").unwrap();

    let outcome = streams.seal("j").await.unwrap();
    assert_eq!(outcome, written, "deterministic encoding");
    assert_eq!(
        streams.read_window("j", 0, usize::MAX).await.unwrap(),
        before
    );
    assert_eq!(rows(&storage, "j"), 0);
}

#[tokio::test]
async fn a_crash_after_the_head_flip_reads_from_the_blob_and_resealing_drops_the_entries() {
    let (storage, streams) = handle();
    let before = fill(&streams, "j", 20).await;
    let written = streams.seal_write_blob("j").unwrap();
    streams.seal_mark("j", written).unwrap();
    assert_eq!(rows(&storage, "j"), 20, "the entries are still stored");

    // Prove the reads do not come from the entries: replace them.
    {
        let mut state = storage.db.lock().unwrap();
        let key = (Scope::local().as_str().to_owned(), "j".to_owned());
        let stream = state.streams.get_mut(&key).unwrap();
        stream
            .entries
            .iter_mut()
            .for_each(|value| *value = json!("stale"));
    }
    assert_eq!(
        streams.read_window("j", 0, usize::MAX).await.unwrap(),
        before
    );
    assert_eq!(streams.len("j").await.unwrap(), 20);
    assert_eq!(
        streams.append("j", json!(1)).await.unwrap_err().kind(),
        ErrorKind::Conflict
    );

    let outcome = streams.seal("j").await.unwrap();
    assert_eq!(outcome, written);
    assert_eq!(rows(&storage, "j"), 0);
    assert_eq!(
        streams.read_window("j", 0, usize::MAX).await.unwrap(),
        before
    );
    assert_eq!(streams.len("j").await.unwrap(), 20);
}

#[tokio::test]
async fn marking_refuses_a_blob_the_stream_has_outgrown() {
    let (_storage, streams) = handle();
    fill(&streams, "j", 5).await;
    let written = streams.seal_write_blob("j").unwrap();
    streams.append("j", json!("late")).await.unwrap();
    assert_eq!(
        streams.seal_mark("j", written).unwrap_err().kind(),
        ErrorKind::Conflict
    );
    streams.truncate_before("j", 2).await.unwrap();
    let outgrown = streams.seal_write_blob("j").unwrap();
    streams.truncate_before("j", 4).await.unwrap();
    assert_eq!(
        streams.seal_mark("j", outgrown).unwrap_err().kind(),
        ErrorKind::Conflict,
        "a truncation also invalidates the blob"
    );
    assert_eq!(streams.len("j").await.unwrap(), 6, "nothing was sealed");
    assert_eq!(streams.append("j", json!(0)).await.unwrap(), 6);
}

#[tokio::test]
async fn the_sealed_blob_holds_the_entries_in_the_shared_format() {
    let (storage, streams) = handle();
    let before = fill(&streams, "a/b", 3).await;
    streams.seal("a/b").await.unwrap();
    // The blob is reserved, so it is read from the store, not the blob port.
    let bytes = {
        let state = storage.db.lock().unwrap();
        let key = (
            Scope::local().as_str().to_owned(),
            "_tsd/sealed/a%2Fb".to_owned(),
        );
        state.blobs.get(&key).unwrap().1.clone()
    };
    assert_eq!(SealedLog::decode(&bytes).unwrap().window(0, 10), before);
}

#[tokio::test]
async fn a_sealed_stream_whose_blob_is_gone_reports_a_backend_error() {
    let (storage, streams) = handle();
    fill(&streams, "j", 3).await;
    streams.seal("j").await.unwrap();
    {
        let mut state = storage.db.lock().unwrap();
        state.blobs.clear();
    }
    for error in [
        streams.read_window("j", 0, 10).await.unwrap_err(),
        streams.seal("j").await.unwrap_err(),
    ] {
        assert_eq!(error.kind(), ErrorKind::Backend);
    }
    assert!(streams.delete_stream("j").await.unwrap());
}

#[tokio::test]
async fn deleting_an_unsealed_stream_clears_a_blob_a_crash_left() {
    let (storage, streams) = handle();
    fill(&streams, "j", 3).await;
    streams.seal_write_blob("j").unwrap();
    assert!(streams.delete_stream("j").await.unwrap());
    assert!(!blob_exists(&storage, "j"));
    assert_eq!(streams.append("j", json!(1)).await.unwrap(), 0);
    assert_eq!(streams.seal("j").await.unwrap().len, 1);
}

#[tokio::test]
async fn sealing_one_scope_leaves_a_same_named_stream_in_another_alone() {
    let storage = MemoryStorage::new();
    let a = storage.for_scope(&Scope::new("a").unwrap()).unwrap();
    let b = storage.for_scope(&Scope::new("b").unwrap()).unwrap();
    a.streams().append("j", json!("a")).await.unwrap();
    b.streams().append("j", json!("b")).await.unwrap();
    a.streams().seal("j").await.unwrap();
    assert_eq!(b.streams().append("j", json!("b2")).await.unwrap(), 1);
    let read = b.streams().read_window("j", 0, 10).await.unwrap();
    assert_eq!(read.len(), 2);
    assert_eq!(read[0].value, json!("b"));
    assert_eq!(a.streams().read_window("j", 0, 10).await.unwrap().len(), 1);
}

#[tokio::test]
async fn marking_refuses_a_stream_that_was_deleted_and_recreated_meanwhile() {
    let (storage, streams) = handle();
    fill(&streams, "j", 3).await;
    let written = streams.seal_write_blob("j").unwrap();
    streams.delete_stream("j").await.unwrap();
    fill(&streams, "j", 3).await;
    assert_eq!(
        streams.seal_mark("j", written).unwrap_err().kind(),
        ErrorKind::Conflict,
        "the recreated stream has the old length but not the old blob"
    );
    assert!(!blob_exists(&storage, "j"));
    assert_eq!(streams.append("j", json!(1)).await.unwrap(), 3);
}
