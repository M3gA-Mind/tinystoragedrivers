//! The seal steps against a live server: damaged blobs and segments, the
//! sealed-concurrently and deleted-meanwhile refusals, the unfenced marker
//! path of a deployment without physical transactions, existing-schema
//! handles, and sealing against a stream that is deleted and recreated
//! mid-seal (the sealed blob belongs to one generation). Each skips without
//! `TSD_MONGO_URL`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use mongodb::bson::doc;
use serde_json::json;
use tinystoragedrivers_core::{
    BlobStore, Capability, ErrorKind, Scope, SealOutcome, StorageBackend, StreamStore,
    sealed_blob_key,
};

use super::*;
use crate::backend::Shared;
use crate::{MongoStorage, naming};

async fn connect(test: &str) -> Option<MongoStorage> {
    let Some(url) = std::env::var("TSD_MONGO_URL")
        .ok()
        .filter(|url| !url.is_empty())
    else {
        eprintln!("{test}: TSD_MONGO_URL is not set; skipping");
        return None;
    };
    let rest = url.split_once("://").map_or(url.as_str(), |(_, rest)| rest);
    let path = rest.split_once('/').map_or("", |(_, path)| path);
    let name = path
        .split(['?', '/'])
        .next()
        .filter(|name| !name.is_empty())
        .unwrap_or("tsd_test");
    Some(MongoStorage::connect(&url, name).await.unwrap())
}

fn unique_scope(label: &str) -> Scope {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    Scope::new(format!(
        "sealtest_{label}_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
    .unwrap()
}

fn streams(storage: &MongoStorage, label: &str) -> MongoStreams {
    MongoStreams::new(Arc::clone(&storage.shared), unique_scope(label), None)
}

/// Handles over the same server that write markers the way a deployment
/// without multi-document transactions does.
fn unfenced_streams(storage: &MongoStorage, label: &str) -> MongoStreams {
    let shared = Shared::new(
        storage.shared.client.clone(),
        storage.shared.db.clone(),
        Arc::clone(&storage.shared.clock),
        false,
        false,
    );
    MongoStreams::new(Arc::new(shared), unique_scope(label), None)
}

async fn append_three(streams: &MongoStreams, name: &str) {
    for i in 0..3 {
        streams.append(name, json!({"i": i})).await.unwrap();
    }
}

#[tokio::test]
async fn live_a_sealed_stream_whose_blob_vanished_is_a_backend_error() {
    let Some(storage) = connect("live_a_sealed_stream_whose_blob_vanished").await else {
        return;
    };
    let streams = streams(&storage, "noblob");
    append_three(&streams, "log").await;
    streams.seal("log").await.unwrap();
    let generation = streams.generation("log", None).await.unwrap();
    streams
        .blobs()
        .delete(&MongoStreams::generation_blob_key("log", generation).unwrap())
        .await
        .unwrap();

    let read = streams.read_window("log", 0, 10).await.unwrap_err();
    assert_eq!(read.kind(), ErrorKind::Backend);
    assert!(read.to_string().contains("no blob"), "{read}");
    // Sealing again must not invent an outcome for a blob it cannot size.
    let again = streams.seal("log").await.unwrap_err();
    assert_eq!(again.kind(), ErrorKind::Backend);
}

#[tokio::test]
async fn live_segments_that_do_not_tile_the_stream_refuse_to_seal() {
    let Some(storage) = connect("live_segments_that_do_not_tile").await else {
        return;
    };
    let streams = streams(&storage, "gap");
    append_three(&streams, "log").await;
    // Lose the middle segment behind the driver's back.
    let removed = storage
        .shared
        .raw(naming::STREAMS)
        .delete_one(doc! {"_scope": streams.scope.as_str(), "s": "log", "o": 1_i64})
        .await
        .unwrap();
    assert_eq!(removed.deleted_count, 1);

    let error = streams.seal("log").await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
    assert!(error.to_string().contains("refusing to seal"), "{error}");
}

#[tokio::test]
async fn live_collecting_a_sealed_stream_reports_a_concurrent_seal() {
    let Some(storage) = connect("live_collecting_a_sealed_stream").await else {
        return;
    };
    let streams = streams(&storage, "collect");
    append_three(&streams, "log").await;
    streams.seal("log").await.unwrap();

    let handle = streams.segments().await.unwrap();
    let generation = streams.generation("log", None).await.unwrap();
    let error = streams
        .seal_collect(&handle, "log", generation)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Conflict);
}

#[tokio::test]
async fn live_a_stream_deleted_mid_seal_is_a_conflict() {
    let Some(storage) = connect("live_a_stream_deleted_mid_seal").await else {
        return;
    };
    let streams = streams(&storage, "deleted");
    append_three(&streams, "log").await;
    let generation = streams.generation("log", None).await.unwrap();
    streams
        .seal_generation_unchanged("log", generation)
        .await
        .unwrap();

    assert!(streams.delete_stream("log").await.unwrap());
    let error = streams
        .seal_generation_unchanged("log", generation)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Conflict);
}

#[tokio::test]
async fn live_sealing_without_physical_transactions_places_one_marker() {
    let Some(storage) = connect("live_sealing_without_physical_transactions").await else {
        return;
    };
    let streams = unfenced_streams(&storage, "unfenced");
    append_three(&streams, "log").await;

    let outcome = streams.seal("log").await.unwrap();
    assert_eq!((outcome.base, outcome.len), (0, 3));
    assert_eq!(streams.seal("log").await.unwrap(), outcome);
    assert_eq!(streams.read_window("log", 1, 10).await.unwrap().len(), 2);
    assert_eq!(streams.len("log").await.unwrap(), 3);
    assert_eq!(
        streams.append("log", json!(1)).await.unwrap_err().kind(),
        ErrorKind::Conflict
    );
}

#[tokio::test]
async fn live_a_repeated_marker_is_accepted_only_for_the_same_entries() {
    let Some(storage) = connect("live_a_repeated_marker").await else {
        return;
    };
    let streams = unfenced_streams(&storage, "marker");
    append_three(&streams, "log").await;
    let outcome = streams.seal("log").await.unwrap();
    let handle = streams.segments().await.unwrap();
    let generation = streams.generation("log", None).await.unwrap();

    // Another sealer of the same entries placed the identical marker.
    streams
        .seal_mark(&handle, "log", generation, outcome, None)
        .await
        .unwrap();
    // A marker at the same end but for another base collides and is refused.
    let other = SealOutcome::new(outcome.base + 1, outcome.len, outcome.blob_bytes);
    let error = streams
        .seal_mark(&handle, "log", generation, other, None)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Conflict);
}

#[tokio::test]
async fn live_a_marker_for_a_deleted_stream_is_refused_without_transactions() {
    let Some(storage) = connect("live_a_marker_for_a_deleted_stream").await else {
        return;
    };
    let streams = unfenced_streams(&storage, "retired");
    append_three(&streams, "log").await;
    let handle = streams.segments().await.unwrap();
    let generation = streams.generation("log", None).await.unwrap();
    let outcome = SealOutcome::new(0, 3, 0);
    assert!(streams.delete_stream("log").await.unwrap());

    let error = streams
        .seal_mark(&handle, "log", generation, outcome, Some(1))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Conflict);
}

#[tokio::test]
async fn live_existing_schema_handles_neither_seal_nor_need_a_blob_to_delete() {
    let Some(storage) = connect("live_existing_schema_handles").await else {
        return;
    };
    storage.prepare_schema().await.unwrap();
    let actor = storage
        .existing_schema()
        .unwrap()
        .for_scope(&unique_scope("existing"))
        .unwrap();
    let error = actor.streams().seal("log").await.unwrap_err();
    assert_eq!(
        error.kind(),
        ErrorKind::Unsupported(Capability::ExistingSchema)
    );
    assert!(!actor.streams().delete_stream("log").await.unwrap());
}

#[tokio::test]
async fn live_a_seal_that_paused_across_a_delete_cannot_replace_the_recreated_streams_blob() {
    let Some(storage) = connect("a_seal_that_paused_across_a_delete").await else {
        return;
    };
    for transactions in [true, false] {
        let storage = if transactions {
            storage.clone()
        } else {
            storage.clone().without_transactions()
        };
        let streams = streams(&storage, "paused");
        let name = format!("paused_{transactions}");
        streams.delete_stream(&name).await.unwrap();
        streams
            .append_batch(&name, vec![json!("old-1"), json!("old-2")])
            .await
            .unwrap();

        // The first seal collects generation G, then stalls before publishing.
        let handle = streams.segments().await.unwrap();
        let generation = streams.generation(&name, None).await.unwrap();
        let (stale, last_append_ms) = streams
            .seal_collect(&handle, &name, generation)
            .await
            .unwrap();

        // Meanwhile the stream is deleted, recreated and sealed as G+1.
        assert!(streams.delete_stream(&name).await.unwrap());
        let fresh = vec![json!("new-1"), json!("new-2"), json!("new-3")];
        streams.append_batch(&name, fresh.clone()).await.unwrap();
        streams.seal(&name).await.unwrap();

        // The stalled seal resumes: its publish and mark must not touch G+1.
        let outcome = streams
            .seal_publish(&name, generation, &stale)
            .await
            .unwrap();
        let marked = streams
            .seal_mark(&handle, &name, generation, outcome, last_append_ms)
            .await
            .unwrap_err();
        assert_eq!(
            marked.kind(),
            ErrorKind::Conflict,
            "transactions={transactions}"
        );
        let read: Vec<_> = streams
            .read_window(&name, 0, 10)
            .await
            .unwrap()
            .into_iter()
            .map(|entry| entry.value)
            .collect();
        assert_eq!(read, fresh, "G+1 still reads its own entries");

        streams.delete_stream(&name).await.unwrap();
    }
}

#[tokio::test]
async fn live_a_failed_or_deleted_seal_leaves_no_stale_blob_behind() {
    let Some(storage) = connect("a_failed_or_deleted_seal").await else {
        return;
    };
    let streams = streams(&storage, "stale");
    let name = "orphan";
    streams.delete_stream(name).await.unwrap();
    streams.append(name, json!(1)).await.unwrap();
    let handle = streams.segments().await.unwrap();
    let generation = streams.generation(name, None).await.unwrap();
    let (log, _) = streams
        .seal_collect(&handle, name, generation)
        .await
        .unwrap();
    streams.delete_stream(name).await.unwrap();
    streams.seal_publish(name, generation, &log).await.unwrap();
    streams.purge_stale_sealed_blobs(name).await.unwrap();
    let left = streams
        .blobs()
        .list(&sealed_blob_key(name).unwrap())
        .await
        .unwrap();
    assert!(left.is_empty(), "stale blobs: {left:?}");
}
