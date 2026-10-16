//! Sealing against a stream that is deleted and recreated mid-seal.

use super::*;
use crate::MongoStorage;
use serde_json::json;
use std::sync::Arc;
use tinystoragedrivers_core::{BlobStore, ErrorKind, Scope, StreamStore};

async fn live(label: &str) -> Option<MongoStorage> {
    let url = std::env::var("TSD_MONGO_URL").ok()?;
    let configured = mongodb::options::ClientOptions::parse(&url)
        .await
        .unwrap()
        .default_database
        .unwrap_or_else(|| "tsd_test".into());
    let fixture = format!("seal_{label}_{}", std::process::id());
    Some(
        MongoStorage::connect(&url, &configured)
            .await
            .unwrap()
            .fixture_named(&fixture)
            .unwrap(),
    )
}

#[tokio::test]
async fn a_seal_that_paused_across_a_delete_cannot_replace_the_recreated_streams_blob() {
    let Some(backend) = live("generation_blob").await else {
        return;
    };
    for transactions in [true, false] {
        let backend = if transactions {
            backend.clone()
        } else {
            backend.clone().without_transactions()
        };
        let streams = MongoStreams::new(Arc::clone(&backend.shared), Scope::local(), None);
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
async fn a_failed_or_deleted_seal_leaves_no_stale_blob_behind() {
    let Some(backend) = live("stale_blob").await else {
        return;
    };
    let streams = MongoStreams::new(Arc::clone(&backend.shared), Scope::local(), None);
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
