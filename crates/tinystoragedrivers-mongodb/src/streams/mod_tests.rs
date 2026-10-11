//! Segment encoding and the arithmetic of trimmed segments.

use super::*;
use serde_json::json;
use tinystoragedrivers_core::ErrorKind;

#[test]
fn new_segments_cover_their_batch() {
    let segment = new_segment("log", 0, 4, &[json!(1), json!({"a": 2})]).unwrap();
    assert_eq!(segment.get_str("s").unwrap(), "log");
    assert_eq!(segment.get_i64("o").unwrap(), 4);
    assert_eq!(segment.get_i64("n").unwrap(), 2);
    assert_eq!(segment.get_i64("e").unwrap(), 6);
    assert_eq!(segment.get_i64("t").unwrap(), 0);
    let decoded = Segment::decode(&segment).unwrap();
    assert_eq!((decoded.start, decoded.end, decoded.trimmed), (4, 6, 0));
    assert_eq!(decoded.id, Bson::Null);

    assert_eq!(
        new_segment("log", 0, u64::MAX, &[json!(1)])
            .unwrap_err()
            .kind(),
        ErrorKind::Backend
    );
    assert_eq!(
        new_segment("log", 0, u64::MAX - 1, &[json!(1)])
            .unwrap_err()
            .kind(),
        ErrorKind::Backend
    );
    assert_eq!(
        new_segment("log", 0, 0, &[json!(u64::MAX)])
            .unwrap_err()
            .kind(),
        ErrorKind::Serialization
    );
}

#[test]
fn trimmed_segments_number_their_remaining_entries() {
    let segment = Segment::decode(&doc! {
        "_id": 1, "o": 10_i64, "e": 14_i64, "t": 2_i64, "vs": ["c", "d"],
    })
    .unwrap();
    let offsets: Vec<u64> = segment.entries_from(0).map(|(offset, _)| offset).collect();
    assert_eq!(offsets, [12, 13]);
    let later: Vec<u64> = segment.entries_from(13).map(|(offset, _)| offset).collect();
    assert_eq!(later, [13]);
}

#[test]
fn malformed_segments_are_rejected() {
    for broken in [
        doc! {"o": 0_i64, "e": 1_i64, "t": 0_i64},
        doc! {"o": 0_i64, "e": 2_i64, "t": 0_i64, "vs": [1]},
        doc! {"o": -1_i64, "e": 0_i64, "t": 0_i64, "vs": []},
        doc! {"e": 0_i64, "t": 0_i64, "vs": []},
    ] {
        assert_eq!(
            Segment::decode(&broken).unwrap_err().kind(),
            ErrorKind::Serialization
        );
    }
    assert_eq!(
        stored_offset(u64::MAX).unwrap_err().kind(),
        ErrorKind::Backend
    );
    assert_eq!(segment_models().len(), 2);
}

#[tokio::test]
async fn live_conditional_append_rejects_a_generation_deleted_after_its_snapshot() {
    let Ok(url) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let backend = crate::MongoStorage::connect(&url, "conditional_generation_test")
        .await
        .unwrap();
    let streams = MongoStreams::new(Arc::clone(&backend.shared), Scope::local(), None);
    let name = format!("generation_{}", std::process::id());
    streams.delete_stream(&name).await.unwrap();
    streams.append(&name, json!(1)).await.unwrap();
    // Verify explicit public-capability refusal on every deployment, then
    // run the transaction snapshot race only where transactions are native.
    let disabled = backend.clone().without_transactions();
    let disabled_streams = MongoStreams::new(Arc::clone(&disabled.shared), Scope::local(), None);
    let error = disabled_streams
        .append_batch_if_offset(&name, 1, vec![json!(2)])
        .await
        .unwrap_err();
    assert_eq!(
        error.kind(),
        ErrorKind::Unsupported(Capability::ConditionalStreamAppend)
    );
    assert_eq!(streams.len(&name).await.unwrap(), 1);
    assert_eq!(
        streams.read_window(&name, 0, 10).await.unwrap()[0].value,
        json!(1)
    );
    if !tinystoragedrivers_core::StorageBackend::capabilities(&backend)
        .contains(Capability::ConditionalStreamAppend)
    {
        return;
    }
    let handle = streams.segments().await.unwrap();
    let mut session = backend.shared.client.start_session().await.unwrap();
    session.start_transaction().await.unwrap();
    streams.generation(&name, Some(&mut session)).await.unwrap();
    streams.delete_stream(&name).await.unwrap();
    let attempt = streams
        .append_once(&handle, &name, &[json!(2)], Some(1), Some(&mut session))
        .await;
    assert!(
        attempt.is_err(),
        "deleted generation must abort the conditional attempt"
    );
    session.abort_transaction().await.unwrap();
    assert_eq!(streams.len(&name).await.unwrap(), 0);
    assert_eq!(
        streams
            .append_batch_if_offset(&name, 0, vec![json!(3)])
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        streams.read_window(&name, 0, 10).await.unwrap()[0].value,
        json!(3)
    );
}
