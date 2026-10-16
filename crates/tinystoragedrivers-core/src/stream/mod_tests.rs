//! Stream name validation and entry serde.

use serde_json::json;

use super::*;

#[test]
fn stream_names_validate() {
    assert!(validate_stream("journal/run-1").is_ok());
    assert!(validate_stream("").is_err());
    assert!(validate_stream("nul\0").is_err());
    assert!(validate_stream(&"a".repeat(513)).is_err());
}

#[test]
fn entries_serialize_with_their_offset() {
    let entry = StreamEntry {
        offset: 3,
        value: json!({"k": 1}),
    };
    assert_eq!(
        serde_json::to_value(&entry).unwrap(),
        json!({"offset": 3, "value": {"k": 1}})
    );
}

/// A store that implements only the required methods, so the provided ones
/// are the trait's defaults.
#[derive(Debug)]
struct Minimal;

#[async_trait]
impl StreamStore for Minimal {
    async fn append(&self, _stream: &str, _value: Value) -> Result<u64> {
        Ok(0)
    }
    async fn append_batch(&self, _stream: &str, _values: Vec<Value>) -> Result<u64> {
        Ok(0)
    }
    async fn read_window(
        &self,
        _stream: &str,
        _from: u64,
        _limit: usize,
    ) -> Result<Vec<StreamEntry>> {
        Ok(Vec::new())
    }
    async fn len(&self, _stream: &str) -> Result<u64> {
        Ok(0)
    }
    async fn truncate_before(&self, _stream: &str, _offset: u64) -> Result<u64> {
        Ok(0)
    }
    async fn delete_stream(&self, _stream: &str) -> Result<bool> {
        Ok(false)
    }
    async fn streams(&self, _prefix: &str) -> Result<Vec<String>> {
        Ok(Vec::new())
    }
}

#[tokio::test]
async fn a_minimal_store_cannot_append_conditionally() {
    let error = Minimal
        .append_batch_if_offset("s", 0, vec![json!(1)])
        .await
        .unwrap_err();
    assert_eq!(
        error.kind(),
        crate::ErrorKind::Unsupported(crate::Capability::ConditionalStreamAppend)
    );
}

#[tokio::test]
async fn a_minimal_store_cannot_seal() {
    let error = Minimal.seal("s").await.unwrap_err();
    assert_eq!(
        error.kind(),
        crate::ErrorKind::Unsupported(crate::Capability::StreamSeal)
    );
}

#[tokio::test]
async fn a_minimal_store_records_no_stream_times() {
    let error = Minimal.info("s").await.unwrap_err();
    assert_eq!(
        error.kind(),
        crate::ErrorKind::Unsupported(crate::Capability::StreamSeal)
    );
}

#[tokio::test]
async fn stream_entries_round_trip_through_serde() {
    let entry = StreamEntry {
        offset: 9,
        value: json!([1, 2]),
    };
    let back: StreamEntry = serde_json::from_value(serde_json::to_value(&entry).unwrap()).unwrap();
    assert_eq!(back, entry);
}
