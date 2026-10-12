use super::*;
use serde_json::json;

#[test]
fn blob_messages_roundtrip() {
    let meta = BlobInfo {
        key: "a/b".into(),
        len: 3,
        content_type: Some("text/plain".into()),
    };
    let body = BlobBody {
        meta: meta.clone(),
        bytes: vec![0, 1, 255],
    };
    let wire = serde_json::to_value(&body).unwrap();
    assert_eq!(serde_json::from_value::<BlobBody>(wire).unwrap(), body);
    let put = PutBlobStreamRequest {
        key: "k".into(),
        len: 1 << 30,
        content_type: None,
    };
    let wire = serde_json::to_value(&put).unwrap();
    assert!(wire.get("content_type").is_none());
    assert_eq!(
        serde_json::from_value::<PutBlobStreamRequest>(wire).unwrap(),
        put
    );
    let range = GetRangeRequest {
        key: "k".into(),
        start: 1,
        end: 9,
    };
    let wire = serde_json::to_value(&range).unwrap();
    assert_eq!(
        serde_json::from_value::<GetRangeRequest>(wire).unwrap(),
        range
    );
    let caps = BlobCapabilities {
        presigned_urls: true,
        max_key_len: 1024,
    };
    let wire = serde_json::to_value(caps).unwrap();
    assert_eq!(
        serde_json::from_value::<BlobCapabilities>(wire).unwrap(),
        caps
    );
}

#[test]
fn blob_requests_reject_authority_selectors_and_hide_urls() {
    assert!(serde_json::from_value::<BlobKeyRequest>(json!({"key":"k","bucket":"b"})).is_err());
    assert!(
        serde_json::from_value::<PresignRequest>(json!({"key":"k","ttl_secs":5,"endpoint":"x"}))
            .is_err()
    );
    let url = PresignedUrl {
        url: "https://example.test/?sig=secret".into(),
    };
    assert!(!format!("{url:?}").contains("secret"));
}

#[test]
fn blob_selectors_are_fixed() {
    assert_eq!(BLOBS_OBJECT, "Blobs");
    assert_eq!(BLOBS_SELECTORS.len(), 11);
    assert!(is_blobs_selector("GetRange"));
    assert!(!is_blobs_selector("Arbitrary"));
}
