//! Plain blob metadata and payload wire values.
use super::*;

#[test]
fn blob_values_round_trip_with_optional_content_type() {
    let blob = Blob {
        meta: BlobMeta {
            key: "images/one".into(),
            len: 3,
            content_type: Some("image/png".into()),
        },
        bytes: vec![1, 2, 3],
    };
    let wire = serde_json::to_value(&blob).unwrap();
    assert_eq!(serde_json::from_value::<Blob>(wire).unwrap(), blob);
    let file = TreeFile::new("one.txt", b"abc".to_vec());
    assert_eq!(file.content_type, None);
    let entry = TreeEntry::Blob {
        name: "one".into(),
        meta: blob.meta,
    };
    assert_eq!(entry.name(), "one");
}

#[test]
fn paged_blob_values_preserve_unknown_age_and_cursor() {
    let page = BlobListPage {
        items: vec![BlobListEntry {
            meta: BlobMeta {
                key: "a".into(),
                len: 0,
                content_type: None,
            },
            written_at_ms: None,
        }],
        next_after: Some("a".into()),
    };
    let encoded = serde_json::to_value(&page).unwrap();
    assert_eq!(
        serde_json::from_value::<BlobListPage>(encoded.clone()).unwrap(),
        page
    );
    assert_eq!(encoded["items"][0].get("written_at_ms"), None);
    assert_eq!(MAX_BLOB_LIST_PAGE_ITEMS, 1_000);
}

#[test]
fn older_paged_blob_values_decode_without_age_or_cursor() {
    let page = serde_json::json!({
        "items": [{"meta": {"key": "old", "len": 1, "content_type": null}}]
    });
    let decoded: BlobListPage = serde_json::from_value(page).unwrap();
    assert_eq!(decoded.items[0].written_at_ms, None);
    assert_eq!(decoded.next_after, None);
}

#[test]
fn sealed_blob_namespace_is_reserved_only_below_its_prefix() {
    assert!(is_reserved_blob_key("_tsd/sealed/stream"));
    assert!(!is_reserved_blob_key("_tsd/sealed"));
    assert!(!is_reserved_blob_key("_tsd/sealedish/stream"));
}
