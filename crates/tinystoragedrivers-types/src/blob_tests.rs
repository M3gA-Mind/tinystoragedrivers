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
