//! Optional and availability capability set behavior.
use super::*;

#[test]
fn capability_sets_support_all_set_operations_and_stable_iteration() {
    let all = Capabilities::all();
    assert!(all.contains(Capability::Documents));
    assert!(all.contains(Capability::CrossProcessCas));
    assert!(all.contains(Capability::ConditionalSecretWrites));
    assert!(all.contains(Capability::BlobListing));
    assert!(all.require(Capability::Documents).is_ok());
    let chosen = all.without(Capability::FullText).with(Capability::Streams);
    assert!(!chosen.contains(Capability::FullText));
    assert_eq!(chosen.iter().next(), Some(Capability::Documents));
    assert_eq!(
        Capabilities::none()
            .with(Capability::Kv)
            .iter()
            .collect::<Vec<_>>(),
        vec![Capability::Kv]
    );
}

#[test]
fn missing_capability_is_typed() {
    assert_eq!(
        Capabilities::none()
            .require(Capability::BlobTtl)
            .unwrap_err()
            .kind(),
        crate::ErrorKind::Unsupported(Capability::BlobTtl)
    );
}

#[test]
fn capability_wire_names_and_display_names_cover_every_variant() {
    let cases = [
        (Capability::Documents, "documents"),
        (Capability::Streams, "streams"),
        (Capability::Kv, "kv"),
        (Capability::Blobs, "blobs"),
        (Capability::Queues, "queues"),
        (Capability::Secrets, "secrets"),
        (Capability::CollectionValidation, "collection_validation"),
        (Capability::FullText, "full_text"),
        (Capability::Transactions, "transactions"),
        (Capability::Ttl, "ttl"),
        (Capability::PresignedUrls, "presigned_urls"),
        (Capability::StreamingBlobs, "streaming_blobs"),
        (Capability::CollectionEnumeration, "collection_enumeration"),
        (
            Capability::ConditionalStreamAppend,
            "conditional_stream_append",
        ),
        (Capability::ValidatedBatch, "validated_batch"),
        (Capability::Watch, "watch"),
        (Capability::BlobTtl, "blob_ttl"),
        (Capability::RecordMetadata, "record_metadata"),
        (Capability::StreamSeal, "stream_seal"),
        (Capability::SharedAcrossProcesses, "shared_across_processes"),
        (Capability::CrossProcessCas, "cross_process_cas"),
        (
            Capability::ConditionalSecretWrites,
            "conditional_secret_writes",
        ),
        (Capability::BlobListing, "blob_listing"),
    ];
    for (capability, name) in cases {
        assert_eq!(capability.to_string(), name);
        assert_eq!(
            serde_json::to_string(&capability).unwrap(),
            format!("\"{name}\"")
        );
    }
    assert!(format!("{:?}", Capabilities::none()).contains("{}"));
}
