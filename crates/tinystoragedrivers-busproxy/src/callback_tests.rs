//! Regression tests for confined host document callbacks.
use super::*;
use serde_json::json;
use tinybus::{broker::Broker, module::ModuleHost};
use tinystoragedrivers_bus::{StorageFailure, StorageKind, Versioned};
use tinystoragedrivers_testkit::memory_storage;
use tinystoragedrivers_types::{Meta, Scope, Version};

async fn service(
    documents: Arc<dyn DocumentStore>,
    collection: &str,
) -> (HostCallbackBinding, DocumentService) {
    let host = ModuleHost::new(Broker::new());
    let declaration = endpoint("/documents").unwrap();
    let binding = host
        .prepare_host_callbacks(vec![declaration.clone()])
        .await
        .unwrap();
    let service = DocumentService {
        documents,
        collection: collection.into(),
        guard: binding.guard(),
        interface: declaration.interface,
        members: declaration.members,
    };
    (binding, service)
}

#[tokio::test]
async fn native_cas_conflicts_preserve_document_and_version() {
    let storage = memory_storage(&Scope::new("callback-cas").unwrap());
    let docs = storage.documents().clone();
    let (_binding, service) = service(docs, "learned").await;
    let put = "Put".parse().unwrap();
    let first = service
        .dispatch(
            &put,
            json!([{"document_id":"index","document_json":"{\"value\":1}","precondition":{"kind":"absent"}}]),
        )
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_value::<std::result::Result<Version, StorageFailure>>(first)
            .unwrap()
            .unwrap(),
        Version(1)
    );
    let conflict = service
        .dispatch(
            &put,
            json!([{"document_id":"index","document_json":"{\"value\":2}","precondition":{"kind":"absent"}}]),
        )
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_value::<std::result::Result<Version, StorageFailure>>(conflict)
            .unwrap()
            .unwrap_err()
            .kind,
        StorageKind::Conflict
    );
    let get = service
        .dispatch(&"Get".parse().unwrap(), json!([{"document_id":"index"}]))
        .await
        .unwrap();
    let versioned = serde_json::from_value::<
        std::result::Result<Option<Versioned<Value>>, StorageFailure>,
    >(get)
    .unwrap()
    .unwrap()
    .unwrap();
    assert_eq!(versioned.id, "index");
    assert_eq!(versioned.version, Version(1));
    assert_eq!(versioned.doc, json!({"value":1}));
    assert_eq!(versioned.meta.schema, Meta::unknown().schema);
    assert!(versioned.meta.created_ms > 0);
    let second = service
        .dispatch(
            &put,
            json!([{"document_id":"index","document_json":"{\"value\":2}","precondition":{"kind":"version","version":1}}]),
        )
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_value::<std::result::Result<Version, StorageFailure>>(second)
            .unwrap()
            .unwrap(),
        Version(2)
    );
    let stale = service
        .dispatch(
            &"Delete".parse().unwrap(),
            json!([{"document_id":"index","precondition":{"kind":"version","version":1}}]),
        )
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_value::<std::result::Result<bool, StorageFailure>>(stale)
            .unwrap()
            .unwrap_err()
            .kind,
        StorageKind::Conflict
    );
    let deleted = service
        .dispatch(
            &"Delete".parse().unwrap(),
            json!([{"document_id":"index","precondition":{"kind":"version","version":2}}]),
        )
        .await
        .unwrap();
    assert!(
        serde_json::from_value::<std::result::Result<bool, StorageFailure>>(deleted)
            .unwrap()
            .unwrap()
    );
}

#[tokio::test]
async fn captured_collection_and_scope_cannot_be_selected_by_document_body() {
    let storage = memory_storage(&Scope::new("callback-scope").unwrap());
    let docs = storage.documents().clone();
    let (_binding, service) = service(docs.clone(), "learned").await;
    let request = json!([{"document_id":"index","document_json":"{\"collection\":\"credentials\",\"scope\":\"forged\"}","precondition":{"kind":"absent"}}]);
    service
        .dispatch(&"Put".parse().unwrap(), request)
        .await
        .unwrap();
    assert!(docs.get("learned", "index").await.unwrap().is_some());
    assert!(docs.get("credentials", "index").await.unwrap().is_none());
}

#[tokio::test]
async fn direct_and_unverified_calls_refuse_before_mutation() {
    let storage = memory_storage(&Scope::new("callback-guard").unwrap());
    let docs = storage.documents().clone();
    let (_binding, service) = service(docs.clone(), "learned").await;
    let member = "Put".parse().unwrap();
    let request =
        json!([{"document_id":"index","document_json":"{}","precondition":{"kind":"absent"}}]);
    assert!(service.call(&member, request.clone()).await.is_err());
    assert!(
        service
            .call_with_context(&member, request, &CallContext::default())
            .await
            .is_err()
    );
    assert!(docs.get("learned", "index").await.unwrap().is_none());
}

#[tokio::test]
async fn callback_rejects_selector_fields_and_invalid_collections() {
    let storage = memory_storage(&Scope::new("callback-input").unwrap());
    let docs = storage.documents().clone();
    let (_binding, service) = service(docs.clone(), "learned").await;
    let mut request =
        json!({"document_id":"index","document_json":"{}","precondition":{"kind":"absent"}});
    request["collection"] = json!("credentials");
    assert!(
        service
            .dispatch(&"Put".parse().unwrap(), json!([request]))
            .await
            .is_err()
    );
    let host = ModuleHost::new(Broker::new());
    let binding = host
        .prepare_host_callbacks(vec![endpoint("/documents").unwrap()])
        .await
        .unwrap();
    assert!(
        serve_collection(
            &binding,
            "/documents".parse().unwrap(),
            docs,
            "_tsd_internal"
        )
        .await
        .is_err()
    );
}

#[test]
fn callback_failure_mapping_is_content_free() {
    use tinystoragedrivers_types::{Capability, ErrorKind, StorageError};
    for (kind, expected) in [
        (ErrorKind::NotFound, StorageKind::NotFound),
        (ErrorKind::Conflict, StorageKind::Conflict),
        (ErrorKind::AlreadyExists, StorageKind::AlreadyExists),
        (
            ErrorKind::Unsupported(Capability::Watch),
            StorageKind::Unsupported,
        ),
        (ErrorKind::InvalidInput, StorageKind::InvalidInput),
        (ErrorKind::Serialization, StorageKind::Serialization),
        (ErrorKind::Crypto, StorageKind::Crypto),
        (ErrorKind::Unavailable, StorageKind::Unavailable),
        (ErrorKind::Backend, StorageKind::Backend),
    ] {
        let error = StorageError::new(kind, "private document");
        let failure = storage_failure(&error);
        assert_eq!(failure.kind, expected);
        assert!(!serde_json::to_string(&failure).unwrap().contains("private"));
    }
}

#[test]
fn endpoint_rejects_invalid_paths() {
    assert!(endpoint("invalid path").is_err());
}
