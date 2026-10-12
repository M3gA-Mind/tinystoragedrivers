//! Captured collection and native conditional forwarding regressions.
use super::*;
use serde_json::json;
use tinybus::{broker::Broker, module::ModuleHost};
use tinystoragedrivers_bus::{Outcome, StorageKind, Version, Versioned};
use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};

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
    let storage = MemoryStorage::new();
    let docs = storage
        .for_scope(&Scope::new("alice").unwrap())
        .unwrap()
        .documents()
        .clone();
    let (_binding, service) = service(docs, "learned").await;
    let put = "Put".parse().unwrap();
    let first = service
        .dispatch(
            &put,
            json!([{"id":"index","doc":{"value":1},"precondition":{"kind":"absent"}}]),
        )
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_value::<Outcome<Version>>(first)
            .unwrap()
            .unwrap(),
        Version(1)
    );
    let conflict = service
        .dispatch(
            &put,
            json!([{"id":"index","doc":{"value":2},"precondition":{"kind":"absent"}}]),
        )
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_value::<Outcome<Version>>(conflict)
            .unwrap()
            .unwrap_err()
            .kind,
        StorageKind::Conflict
    );
    let get = service
        .dispatch(&"Get".parse().unwrap(), json!([{"id":"index"}]))
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_value::<Outcome<Option<Versioned>>>(get)
            .unwrap()
            .unwrap()
            .unwrap(),
        Versioned {
            id: "index".into(),
            version: Version(1),
            doc: json!({"value":1})
        }
    );
    let second = service
        .dispatch(
            &put,
            json!([{"id":"index","doc":{"value":2},"precondition":{"kind":"version","version":1}}]),
        )
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_value::<Outcome<Version>>(second)
            .unwrap()
            .unwrap(),
        Version(2)
    );
    let stale = service
        .dispatch(
            &"Delete".parse().unwrap(),
            json!([{"id":"index","precondition":{"kind":"version","version":1}}]),
        )
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_value::<Outcome<bool>>(stale)
            .unwrap()
            .unwrap_err()
            .kind,
        StorageKind::Conflict
    );
    let deleted = service
        .dispatch(
            &"Delete".parse().unwrap(),
            json!([{"id":"index","precondition":{"kind":"version","version":2}}]),
        )
        .await
        .unwrap();
    assert!(
        serde_json::from_value::<Outcome<bool>>(deleted)
            .unwrap()
            .unwrap()
    );
    let missing = service
        .dispatch(&"Get".parse().unwrap(), json!([{"id":"index"}]))
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_value::<Outcome<Option<Versioned>>>(missing)
            .unwrap()
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn captured_scope_and_collection_cannot_be_selected_by_document_body() {
    let storage = MemoryStorage::new();
    let alice = storage
        .for_scope(&Scope::new("alice").unwrap())
        .unwrap()
        .documents()
        .clone();
    let bob = storage
        .for_scope(&Scope::new("bob").unwrap())
        .unwrap()
        .documents()
        .clone();
    let (_binding, service) = service(alice.clone(), "learned").await;
    let request = json!([{"id":"index","doc":{"collection":"credentials","scope":"bob"},"precondition":{"kind":"absent"}}]);
    service
        .dispatch(&"Put".parse().unwrap(), request)
        .await
        .unwrap();
    assert!(alice.get("learned", "index").await.unwrap().is_some());
    assert!(alice.get("credentials", "index").await.unwrap().is_none());
    assert!(bob.get("learned", "index").await.unwrap().is_none());
}

#[tokio::test]
async fn direct_and_unverified_calls_refuse_before_document_mutation() {
    let storage = MemoryStorage::new();
    let docs = storage
        .for_scope(&Scope::new("alice").unwrap())
        .unwrap()
        .documents()
        .clone();
    let (_binding, service) = service(docs.clone(), "learned").await;
    let member = "Put".parse().unwrap();
    let request = json!([{"id":"index","doc":{},"precondition":{"kind":"absent"}}]);
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
async fn selector_fields_are_rejected_by_actual_dispatch_without_mutation() {
    let storage = MemoryStorage::new();
    let docs = storage
        .for_scope(&Scope::new("alice").unwrap())
        .unwrap()
        .documents()
        .clone();
    let (_binding, service) = service(docs.clone(), "learned").await;
    for field in ["scope", "collection", "url", "database", "generation"] {
        let mut request = json!({"id":"index","doc":{},"precondition":{"kind":"absent"}});
        request[field] = json!("forged");
        assert!(
            service
                .dispatch(&"Put".parse().unwrap(), json!([request]))
                .await
                .is_err()
        );
    }
    assert!(docs.get("learned", "index").await.unwrap().is_none());
}

#[tokio::test]
async fn registration_rejects_invalid_collection_without_creating_schema() {
    let host = ModuleHost::new(Broker::new());
    let binding = host
        .prepare_host_callbacks(vec![endpoint("/documents").unwrap()])
        .await
        .unwrap();
    let storage = MemoryStorage::new();
    let docs = storage
        .for_scope(&Scope::new("alice").unwrap())
        .unwrap()
        .documents()
        .clone();
    assert!(
        serve_collection(
            &binding,
            "/documents".parse().unwrap(),
            docs.clone(),
            "_tsd_internal"
        )
        .await
        .is_err()
    );
    assert!(docs.collections("").await.unwrap().is_empty());
}

#[tokio::test]
async fn retained_native_fence_refuses_late_write_without_changing_document() {
    use tinystoragedrivers_core::{Fence, Filter, Precondition};
    let storage = MemoryStorage::new();
    let guard_scope = Scope::new("cluster").unwrap();
    let guard = storage.for_scope(&guard_scope).unwrap();
    guard
        .documents()
        .put("leases", "alice", json!({"epoch":1}), Precondition::None)
        .await
        .unwrap();
    let target = storage
        .for_scope_fenced(
            &Scope::new("alice").unwrap(),
            &Fence::new(guard_scope, "leases", "alice", Filter::eq("epoch", 1)),
        )
        .unwrap();
    let (_binding, service) = service(target.documents().clone(), "learned").await;
    guard
        .documents()
        .put("leases", "alice", json!({"epoch":2}), Precondition::None)
        .await
        .unwrap();
    let late = service.dispatch(&"Put".parse().unwrap(), json!([{"id":"index","doc":{"private":"do not expose"},"precondition":{"kind":"none"}}])).await.unwrap();
    assert_eq!(late, json!({"Err":{"kind":"fenced"}}));
    assert!(
        target
            .documents()
            .get("learned", "index")
            .await
            .unwrap()
            .is_none()
    );
}

#[test]
fn every_native_failure_category_has_a_content_free_wire_mapping() {
    use tinystoragedrivers_core::{Capability, ErrorKind, StorageError};
    for (kind, expected) in [
        (ErrorKind::NotFound, StorageKind::NotFound),
        (ErrorKind::Conflict, StorageKind::Conflict),
        (ErrorKind::AlreadyExists, StorageKind::AlreadyExists),
        (
            ErrorKind::Unsupported(Capability::Fencing),
            StorageKind::Unsupported,
        ),
        (ErrorKind::InvalidInput, StorageKind::InvalidInput),
        (ErrorKind::Serialization, StorageKind::Serialization),
        (ErrorKind::Crypto, StorageKind::Crypto),
        (ErrorKind::Unavailable, StorageKind::Unavailable),
        (ErrorKind::Backend, StorageKind::Backend),
        (ErrorKind::Fenced, StorageKind::Fenced),
    ] {
        let error = StorageError::new(kind, "private credential and user document")
            .with_source(std::io::Error::other("private backend cause"));
        let failure = storage_failure(error);
        assert_eq!(failure.kind, expected);
        assert!(!serde_json::to_string(&failure).unwrap().contains("private"));
    }
}

#[tokio::test]
async fn malformed_arguments_unknown_members_and_native_invalid_input_are_distinct() {
    let storage = MemoryStorage::new();
    let scoped = storage.for_scope(&Scope::new("alice").unwrap()).unwrap();
    let (_binding, service) = service(scoped.documents().clone(), "learned").await;
    for args in [json!({}), json!([]), json!([{}, {}])] {
        let error = service
            .dispatch(&"Get".parse().unwrap(), args)
            .await
            .unwrap_err();
        assert_eq!(error.wire_message(), "invalid document callback arguments");
    }
    assert!(
        service
            .dispatch(&"Query".parse().unwrap(), json!([]))
            .await
            .is_err()
    );
    let invalid = service
        .dispatch(
            &"Put".parse().unwrap(),
            json!([{"id":"index","doc":"private invalid body","precondition":{"kind":"none"}}]),
        )
        .await
        .unwrap();
    assert_eq!(invalid, json!({"Err":{"kind":"invalid_input"}}));
}

#[tokio::test]
async fn callback_registration_is_fixed_declared_and_single_use() {
    let host = ModuleHost::new(Broker::new());
    let binding = host
        .prepare_host_callbacks(vec![endpoint("/documents").unwrap()])
        .await
        .unwrap();
    let storage = MemoryStorage::new();
    let scoped = storage.for_scope(&Scope::new("alice").unwrap()).unwrap();
    let docs = scoped.documents().clone();
    assert!(endpoint("invalid path").is_err());
    assert!(
        serve_collection(&binding, "/other".parse().unwrap(), docs.clone(), "learned")
            .await
            .is_err()
    );
    serve_collection(
        &binding,
        "/documents".parse().unwrap(),
        docs.clone(),
        "learned",
    )
    .await
    .unwrap();
    assert!(
        serve_collection(
            &binding,
            "/documents".parse().unwrap(),
            docs.clone(),
            "credentials"
        )
        .await
        .is_err()
    );
    // Registration has no declaration/create/write authority.
    assert!(docs.collections("").await.unwrap().is_empty());
}

#[test]
fn response_serialization_errors_never_echo_custom_private_prose() {
    struct RefusesSerialization;
    impl serde::Serialize for RefusesSerialization {
        fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("private source"))
        }
    }
    assert_eq!(
        encode(RefusesSerialization).unwrap_err().wire_message(),
        "document callback serialization failed"
    );
}
