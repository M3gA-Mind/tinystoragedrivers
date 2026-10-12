use super::*;
use crate::PerCallerEndpoint;
use tinybus::{broker::Broker, transport::memory::MemoryBus};
use tinystoragedrivers_bus::STORAGE_INTERFACE;
use tinystoragedrivers_client::StorageServer;
use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};

async fn service(object: Object) -> (ObjectService, Connection) {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let backend = MemoryStorage::new();
    let endpoint = PerCallerEndpoint::new(move |_| {
        Some(StorageServer::new(
            backend.for_scope(&Scope::local()).unwrap(),
        ))
    });
    let shared = Arc::new(Shared {
        connection: connection.downgrade(),
        endpoint: Arc::new(endpoint),
        forwarders: Mutex::new(HashMap::new()),
    });
    (ObjectService { object, shared }, connection)
}

fn member(name: &str) -> MemberName {
    MemberName::new(name).unwrap()
}

#[test]
fn request_arguments_are_zero_or_one() {
    assert_eq!(request_of(Value::Null).unwrap(), Value::Null);
    assert_eq!(request_of(json!([])).unwrap(), Value::Null);
    assert_eq!(request_of(json!([{"a": 1}])).unwrap(), json!({"a": 1}));
    assert_eq!(request_of(json!([1, 2])).unwrap_err(), invalid());
    assert_eq!(request_of(json!({"a": 1})).unwrap_err(), invalid());
    assert_eq!(
        HostFailure::from(StorageFailure {
            kind: StorageKind::Fenced
        }),
        HostFailure::Storage(StorageFailure {
            kind: StorageKind::Fenced
        })
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn each_object_advertises_its_selectors_and_only_secrets_are_confidential() {
    for (object, count) in [
        (
            Object::Documents,
            tinystoragedrivers_bus::DOCUMENTS_SELECTORS.len(),
        ),
        (
            Object::Streams,
            tinystoragedrivers_bus::STREAMS_SELECTORS.len(),
        ),
        (Object::Blobs, tinystoragedrivers_bus::BLOBS_SELECTORS.len()),
        (
            Object::Secrets,
            tinystoragedrivers_bus::SECRETS_SELECTORS.len(),
        ),
    ] {
        let (service, _connection) = service(object).await;
        assert_eq!(service.name().as_str(), STORAGE_INTERFACE);
        assert_eq!(service.members().len(), count);
        assert_eq!(
            service.requires_confidential(&member("Get")),
            object == Object::Secrets
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_call_without_a_proven_sender_is_refused_on_every_path() {
    let (documents, _c1) = service(Object::Documents).await;
    let error = documents
        .call(&member("Capabilities"), Value::Null)
        .await
        .unwrap_err();
    assert!(matches!(&error, tinybus::Error::MethodFailed { name, .. } if name == ERROR_DENIED));
    let context = CallContext::default();
    let error = documents
        .call_with_context(&member("Capabilities"), Value::Null, &context)
        .await
        .unwrap_err();
    assert!(
        matches!(&error, tinybus::Error::MethodFailed { name, message }
        if name == ERROR_DENIED && message == "unauthenticated-caller")
    );

    let (blobs, _c2) = service(Object::Blobs).await;
    // GetStream has nowhere safe to send a body without a sender.
    let error = blobs
        .call_with_context(&member("GetStream"), json!([{"key": "k"}]), &context)
        .await
        .unwrap_err();
    assert!(
        matches!(&error, tinybus::Error::MethodFailed { name, message }
        if name == ERROR_DENIED && message == "unauthenticated-caller")
    );
    // Malformed streamed-call arguments are invalid input, not a panic.
    for args in [
        Value::Null,
        json!([{"key": "k", "len": 1}]),
        json!([{"key": "k", "len": 1}, {"id": "s9"}, 1]),
        json!([{"bad": 1}, {"id": "s9"}]),
        json!([{"key": "k", "len": 1}, {"bad": 1}]),
        json!([{"key": "k", "len": 1}, {"id": "s9", "len": 2}]),
        json!([{"key": "k", "len": 1}, {"id": "no-such-stream", "len": 1}]),
    ] {
        let error = blobs
            .call_with_context(&member("PutStream"), args, &context)
            .await
            .unwrap_err();
        assert!(
            matches!(&error, tinybus::Error::MethodFailed { name, message }
                if name == ERROR_STORAGE && message == "invalid_input"),
            "{error:?}"
        );
    }
    let error = blobs
        .call_with_context(&member("GetStream"), json!({"not": "an array"}), &context)
        .await
        .unwrap_err();
    assert!(matches!(&error, tinybus::Error::MethodFailed { name, .. } if name == ERROR_STORAGE));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_connection_makes_the_host_unavailable_not_a_panic() {
    let (blobs, connection) = service(Object::Blobs).await;
    drop(connection);
    let error = blobs.connection().err();
    assert_eq!(
        error,
        Some(HostFailure::Storage(StorageFailure {
            kind: StorageKind::Unavailable
        }))
    );
}
