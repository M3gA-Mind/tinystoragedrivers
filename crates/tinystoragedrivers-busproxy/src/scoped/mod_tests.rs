//! Invalid routing and absent capabilities fail closed without public payloads.
use super::*;
use tinybus::{broker::Broker, transport::memory::MemoryBus};
async fn connection() -> (Connection, tokio::task::JoinHandle<tinybus::Result<()>>) {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let task = broker.spawn(bus.clone());
    (
        Connection::connect(bus.connect().await.unwrap())
            .await
            .unwrap(),
        task,
    )
}
fn route() -> ScopedStorageRoute {
    ScopedStorageRoute {
        version: 1,
        destination: ":1.999".into(),
        object_root: "/storage".into(),
        profile: ScopedStorageProfile::DocumentsStreamsSecretsV1,
    }
}
#[tokio::test]
async fn invalid_unavailable_and_well_known_routes_fail_statically() {
    let (connection, task) = connection().await;
    let mut bad = route();
    bad.version = 2;
    let mut well_known = route();
    well_known.destination = crate::HOST_NAME.into();
    let mut bad_name = route();
    bad_name.destination = "private-invalid-destination".into();
    let mut bad_path = route();
    bad_path.object_root = "private-invalid-path".into();
    for route in [bad, well_known, bad_name, bad_path] {
        let error = connect_scoped_storage(connection.clone(), route)
            .await
            .unwrap_err();
        assert_eq!(
            error.kind(),
            tinystoragedrivers_core::ErrorKind::InvalidInput
        );
        assert!(!error.to_string().contains("private-"));
    }
    let error = connect_scoped_storage(connection.clone(), route())
        .await
        .unwrap_err();
    assert_eq!(
        error.kind(),
        tinystoragedrivers_core::ErrorKind::Unavailable
    );
    let (signals, _) = broadcast::channel(1);
    let transport = ScopedTransport {
        connection,
        destination: ":1.999".parse().unwrap(),
        root: "/".parse().unwrap(),
        signals,
    };
    assert!(!format!("{transport:?}").contains(":1.999"));
    let _signals = transport.signals();
    for (object, selector) in [
        (Object::Blobs, "Get"),
        (Object::Documents, "WatchOpen"),
        (Object::Documents, "WatchClose"),
    ] {
        assert_eq!(
            transport
                .call(object, selector, Value::Null)
                .await
                .unwrap_err(),
            unsupported()
        );
    }
    assert_eq!(
        transport
            .get_blob_stream(BlobKeyRequest {
                key: "private".into()
            })
            .await
            .err()
            .unwrap(),
        unsupported()
    );
    let head = PutBlobStreamRequest {
        key: "private".into(),
        len: 0,
        ttl_ms: None,
        content_type: None,
    };
    assert_eq!(
        transport
            .put_blob_stream(head, Box::pin(std::io::Cursor::new(Vec::<u8>::new())))
            .await
            .unwrap_err(),
        unsupported()
    );
    task.abort();
}
#[test]
fn callback_errors_keep_only_static_storage_categories() {
    for kind in [
        "conflict",
        "fenced",
        "scope_generation_rejected",
        "invalid_input",
    ] {
        assert!(matches!(
            scoped_wire_error(tinybus::Error::MethodFailed {
                name: crate::ERROR_STORAGE.into(),
                message: kind.into()
            }),
            WireError::Storage(_)
        ));
    }
    assert_eq!(
        scoped_wire_error(tinybus::Error::MethodFailed {
            name: crate::ERROR_STORAGE.into(),
            message: "private".into()
        }),
        WireError::Storage(StorageFailure {
            kind: StorageKind::Backend
        })
    );
    assert_eq!(
        scoped_wire_error(tinybus::Error::failed("private")),
        WireError::Transport("storage callback unavailable".into())
    );
}
