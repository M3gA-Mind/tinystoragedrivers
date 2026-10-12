use super::*;
use tinybus::{broker::Broker, transport::memory::MemoryBus};

#[test]
fn stream_replies_are_decoded_strictly() {
    assert!(decode_stream_reply(Value::Null).unwrap().is_none());
    let head = json!({"meta": {"key": "k", "len": 3}});
    let stream = json!({"id": "s1", "len": 3});
    let (decoded, reference) = decode_stream_reply(json!([head, stream])).unwrap().unwrap();
    assert_eq!(decoded.meta.len, 3);
    assert_eq!(reference.id, "s1");
    for malformed in [
        json!("nope"),
        json!([]),
        json!([head]),
        json!([head, stream, 1]),
        json!([{"bad": 1}, stream]),
        json!([head, {"bad": 1}]),
    ] {
        let error = decode_stream_reply(malformed).unwrap_err();
        assert_eq!(
            error,
            WireError::Transport("malformed GetStream reply".into())
        );
    }
}

#[test]
fn remote_failures_keep_their_category_and_other_errors_become_transport() {
    let storage = tinybus::Error::MethodFailed {
        name: ERROR_STORAGE.into(),
        message: "conflict".into(),
    };
    assert_eq!(
        wire_error(storage),
        WireError::Storage(StorageFailure {
            kind: StorageKind::Conflict
        })
    );
    let unknown_kind = tinybus::Error::MethodFailed {
        name: ERROR_STORAGE.into(),
        message: "from-the-future".into(),
    };
    assert_eq!(
        wire_error(unknown_kind),
        WireError::Storage(StorageFailure {
            kind: StorageKind::Backend
        })
    );
    let denied = tinybus::Error::MethodFailed {
        name: ERROR_DENIED.into(),
        message: "no-grant-declared".into(),
    };
    assert_eq!(
        wire_error(denied),
        WireError::Transport("the storage host denied the call: no-grant-declared".into())
    );
    assert!(matches!(
        wire_error(tinybus::Error::ConnectionClosed),
        WireError::Transport(_)
    ));
}

#[test]
fn watch_signals_must_be_a_single_well_formed_notice() {
    let signal = |body: Value| {
        Message::signal(
            tinybus::ObjectPath::new("/a").unwrap(),
            interface(),
            member(WATCH_MEMBER),
            body,
        )
    };
    let notice = json!({"watch_id": "w1", "notice": {"kind": "invalidated"}});
    let decoded = decode_signal(&signal(json!([notice.clone()]))).unwrap();
    assert_eq!(decoded.watch_id, "w1");
    assert!(decode_signal(&signal(notice.clone())).is_none());
    assert!(decode_signal(&signal(json!([notice.clone(), notice]))).is_none());
    assert!(decode_signal(&signal(json!([{"bad": 1}]))).is_none());
}

/// A host-less broker with one module connection attached to a transport.
#[tokio::test(flavor = "multi_thread")]
async fn a_transport_without_a_host_cannot_be_built_and_debug_hides_the_wire() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let lonely = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let error = BusTransport::connect(lonely).await.unwrap_err();
    assert!(matches!(error, tinybus::Error::NameHasNoOwner(_)));
}
