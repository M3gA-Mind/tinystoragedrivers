use super::*;
use tinybus::{broker::Broker, transport::memory::MemoryBus};

#[tokio::test]
async fn upload_length_failure_wins_when_peer_abort_and_short_source_are_ready() {
    let reply = async {
        Err(WireError::Storage(StorageFailure {
            kind: StorageKind::Unavailable,
        }))
    };
    let sent = async { Err(PumpError::Short) };

    let result = resolve_put_stream(reply, sent).await;

    assert!(matches!(
        result,
        Err(WireError::Storage(StorageFailure {
            kind: StorageKind::InvalidInput
        }))
    ));
}

#[tokio::test]
async fn upload_source_failure_wins_when_peer_abort_and_source_error_are_ready() {
    let reply = async {
        Err(WireError::Storage(StorageFailure {
            kind: StorageKind::Unavailable,
        }))
    };
    let sent = async { Err(PumpError::Source) };

    let result = resolve_put_stream(reply, sent).await;

    assert!(matches!(
        result,
        Err(WireError::Storage(StorageFailure {
            kind: StorageKind::Backend
        }))
    ));
}

#[tokio::test]
async fn host_refusal_wins_when_abort_and_typed_reply_are_ready() {
    let reply = async {
        Err(WireError::Storage(StorageFailure {
            kind: StorageKind::Unsupported,
        }))
    };
    let sent = async { Err(PumpError::Bus(tinybus::Error::ConnectionClosed)) };

    let result = resolve_put_stream(reply, sent).await;

    assert_eq!(
        result,
        Err(WireError::Storage(StorageFailure {
            kind: StorageKind::Unsupported
        }))
    );
}

#[tokio::test]
async fn invalid_input_reply_wins_when_upload_abort_is_also_ready() {
    let reply = async {
        Err(WireError::Storage(StorageFailure {
            kind: StorageKind::InvalidInput,
        }))
    };
    let sent = async { Err(PumpError::Bus(tinybus::Error::ConnectionClosed)) };

    assert_eq!(
        resolve_put_stream(reply, sent).await,
        Err(WireError::Storage(StorageFailure {
            kind: StorageKind::InvalidInput
        }))
    );
}

#[tokio::test]
async fn bus_failure_does_not_wait_for_a_pending_host_reply() {
    let reply = std::future::pending();
    let sent = async { Err(PumpError::Bus(tinybus::Error::ConnectionClosed)) };

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        resolve_put_stream(reply, sent),
    )
    .await;

    assert!(matches!(result, Ok(Err(WireError::Transport(_)))));
}

#[tokio::test]
async fn successful_host_reply_does_not_hide_a_later_bus_failure() {
    let reply = async { Ok(json!({ "stored": true })) };
    let sent = async {
        tokio::task::yield_now().await;
        Err(PumpError::Bus(tinybus::Error::ConnectionClosed))
    };

    assert!(matches!(
        resolve_put_stream(reply, sent).await,
        Err(WireError::Transport(_))
    ));
}

#[tokio::test]
async fn successful_upload_returns_host_reply_after_pump_completes() {
    let reply = async { Ok(json!({ "stored": true })) };
    let sent = async {
        tokio::task::yield_now().await;
        Ok(())
    };

    assert_eq!(
        resolve_put_stream(reply, sent).await,
        Ok(json!({ "stored": true }))
    );
}

#[tokio::test]
async fn successful_host_reply_does_not_hide_a_later_upload_failure() {
    let reply = async { Ok(json!({ "stored": true })) };
    let sent = async {
        tokio::task::yield_now().await;
        Err(PumpError::Short)
    };

    let result = resolve_put_stream(reply, sent).await;

    assert!(matches!(
        result,
        Err(WireError::Storage(StorageFailure {
            kind: StorageKind::InvalidInput
        }))
    ));
}

#[tokio::test]
async fn host_refusal_does_not_wait_for_a_pending_upload_source() {
    let reply = async {
        Err(WireError::Storage(StorageFailure {
            kind: StorageKind::Unsupported,
        }))
    };
    let sent = std::future::pending();

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        resolve_put_stream(reply, sent),
    )
    .await;

    assert!(matches!(
        result,
        Ok(Err(WireError::Storage(StorageFailure {
            kind: StorageKind::Unsupported
        })))
    ));
}

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
    assert_eq!(
        wire_error(tinybus::Error::NotAttested {
            name: HOST_NAME.parse().unwrap(),
            reason: "only a loaded module with a verified artifact may receive a secret",
        }),
        WireError::NotAttested
    );
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

#[tokio::test(flavor = "multi_thread")]
async fn connected_transport_supports_timeout_configuration_and_debug_redaction() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let host = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let endpoint = crate::PerCallerEndpoint::new(|_| {
        Some(crate::StorageServer::new(
            tinystoragedrivers_testkit::memory_storage(
                &tinystoragedrivers_types::Scope::new("transport-unit").unwrap(),
            ),
        ))
    });
    let served = crate::serve(&host, Arc::new(endpoint)).await.unwrap();
    let module = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let recipient = module.unique_name().unwrap();
    let transport = BusTransport::connect(module)
        .await
        .unwrap()
        .with_timeout(Duration::from_secs(2));
    assert!(format!("{transport:?}").contains("BusTransport"));
    let mut signals = transport.signals();
    let notice = WatchChanged {
        watch_id: "transport-watch".into(),
        notice: tinystoragedrivers_bus::WatchNotice::Invalidated {
            cursor: Some(vec![3, 1, 4]),
        },
    };
    host.emit_to(
        recipient.clone(),
        watch_path(&recipient).unwrap(),
        interface(),
        member(WATCH_MEMBER),
        (notice.clone(),),
    )
    .await
    .unwrap();
    assert_eq!(signals.recv().await.unwrap(), notice);
    let reply = transport
        .call(Object::Kv, "Capabilities", Value::Null)
        .await
        .unwrap();
    assert!(reply["capabilities"].is_array());
    drop(transport);
    served.shutdown().await.unwrap();
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
