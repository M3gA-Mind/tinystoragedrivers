use super::*;
use crate::PerCallerEndpoint;
use crate::StorageServer;
use tinybus::{broker::Broker, transport::memory::MemoryBus};
use tinystoragedrivers_api::StorageBackend;
use tinystoragedrivers_bus::STORAGE_INTERFACE;
use tinystoragedrivers_testkit::MemoryBackend;
use tinystoragedrivers_types::Scope;

struct SignalEndpoint {
    signals: Mutex<Option<tokio::sync::broadcast::Sender<WatchChanged>>>,
    has_signals: bool,
}

#[async_trait::async_trait]
impl StorageEndpoint for SignalEndpoint {
    async fn call(&self, _call: StorageCall<'_>) -> Result<Value, HostFailure> {
        Err(HostFailure::Denied("test-endpoint"))
    }

    async fn put_blob_stream(
        &self,
        _sender: Option<&BusName>,
        _head: PutBlobStreamRequest,
        _body: tinystoragedrivers_api::BlobReader,
    ) -> Result<Value, HostFailure> {
        Err(HostFailure::Denied("test-endpoint"))
    }

    async fn get_blob_stream(
        &self,
        _sender: Option<&BusName>,
        _request: BlobKeyRequest,
    ) -> Result<Option<(BlobStreamHead, tinystoragedrivers_api::BlobReader)>, HostFailure> {
        Err(HostFailure::Denied("test-endpoint"))
    }

    fn watch_signals(
        &self,
        _sender: &BusName,
    ) -> Option<tokio::sync::broadcast::Receiver<WatchChanged>> {
        if self.has_signals {
            self.signals
                .lock()
                .ok()?
                .as_ref()
                .map(|signals| signals.subscribe())
        } else {
            None
        }
    }
}

fn signal_endpoint(
    has_signals: bool,
) -> (SignalEndpoint, tokio::sync::broadcast::Sender<WatchChanged>) {
    let (signals, _) = tokio::sync::broadcast::channel(8);
    (
        SignalEndpoint {
            signals: Mutex::new(Some(signals.clone())),
            has_signals,
        },
        signals,
    )
}

async fn service(object: Object) -> (ObjectService, Connection) {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let backend = MemoryBackend::new();
    let endpoint = PerCallerEndpoint::new(move |_| {
        Some(StorageServer::new(
            backend.for_scope(&Scope::new("local").unwrap()).unwrap(),
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

#[test]
fn host_failures_keep_only_the_denial_label_or_storage_kind_on_the_bus() {
    let denied: tinybus::Error = HostFailure::Denied("scope-not-authorized").into();
    assert!(matches!(
        denied,
        tinybus::Error::MethodFailed { name, message }
            if name == ERROR_DENIED && message == "scope-not-authorized"
    ));

    let storage: tinybus::Error = HostFailure::Storage(StorageFailure {
        kind: StorageKind::Unavailable,
    })
    .into();
    assert!(matches!(
        storage,
        tinybus::Error::MethodFailed { name, message }
            if name == ERROR_STORAGE && message == "unavailable"
    ));
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
async fn serve_claims_the_name_and_shutdown_withdraws_every_object() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let host = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let endpoint = PerCallerEndpoint::new(|_| None);
    let served = serve(&host, Arc::new(endpoint)).await.unwrap();
    assert!(format!("{served:?}").contains("ServedStorage"));
    assert!(host.name_owner(HOST_NAME).await.unwrap().is_some());
    served.shutdown().await.unwrap();
    assert!(host.name_owner(HOST_NAME).await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_taken_host_name_rolls_back_the_second_registration() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let first_host = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let second_host = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let first = serve(&first_host, Arc::new(PerCallerEndpoint::new(|_| None)))
        .await
        .unwrap();
    let second = serve(&second_host, Arc::new(PerCallerEndpoint::new(|_| None))).await;
    assert!(second.is_err());
    assert_eq!(
        first_host.name_owner(HOST_NAME).await.unwrap(),
        Some(first_host.unique_name().unwrap())
    );
    first.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_object_registration_does_not_claim_the_host_name() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let host = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let observer = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    host.close().await.unwrap();

    let result = serve(&host, Arc::new(PerCallerEndpoint::new(|_| None))).await;

    assert!(result.is_err());
    assert_eq!(observer.name_owner(HOST_NAME).await.unwrap(), None);
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
    // A well-formed upload without a proven sender is refused before any
    // stream is accepted: the stream must be bound to its opener.
    let error = blobs
        .call_with_context(
            &member("PutStream"),
            json!([{"key": "k", "len": 1}, {"id": "no-such-stream", "len": 1}]),
            &context,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(&error, tinybus::Error::MethodFailed { name, message }
        if name == ERROR_DENIED && message == "unauthenticated-caller")
    );
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

#[tokio::test(flavor = "multi_thread")]
async fn a_released_and_rebound_caller_gets_a_fresh_signal_forwarder() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let backend = MemoryBackend::new();
    let endpoint = Arc::new(PerCallerEndpoint::new(move |_| {
        Some(StorageServer::new(
            backend.for_scope(&Scope::new("local").unwrap()).unwrap(),
        ))
    }));
    let shared = Shared {
        connection: connection.downgrade(),
        endpoint: Arc::clone(&endpoint) as Arc<dyn StorageEndpoint>,
        forwarders: Mutex::new(HashMap::new()),
    };
    let caller = BusName::new(":1.7").unwrap();
    let epoch_of = || {
        shared
            .forwarders
            .lock()
            .unwrap()
            .get(&caller)
            .map(|(e, _)| *e)
    };

    shared.forward_watch_signals(&caller);
    let first = epoch_of().expect("a forwarder is started");
    shared.forward_watch_signals(&caller);
    assert_eq!(
        epoch_of(),
        Some(first),
        "the same binding keeps its forwarder"
    );

    assert!(endpoint.release(&caller));
    shared.forward_watch_signals(&caller);
    let second = epoch_of().expect("the rebound caller is forwarded");
    assert_ne!(first, second, "the stale forwarder is replaced on rebind");
    for (_, (_, task)) in shared.forwarders.lock().unwrap().drain() {
        task.abort();
    }
}

#[tokio::test]
async fn watch_signals_are_forwarded_only_to_the_bound_caller() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let host = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let caller = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let observer = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let caller_name = caller.unique_name().unwrap();
    let path = watch_path(&caller_name).unwrap();
    let rule = tinybus::MatchRule {
        kind: Some(tinybus::MessageKind::Signal),
        sender: None,
        interface: Some(interface()),
        member: Some(member(WATCH_MEMBER)),
        path: Some(path.clone()),
        path_namespace: None,
    };
    let mut caller_signals = caller.add_match(rule.clone()).await.unwrap();
    let mut observer_signals = observer.add_match(rule).await.unwrap();
    let (endpoint, sender) = signal_endpoint(true);
    assert_eq!(endpoint.watch_epoch(&caller_name), 0);
    let shared = Shared {
        connection: host.downgrade(),
        endpoint: Arc::new(endpoint),
        forwarders: Mutex::new(HashMap::new()),
    };

    shared.forward_watch_signals(&caller_name);
    for index in 0..16 {
        sender
            .send(WatchChanged {
                watch_id: format!("watch-{index}"),
                notice: tinystoragedrivers_bus::WatchNotice::Invalidated { cursor: None },
            })
            .unwrap();
    }

    let message = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let message = caller_signals.recv().await.unwrap();
            if message.body[0]["watch_id"] == "watch-15" {
                break message;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(message.header.path, Some(path));
    assert_eq!(
        message.header.member.as_ref().map(MemberName::as_str),
        Some(WATCH_MEMBER)
    );
    assert_eq!(message.body[0]["watch_id"], "watch-15");
    observer.name_owner(HOST_NAME).await.unwrap();
    assert!(observer_signals.try_recv().is_err());
    for (_, (_, task)) in shared.forwarders.lock().unwrap().drain() {
        task.abort();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_or_closed_watch_source_does_not_leave_a_live_forwarder() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let caller = BusName::new(":1.7").unwrap();
    let (missing, _) = signal_endpoint(false);
    let shared = Shared {
        connection: connection.downgrade(),
        endpoint: Arc::new(missing),
        forwarders: Mutex::new(HashMap::new()),
    };
    shared.forward_watch_signals(&caller);
    assert!(shared.forwarders.lock().unwrap().is_empty());

    let (closed, sender) = signal_endpoint(true);
    let closed = Arc::new(closed);
    let shared = Shared {
        connection: connection.downgrade(),
        endpoint: Arc::clone(&closed) as Arc<dyn StorageEndpoint>,
        forwarders: Mutex::new(HashMap::new()),
    };
    shared.forward_watch_signals(&caller);
    drop(sender);
    closed.signals.lock().unwrap().take();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            shared.forward_watch_signals(&caller);
            if shared.forwarders.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("a closed watch source is removed from the forwarding table");
}
