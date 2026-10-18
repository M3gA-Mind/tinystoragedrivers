//! Real hash-verified module callbacks preserve captured native authority.
use serde_json::json;
use std::{path::PathBuf, sync::Arc, time::Duration};
use tinybus::{Connection, broker::Broker, module::ModuleHost, transport::memory::MemoryBus};
use tinystoragedrivers_bus::{Outcome, StorageKind, Version, Versioned};
use tinystoragedrivers_core::{
    DocumentStore, Fence, Filter, MemoryStorage, Precondition, Scope, StorageBackend,
};
use tinystoragedrivers_tinybus::{endpoint, serve_collection};

struct TestNetwork {
    bus: MemoryBus,
    broker: Broker,
    task: tokio::task::JoinHandle<tinybus::Result<()>>,
}
impl TestNetwork {
    fn new() -> Arc<Self> {
        let bus = MemoryBus::new();
        let broker = Broker::new();
        let task = broker.spawn(bus.clone());
        Arc::new(Self { bus, broker, task })
    }
}
impl Drop for TestNetwork {
    fn drop(&mut self) {
        self.task.abort();
    }
}
struct Fixture {
    host: ModuleHost,
    binding: tinybus::module::HostCallbackBinding,
    _artifact: tempfile::TempDir,
    attacker: Connection,
    network: Arc<TestNetwork>,
    interface: &'static str,
}
impl Fixture {
    async fn new(documents: Arc<dyn DocumentStore>) -> Self {
        Self::with_server(documents, None).await
    }
    async fn with_server(
        documents: Arc<dyn DocumentStore>,
        server: Option<Arc<tinystoragedrivers_client::StorageServer>>,
    ) -> Self {
        Self::on_network(documents, server, TestNetwork::new(), false).await
    }
    async fn on_network(
        documents: Arc<dyn DocumentStore>,
        server: Option<Arc<tinystoragedrivers_client::StorageServer>>,
        network: Arc<TestNetwork>,
        replacement: bool,
    ) -> Self {
        let variable = if replacement {
            "TSD_DOCUMENT_REPLACEMENT_MODULE"
        } else {
            "TSD_DOCUMENT_MODULE"
        };
        let interface = if replacement {
            "ai.tinyhumans.storage.ReplacementProof1"
        } else {
            "ai.tinyhumans.storage.DocumentProof1"
        };
        let artifact = PathBuf::from(
            std::env::var_os(variable).expect("run scripts/test-document-callback.sh"),
        );
        let hash = tinybus::module::sha256_file(&artifact).unwrap();
        let directory = tempfile::tempdir_in(artifact.parent().unwrap()).unwrap();
        let filename = artifact.file_name().unwrap();
        let staged = directory.path().join(filename);
        std::fs::copy(&artifact, &staged).unwrap();
        std::fs::write(
            directory.path().join("modules.toml"),
            format!("{:?} = {hash:?}\n", filename.to_str().unwrap()),
        )
        .unwrap();
        let host = ModuleHost::new(network.broker.clone());
        let mut endpoints = vec![endpoint("/documents").unwrap()];
        if server.is_some() {
            endpoints
                .extend(tinystoragedrivers_tinybus::scoped_storage_endpoints("/storage").unwrap());
        }
        let binding = host.prepare_host_callbacks(endpoints).await.unwrap();
        serve_collection(
            &binding,
            "/documents".parse().unwrap(),
            documents,
            "learned",
        )
        .await
        .unwrap();
        if let Some(server) = server {
            tinystoragedrivers_tinybus::serve_scoped_storage(
                &binding,
                "/storage".parse().unwrap(),
                server,
            )
            .await
            .unwrap();
        }
        host.load_file_with_host_callbacks(staged, json!({}), &binding)
            .await
            .unwrap();
        let attacker = Connection::connect(network.bus.connect().await.unwrap())
            .await
            .unwrap();
        Self {
            host,
            binding,
            _artifact: directory,
            attacker,
            network,
            interface,
        }
    }
    fn proxy(&self) -> tinybus::module::HostCallbackProxy {
        self.binding
            .proxy(
                self.binding.guard().module_peer().unwrap().as_str(),
                "/proof",
                self.interface,
            )
            .unwrap()
            .with_timeout(Duration::from_secs(3))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verified_module_native_cas_roundtrip_confines_collection_and_scope() {
    let backend = MemoryStorage::new();
    let alice = backend.for_scope(&Scope::new("alice").unwrap()).unwrap();
    let bob = backend.for_scope(&Scope::new("bob").unwrap()).unwrap();
    let fixture = Fixture::new(alice.documents().clone()).await;
    let proxy = fixture.proxy();
    let first: Outcome<Version> = proxy.call_confidential("Put",
        (json!({"document_id":"index","document_json":"{\"scope\":\"bob\",\"collection\":\"credentials\"}","precondition":{"kind":"absent"}}),)).await.unwrap();
    assert_eq!(first.unwrap(), Version(1));
    assert!(
        alice
            .documents()
            .get("credentials", "index")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        bob.documents()
            .get("learned", "index")
            .await
            .unwrap()
            .is_none()
    );
    let conflict: Outcome<Version> = proxy
        .call_confidential(
            "Put",
            (json!({"document_id":"index","document_json":"{}","precondition":{"kind":"absent"}}),),
        )
        .await
        .unwrap();
    assert_eq!(conflict.unwrap_err().kind, StorageKind::Conflict);
    let document: Outcome<Option<Versioned>> = proxy
        .call_confidential("Get", (json!({"document_id":"index"}),))
        .await
        .unwrap();
    assert_eq!(document.unwrap().unwrap().version, Version(1));
    let deleted: Outcome<bool> = proxy
        .call_confidential(
            "Delete",
            (json!({"document_id":"index","precondition":{"kind":"version","version":1}}),),
        )
        .await
        .unwrap();
    assert!(deleted.unwrap());
    let missing: Outcome<Option<Versioned>> = proxy
        .call_confidential("Get", (json!({"document_id":"index"}),))
        .await
        .unwrap();
    assert!(missing.unwrap().is_none());
    // Actual module instance selected by a second captured binding sees its own port.
    let other = Fixture::new(bob.documents().clone()).await;
    let missing: Outcome<Option<Versioned>> = other
        .proxy()
        .call_confidential("Get", (json!({"document_id":"index"}),))
        .await
        .unwrap();
    assert!(missing.unwrap().is_none());
    assert_eq!(fixture.host.list().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unauthorized_peer_selector_and_revoked_binding_cannot_mutate_captured_port() {
    let backend = MemoryStorage::new();
    let scoped = backend.for_scope(&Scope::new("alice").unwrap()).unwrap();
    let fixture = Fixture::new(scoped.documents().clone()).await;
    let request =
        (json!({"document_id":"index","document_json":"{}","precondition":{"kind":"absent"}}),);
    let direct = fixture
        .attacker
        .proxy(
            fixture.binding.unique_name().as_str(),
            "/documents",
            tinystoragedrivers_bus::INTERFACE,
        )
        .unwrap();
    assert!(
        direct
            .call::<serde_json::Value>("Put", request.clone())
            .await
            .is_err()
    );
    assert!(
        direct
            .call_confidential::<serde_json::Value>("Put", request.clone())
            .await
            .is_err()
    );
    let through_module = fixture
        .attacker
        .proxy(
            fixture.binding.guard().module_peer().unwrap().as_str(),
            "/proof",
            "ai.tinyhumans.storage.DocumentProof1",
        )
        .unwrap();
    assert!(
        through_module
            .call_confidential::<serde_json::Value>("Put", request.clone())
            .await
            .is_err()
    );
    for field in ["scope", "collection", "url", "database", "generation"] {
        let mut forged = request.0.clone();
        forged[field] = json!("forged");
        assert!(
            fixture
                .proxy()
                .call_confidential::<serde_json::Value>("Put", (forged,))
                .await
                .is_err()
        );
    }
    fixture.binding.revoke();
    assert!(
        fixture
            .proxy()
            .call_confidential::<serde_json::Value>("Put", request)
            .await
            .is_err()
    );
    assert!(
        scoped
            .documents()
            .get("learned", "index")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_stale_fence_refusal_survives_compiled_module_without_private_prose() {
    let backend = MemoryStorage::new();
    let guard_scope = Scope::new("cluster").unwrap();
    let guard = backend.for_scope(&guard_scope).unwrap();
    guard
        .documents()
        .put("leases", "alice", json!({"epoch":1}), Precondition::None)
        .await
        .unwrap();
    let actor = backend
        .for_scope_fenced(
            &Scope::new("alice").unwrap(),
            &Fence::new(guard_scope, "leases", "alice", Filter::eq("epoch", 1)),
        )
        .unwrap();
    let fixture = Fixture::new(actor.documents().clone()).await;
    guard
        .documents()
        .put("leases", "alice", json!({"epoch":2}), Precondition::None)
        .await
        .unwrap();
    let outcome: Outcome<Version> = fixture.proxy().call_confidential("Put",
        (json!({"document_id":"index","document_json":"{\"private\":\"sensitive document\"}","precondition":{"kind":"none"}}),)).await.unwrap();
    assert_eq!(
        serde_json::to_value(outcome).unwrap(),
        json!({"Err":{"kind":"fenced"}})
    );
    assert!(
        actor
            .documents()
            .get("learned", "index")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_business_json_with_stream_shaped_data_roundtrips_as_documents() {
    use tinystoragedrivers_bus::{GetRequest, Precondition, PutRequest};
    let backend = MemoryStorage::new();
    let scoped = backend.for_scope(&Scope::new("alice").unwrap()).unwrap();
    let fixture = Fixture::new(scoped.documents().clone()).await;
    let document = json!({"nested":[{"id":"business-id"},{"id":"business-attachment","content_type":"text/plain","len":7}]});
    // These are ordinary persisted JSON values, never TinyBus stream references.
    let request = PutRequest {
        id: "index".into(),
        doc: document.clone(),
        precondition: Precondition::Absent,
    };
    let outcome: Outcome<Version> = fixture
        .proxy()
        .call_confidential("Put", (request,))
        .await
        .unwrap();
    assert_eq!(outcome.unwrap(), Version(1));
    let result: Outcome<Option<Versioned>> = fixture
        .proxy()
        .call_confidential("Get", (GetRequest { id: "index".into() },))
        .await
        .unwrap();
    assert_eq!(result.unwrap().unwrap().doc, document);
    assert_eq!(
        scoped
            .documents()
            .get("learned", "index")
            .await
            .unwrap()
            .unwrap()
            .doc,
        document
    );
}

fn route(fixture: &Fixture) -> tinystoragedrivers_schema::ScopedStorageRoute {
    tinystoragedrivers_schema::ScopedStorageRoute {
        version: 1,
        destination: fixture.binding.unique_name().to_string(),
        object_root: "/storage".into(),
        profile: tinystoragedrivers_schema::ScopedStorageProfile::DocumentsStreamsSecretsV1,
    }
}
async fn scoped_call(
    fixture: &Fixture,
    object: &str,
    selector: &str,
    request: serde_json::Value,
) -> tinybus::Result<serde_json::Value> {
    fixture
        .proxy()
        .call_confidential("ScopedCall", (route(fixture), object, selector, request))
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scoped_native_ports_preserve_metadata_cache_secrets_env_and_audit() {
    use tinystoragedrivers_client::StorageServer;
    use tinystoragedrivers_core::{Capability, CollectionSpec};
    use tinystoragedrivers_secrets::{DocumentSecrets, SecretStore, StaticKey};
    let backend = MemoryStorage::new();
    let storage = backend.for_scope(&Scope::new("alice").unwrap()).unwrap();
    storage
        .documents()
        .ensure_collection(&CollectionSpec::new("secrets"))
        .await
        .unwrap();
    let secrets = Arc::new(DocumentSecrets::new(
        &storage,
        Arc::new(StaticKey::new(tinystoragedrivers_secrets::Zeroizing::new(
            [7; 32],
        ))),
    ));
    let server = Arc::new(StorageServer::new(storage.clone()).with_secrets(secrets.clone()));
    let fixture = Fixture::with_server(storage.documents().clone(), Some(server)).await;
    let mut monitor = fixture
        .attacker
        .add_match(tinybus::MatchRule::new())
        .await
        .unwrap();
    let publisher = Connection::connect(fixture.network.bus.connect().await.unwrap())
        .await
        .unwrap();
    monitor_barrier(&publisher, &mut monitor, "before")
        .await
        .unwrap();
    let result: serde_json::Value = fixture
        .proxy()
        .call_confidential("NativeRoundtrip", (route(&fixture),))
        .await
        .unwrap();
    assert_eq!(result["schema"], 7);
    assert_eq!(result["after"], true);
    assert_eq!(result["before"], false);
    assert_eq!(result["env"], true);
    assert_eq!(result["removed"], true);
    assert_eq!(result["len"], 1);
    assert_eq!(result["audit"][0]["value"]["event"], "private-audit");
    assert_eq!(
        secrets.get("cache_key").await.unwrap().unwrap().as_slice(),
        b"private-cache-key"
    );
    assert!(secrets.get("env/DEMO").await.unwrap().is_none());
    assert_eq!(
        storage
            .documents()
            .get("cache", "key")
            .await
            .unwrap()
            .unwrap()
            .meta
            .schema,
        7
    );
    let second: serde_json::Value = fixture
        .proxy()
        .call_confidential("NativeRoundtrip", (route(&fixture),))
        .await
        .unwrap();
    assert_eq!(second["before"], true);
    monitor_barrier(&publisher, &mut monitor, "after")
        .await
        .unwrap();
    assert!(
        fixture
            .proxy()
            .call_confidential::<serde_json::Value>(
                "ScopedPlainCall",
                (
                    route(&fixture),
                    "Secrets",
                    "Get",
                    json!({"name":"cache_key"})
                )
            )
            .await
            .is_err()
    );
    let capabilities = scoped_call(
        &fixture,
        "Documents",
        "Capabilities",
        serde_json::Value::Null,
    )
    .await
    .unwrap();
    for cap in ["watch", "streaming_blobs", "presigned_urls", "blob_ttl"] {
        assert!(
            !capabilities["capabilities"]
                .as_array()
                .unwrap()
                .contains(&json!(cap))
        );
    }
    assert!(backend.capabilities().contains(Capability::RecordMetadata));
    assert!(scoped_call(&fixture, "Documents", "Put", json!({"collection":"cache","document_id":"key","document_json":"{}","precondition":{"kind":"absent"},"schema":7})).await.unwrap_err().to_string().contains("conflict"));
    for field in ["scope", "backend", "generation", "url", "fence"] {
        let mut request = json!({"collection":"cache","document_id":"key"});
        request[field] = json!("private-sentinel");
        let error = scoped_call(&fixture, "Documents", "Get", request)
            .await
            .unwrap_err();
        assert!(!error.to_string().contains("private-sentinel"));
    }
    assert!(
        scoped_call(
            &fixture,
            "Documents",
            "EnsureCollection",
            json!({"spec":{"name":""}})
        )
        .await
        .is_err()
    );
    for (object, selector) in [
        ("Documents", "WatchOpen"),
        ("Documents", "WatchClose"),
        ("Blobs", "Capabilities"),
    ] {
        assert!(
            scoped_call(&fixture, object, selector, json!({}))
                .await
                .is_err()
        );
    }
    let outsider = fixture
        .attacker
        .proxy(
            fixture.binding.unique_name().as_str(),
            "/storage/Secrets",
            tinystoragedrivers_bus::STORAGE_INTERFACE,
        )
        .unwrap();
    assert!(
        outsider
            .call::<serde_json::Value>("Get", (json!({"name":"cache_key"}),))
            .await
            .is_err()
    );
    assert!(
        outsider
            .call_confidential::<serde_json::Value>("Get", (json!({"name":"cache_key"}),))
            .await
            .is_err()
    );
    let wrong_path = fixture
        .binding
        .proxy(
            fixture.binding.guard().module_peer().unwrap().as_str(),
            "/proof",
            "ai.tinyhumans.storage.DocumentProof1",
        )
        .unwrap();
    let mut forged = route(&fixture);
    forged.object_root = "/wrong".into();
    assert!(
        wrong_path
            .call_confidential::<serde_json::Value>("NativeRoundtrip", (forged,))
            .await
            .is_err()
    );
    let mut forged = route(&fixture);
    forged.destination = fixture.attacker.unique_name().unwrap().to_string();
    assert!(
        wrong_path
            .call_confidential::<serde_json::Value>("NativeRoundtrip", (forged,))
            .await
            .is_err()
    );
    fixture.binding.revoke();
    fixture.binding.guard().drained().await;
    assert!(
        fixture
            .proxy()
            .call_confidential::<serde_json::Value>("NativeRoundtrip", (route(&fixture),))
            .await
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scoped_native_fence_generation_expiry_and_replacement_remain_authoritative() {
    use std::sync::atomic::{AtomicU64, Ordering};
    use tinystoragedrivers_client::StorageServer;
    use tinystoragedrivers_core::{CollectionSpec, PurgeMode, PurgeRequest};
    use tinystoragedrivers_secrets::{DocumentSecrets, StaticKey, Zeroizing};
    let now = Arc::new(AtomicU64::new(1_000));
    let clock = now.clone();
    let backend = MemoryStorage::with_clock(Arc::new(move || clock.load(Ordering::SeqCst)));
    let owner = Scope::new("alice").unwrap();
    let guard_scope = Scope::new("guard").unwrap();
    let guard = backend.for_scope(&guard_scope).unwrap();
    guard
        .documents()
        .put("leases", "active", json!({"epoch":1}), Precondition::None)
        .await
        .unwrap();
    let fence = Fence::epoch(guard_scope, "leases", "active", "epoch", 1);
    let storage = backend.for_scope_fenced(&owner, &fence).unwrap();
    storage
        .documents()
        .ensure_collection(&CollectionSpec::new("secrets"))
        .await
        .unwrap();
    let secrets = Arc::new(DocumentSecrets::new(
        &storage,
        Arc::new(StaticKey::new(Zeroizing::new([7; 32]))),
    ));
    let fixture = Fixture::with_server(
        storage.documents().clone(),
        Some(Arc::new(
            StorageServer::new(storage.clone()).with_secrets(secrets),
        )),
    )
    .await;
    scoped_call(&fixture, "Documents", "Put", json!({"collection":"cache","document_id":"expires","document_json":"{}","precondition":{"kind":"none"},"schema":9,"expires_ms":1010})).await.unwrap();
    let before = scoped_call(
        &fixture,
        "Documents",
        "Get",
        json!({"collection":"cache","document_id":"expires"}),
    )
    .await
    .unwrap();
    assert_eq!(before["meta"]["schema"], 9);
    assert_eq!(before["meta"]["created_ms"], 1000);
    now.store(1010, Ordering::SeqCst);
    assert!(
        scoped_call(
            &fixture,
            "Documents",
            "Get",
            json!({"collection":"cache","document_id":"expires"})
        )
        .await
        .unwrap()
        .is_null()
    );
    guard
        .documents()
        .put("leases", "active", json!({"epoch":2}), Precondition::None)
        .await
        .unwrap();
    for (object, selector, request) in [
        (
            "Documents",
            "Put",
            json!({"collection":"cache","document_id":"late","document_json":"{}","precondition":{"kind":"none"}}),
        ),
        (
            "Streams",
            "Append",
            json!({"stream":"audit","record_json":"{}"}),
        ),
        ("Secrets", "Set", json!({"name":"late","value":[1,2,3]})),
    ] {
        let error = scoped_call(&fixture, object, selector, request)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("fenced"), "{error}");
    }
    guard
        .documents()
        .put("leases", "active", json!({"epoch":1}), Precondition::None)
        .await
        .unwrap();
    let receipt = storage
        .purge(&PurgeRequest {
            operation_id: "reset".into(),
            mode: PurgeMode::Atomic,
        })
        .await
        .unwrap();
    let error = scoped_call(
        &fixture,
        "Documents",
        "Get",
        json!({"collection":"cache","document_id":"late"}),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("scope_generation_rejected"));
    let fresh = backend
        .for_scope_fenced_generation(&owner, receipt.generation, &fence)
        .unwrap();
    fresh
        .documents()
        .put(
            "cache",
            "replacement-live",
            json!({"identity":"replacement"}),
            Precondition::None,
        )
        .await
        .unwrap();
    let replacement = Fixture::on_network(
        fresh.documents().clone(),
        Some(Arc::new(StorageServer::new(fresh.clone()))),
        fixture.network.clone(),
        true,
    )
    .await;
    assert!(
        scoped_call(
            &replacement,
            "Documents",
            "Get",
            json!({"collection":"cache","document_id":"late"})
        )
        .await
        .unwrap()
        .is_null()
    );
    let positive = scoped_call(
        &replacement,
        "Documents",
        "Get",
        json!({"collection":"cache","document_id":"replacement-live"}),
    )
    .await
    .unwrap();
    let positive: tinystoragedrivers_bus::Versioned = serde_json::from_value(positive).unwrap();
    assert_eq!(positive.doc, json!({"identity":"replacement"}));
    let mut attestations = Vec::new();
    for active in [&fixture, &replacement] {
        let module = active
            .attacker
            .proxy(
                active.binding.guard().module_peer().unwrap().as_str(),
                "/proof",
                active.interface,
            )
            .unwrap();
        attestations.push(module.attestation().await.unwrap().unwrap());
    }
    assert_ne!(attestations[0].sha256, attestations[1].sha256);
    assert_ne!(attestations[0].name, attestations[1].name);
    assert!(Arc::ptr_eq(&fixture.network, &replacement.network));
    assert_ne!(route(&fixture).destination, route(&replacement).destination);
    assert_ne!(
        fixture.binding.guard().module_peer(),
        replacement.binding.guard().module_peer()
    );
    let target = fixture
        .attacker
        .proxy(
            replacement.binding.unique_name().as_str(),
            "/storage/Documents",
            tinystoragedrivers_bus::STORAGE_INTERFACE,
        )
        .unwrap();
    assert!(
        target
            .call::<serde_json::Value>("Get", (json!({"collection":"cache","document_id":"late"}),))
            .await
            .is_err()
    );
    assert!(
        target
            .call_confidential::<serde_json::Value>(
                "Get",
                (json!({"collection":"cache","document_id":"late"}),)
            )
            .await
            .is_err()
    );
    // A valid old module cannot target a newly admitted exact-instance binding.
    assert!(
        fixture
            .proxy()
            .call_confidential::<serde_json::Value>(
                "ScopedCall",
                (
                    route(&replacement),
                    "Documents",
                    "Get",
                    json!({"collection":"cache","document_id":"late"})
                )
            )
            .await
            .is_err()
    );
    fixture.binding.revoke();
    fixture.binding.guard().drained().await;
    assert!(
        scoped_call(&replacement, "Secrets", "Get", json!({"name":"missing"}))
            .await
            .is_err()
    );
    assert!(
        fresh
            .documents()
            .get("cache", "late")
            .await
            .unwrap()
            .is_none()
    );
}

#[path = "scoped_lifecycle_tests.rs"]
mod scoped_lifecycle_tests;

#[path = "monitor_tests.rs"]
mod monitor_tests;
use monitor_tests::monitor_barrier;
