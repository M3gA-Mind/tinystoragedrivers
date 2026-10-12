//! Real hash-verified module callbacks preserve captured native authority.
use serde_json::json;
use std::{path::PathBuf, sync::Arc, time::Duration};
use tinybus::{Connection, broker::Broker, module::ModuleHost, transport::memory::MemoryBus};
use tinystoragedrivers_bus::{Outcome, StorageKind, Version, Versioned};
use tinystoragedrivers_core::{
    DocumentStore, Fence, Filter, MemoryStorage, Precondition, Scope, StorageBackend,
};
use tinystoragedrivers_tinybus::{endpoint, serve_collection};

struct Fixture {
    host: ModuleHost,
    binding: tinybus::module::HostCallbackBinding,
    _artifact: tempfile::TempDir,
    attacker: Connection,
    broker_task: tokio::task::JoinHandle<tinybus::Result<()>>,
}
impl Fixture {
    async fn new(documents: Arc<dyn DocumentStore>) -> Self {
        let artifact = PathBuf::from(
            std::env::var_os("TSD_DOCUMENT_MODULE").expect("run scripts/test-document-callback.sh"),
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
        let bus = MemoryBus::new();
        let broker = Broker::new();
        let broker_task = broker.spawn(bus.clone());
        let host = ModuleHost::new(broker);
        let binding = host
            .prepare_host_callbacks(vec![endpoint("/documents").unwrap()])
            .await
            .unwrap();
        serve_collection(
            &binding,
            "/documents".parse().unwrap(),
            documents,
            "learned",
        )
        .await
        .unwrap();
        host.load_file_with_host_callbacks(staged, json!({}), &binding)
            .await
            .unwrap();
        let attacker = Connection::connect(bus.connect().await.unwrap())
            .await
            .unwrap();
        Self {
            host,
            binding,
            _artifact: directory,
            attacker,
            broker_task,
        }
    }
    fn proxy(&self) -> tinybus::module::HostCallbackProxy {
        self.binding
            .proxy(
                self.binding.guard().module_peer().unwrap().as_str(),
                "/proof",
                "ai.tinyhumans.storage.DocumentProof1",
            )
            .unwrap()
            .with_timeout(Duration::from_secs(3))
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.broker_task.abort();
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
