//! Real hashed and admitted TinyBus module storage callback proofs.
use std::{path::PathBuf, sync::Arc, time::Duration};

use async_trait::async_trait;
use serde_json::{Value, json};
use tinybus::{
    BusName, Connection,
    broker::Broker,
    module::{HostCallbackBinding, ModuleHost},
    transport::memory::MemoryBus,
};
use tinystoragedrivers_api::{BlobReader, Storage, StorageBackend};
use tinystoragedrivers_bus::{STORAGE_INTERFACE, WatchChanged};
use tinystoragedrivers_busproxy::{
    HostFailure, Object, PerCallerEndpoint, ServedStorage, StorageCall, StorageEndpoint,
    StorageServer, connect_storage_at, object_path, prepare_storage_host_callbacks,
    serve_with_host_callbacks,
};
use tinystoragedrivers_connect::MongoConnection;
use tinystoragedrivers_document::mongodb::{MongoDocuments, MongoStreams};
use tinystoragedrivers_testkit::MemoryBackend;
use tinystoragedrivers_types::{Precondition, Scope};
use tokio::sync::Notify;

struct ObservedEndpoint<F> {
    inner: PerCallerEndpoint<F>,
    watch_opened: Arc<Notify>,
}

#[async_trait]
impl<F> StorageEndpoint for ObservedEndpoint<F>
where
    F: Fn(&BusName) -> Option<StorageServer> + Send + Sync + 'static,
{
    async fn call(&self, call: StorageCall<'_>) -> Result<Value, HostFailure> {
        self.inner.call(call).await
    }

    async fn put_blob_stream(
        &self,
        sender: Option<&BusName>,
        head: tinystoragedrivers_bus::PutBlobStreamRequest,
        body: BlobReader,
    ) -> Result<Value, HostFailure> {
        self.inner.put_blob_stream(sender, head, body).await
    }

    async fn get_blob_stream(
        &self,
        sender: Option<&BusName>,
        request: tinystoragedrivers_bus::BlobKeyRequest,
    ) -> Result<Option<(tinystoragedrivers_bus::BlobStreamHead, BlobReader)>, HostFailure> {
        self.inner.get_blob_stream(sender, request).await
    }

    fn watch_signals(
        &self,
        sender: &BusName,
    ) -> Option<tokio::sync::broadcast::Receiver<WatchChanged>> {
        let receiver = self.inner.watch_signals(sender);
        if receiver.is_some() {
            self.watch_opened.notify_one();
        }
        receiver
    }

    fn watch_epoch(&self, sender: &BusName) -> u64 {
        self.inner.watch_epoch(sender)
    }
}

struct Fixture {
    binding: Arc<HostCallbackBinding>,
    _host: ModuleHost,
    storage: tinystoragedrivers_api::Storage,
    mongo: MongoConnection,
    database: String,
    client: Connection,
    attacker: Connection,
    watch_opened: Arc<Notify>,
    _artifact_dir: tempfile::TempDir,
    _served: ServedStorage,
    _broker_task: tokio::task::JoinHandle<tinybus::Result<()>>,
}

impl Fixture {
    async fn new() -> Self {
        let artifact = PathBuf::from(
            std::env::var_os("TSD_STORAGE_CALLBACK_MODULE")
                .expect("run scripts/test-storage-callback.sh"),
        );
        let hash = tinybus::module::sha256_file(&artifact).unwrap();
        let artifact_dir = tempfile::tempdir_in(artifact.parent().unwrap()).unwrap();
        let staged = artifact_dir.path().join(artifact.file_name().unwrap());
        std::fs::copy(&artifact, &staged).unwrap();
        std::fs::write(
            artifact_dir.path().join("modules.toml"),
            format!(
                "{:?} = {hash:?}\n",
                staged.file_name().unwrap().to_str().unwrap()
            ),
        )
        .unwrap();

        let scope = Scope::new("callback-proof").unwrap();
        let backend = Arc::new(MemoryBackend::new());
        let memory = backend.for_scope(&scope).unwrap();
        let uri =
            std::env::var("TSD_MONGO_URL").expect("TSD_MONGO_URL must point to local MongoDB");
        let database = format!(
            "tsd_callback_proof_{}_{}",
            std::process::id(),
            NEXT_DATABASE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        let mongo = MongoConnection::connect(&uri, &database).await.unwrap();
        let storage = Storage::new(
            Arc::new(MongoDocuments::new(mongo.clone(), scope.clone())),
            Arc::new(MongoStreams::new(mongo.clone(), scope.clone())),
            Arc::clone(memory.kv()),
            Arc::clone(memory.blobs()),
            Arc::clone(memory.queues()),
            Arc::clone(memory.secrets()),
        );
        storage
            .blobs()
            .put("host-download", b"ordinary host stream".to_vec(), None)
            .await
            .unwrap();
        let endpoint_storage = storage.clone();
        let inner =
            PerCallerEndpoint::new(move |_| Some(StorageServer::new(endpoint_storage.clone())));
        let watch_opened = Arc::new(Notify::new());
        let endpoint = Arc::new(ObservedEndpoint {
            inner,
            watch_opened: Arc::clone(&watch_opened),
        });

        let bus = MemoryBus::new();
        let broker = Broker::new();
        let broker_task = broker.spawn(bus.clone());
        let host = ModuleHost::new(broker);
        let binding = prepare_storage_host_callbacks(&host).await.unwrap();
        let served = serve_with_host_callbacks(Arc::clone(&binding), endpoint)
            .await
            .unwrap();
        host.load_file_with_host_callbacks(
            staged,
            json!({"host": binding.unique_name()}),
            &binding,
        )
        .await
        .unwrap();
        let client = Connection::connect(bus.connect().await.unwrap())
            .await
            .unwrap();
        let attacker = Connection::connect(bus.connect().await.unwrap())
            .await
            .unwrap();
        Self {
            binding,
            _host: host,
            storage,
            mongo,
            database,
            client,
            attacker,
            watch_opened,
            _artifact_dir: artifact_dir,
            _served: served,
            _broker_task: broker_task,
        }
    }

    fn module(&self) -> tinybus::Proxy {
        self.client
            .proxy(
                "org.example.StorageCallbackProof",
                "/storage_proof",
                "org.example.StorageCallbackProof",
            )
            .unwrap()
            .with_timeout(Duration::from_secs(5))
    }

    async fn cleanup(self) {
        self.mongo
            .client()
            .database(&self.database)
            .drop()
            .await
            .unwrap();
    }
}

static NEXT_DATABASE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admitted_module_uses_all_six_ports_with_confidential_secret_cas_and_streams() {
    let fixture = Fixture::new().await;
    let result: Value = fixture.module().call("Exercise", ()).await.unwrap();
    assert_eq!(result["document"], json!({"origin":"module"}));
    assert_eq!(result["kv"], json!({"value":7}));
    assert_eq!(result["kv_names"], 1);
    assert_eq!(result["stream_rows"], 1);
    assert_eq!(result["queue_deliveries"], 1);
    assert_eq!(result["upload"], json!("ordinary module stream"));
    assert_eq!(result["download"], json!("ordinary host stream"));
    assert_eq!(result["secret_updated"], true);
    assert_eq!(result["secret_names"], json!(["callback-proof/token"]));

    let uploaded = fixture
        .storage
        .blobs()
        .get("module-upload")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(uploaded.bytes, b"ordinary module stream");
    let (secret, _version) = fixture
        .storage
        .secrets()
        .get_versioned("callback-proof/token")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(secret.as_slice(), b"value-two");
    assert_eq!(secret.as_slice(), b"value-two");
    fixture.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn callback_watch_is_delivered_to_module_and_not_another_peer() {
    let fixture = Fixture::new().await;
    fixture
        .storage
        .documents()
        .ensure_collection(&tinystoragedrivers_types::CollectionSpec::new(
            "callback-proof",
        ))
        .await
        .unwrap();
    let mut outsider_signal = fixture
        .client
        .proxy(
            fixture.binding.unique_name().as_str(),
            object_path(Object::Documents).as_str(),
            STORAGE_INTERFACE,
        )
        .unwrap()
        .receive_directed_signal("WatchChanged")
        .await
        .unwrap();
    fixture
        .module()
        .call::<Value>("OpenWatch", ())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), fixture.watch_opened.notified())
        .await
        .expect("watch server must be registered before mutation");

    fixture
        .storage
        .documents()
        .put(
            "callback-proof",
            "host-change",
            json!({"origin":"host"}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    let delivered: bool = tokio::time::timeout(
        Duration::from_secs(3),
        fixture.module().call("ReadWatch", ()),
    )
    .await
    .expect("module should receive the invalidation")
    .unwrap();
    assert!(delivered);
    assert!(outsider_signal.try_recv().is_err());
    fixture.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unbound_peer_and_revoked_module_cannot_use_confidential_secret_callbacks() {
    let fixture = Fixture::new().await;
    let outsider = connect_storage_at(
        fixture.attacker.clone(),
        fixture.binding.unique_name().clone(),
    )
    .await;
    assert!(
        outsider.is_err(),
        "an unbound peer must not obtain callback storage"
    );
    assert!(
        fixture
            .storage
            .secrets()
            .list("callback-proof/")
            .await
            .unwrap()
            .is_empty()
    );

    fixture
        .module()
        .call::<Value>("Exercise", ())
        .await
        .unwrap();
    fixture.binding.revoke();
    assert!(
        fixture
            .module()
            .call::<Value>("Exercise", ())
            .await
            .is_err()
    );
    let names = fixture
        .storage
        .secrets()
        .list("callback-proof/")
        .await
        .unwrap();
    assert_eq!(names, ["callback-proof/token"]);
    fixture.cleanup().await;
}
