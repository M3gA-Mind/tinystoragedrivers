//! Loopback S3 protocol tests for version-aware deletion and generation isolation.

use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use futures_util::TryStreamExt;
use http::Method;
use object_store::{
    ObjectStore,
    memory::InMemory,
    path::Path,
    signer::{SignedUrlOptions, Signer},
};
use tinystoragedrivers_core::{
    BlobBackend, CompositeStorage, MemoryStorage, PurgeMode, PurgeRequest, Scope, ScopeGeneration,
    StorageBackend, async_trait,
};
use tinystoragedrivers_s3::S3Storage;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
};
use url::Url;

#[derive(Debug)]
struct LoopbackSigner(String);

#[derive(Debug)]
struct LeakyFailureSigner;

#[async_trait]
impl Signer for LoopbackSigner {
    async fn signed_url(
        &self,
        _method: Method,
        path: &Path,
        _expires_in: Duration,
    ) -> object_store::Result<Url> {
        Url::parse(&format!("{}{path}", self.0)).map_err(|error| object_store::Error::Generic {
            store: "loopback signer",
            source: Box::new(error),
        })
    }

    async fn signed_url_opts(
        &self,
        _method: Method,
        path: &Path,
        _expires_in: Duration,
        options: &SignedUrlOptions,
    ) -> object_store::Result<Url> {
        let mut url = self
            .signed_url(Method::GET, path, Duration::from_secs(1))
            .await?;
        url.query_pairs_mut().extend_pairs(&options.extra_query);
        Ok(url)
    }
}

#[async_trait]
impl Signer for LeakyFailureSigner {
    async fn signed_url(
        &self,
        _method: Method,
        _path: &Path,
        _expires_in: Duration,
    ) -> object_store::Result<Url> {
        Err(leaky_signing_error())
    }

    async fn signed_url_opts(
        &self,
        _method: Method,
        _path: &Path,
        _expires_in: Duration,
        _options: &SignedUrlOptions,
    ) -> object_store::Result<Url> {
        Err(leaky_signing_error())
    }
}

fn leaky_signing_error() -> object_store::Error {
    object_store::Error::Generic {
        store: "test signer",
        source: Box::new(std::io::Error::other(
            "https://user:secret@example.invalid/?X-Amz-Signature=secret",
        )),
    }
}

#[derive(Default)]
struct FixtureState {
    objects: Option<Arc<InMemory>>,
    pages: Mutex<VecDeque<String>>,
    get_requests: Mutex<Vec<String>>,
    delete_requests: Mutex<Vec<String>>,
    pause_first_delete: AtomicBool,
    delete_entered: Notify,
    release_delete: Notify,
}

struct Fixture {
    base: String,
    state: Arc<FixtureState>,
    task: tokio::task::JoinHandle<()>,
}

impl Fixture {
    async fn new(state: FixtureState) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let state = Arc::new(state);
        let task_state = Arc::clone(&state);
        let task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(handle_request(stream, Arc::clone(&task_state)));
            }
        });
        Self {
            base: format!("http://{address}/"),
            state,
            task,
        }
    }

    fn signer(&self) -> Arc<dyn Signer> {
        Arc::new(LoopbackSigner(self.base.clone()))
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn handle_request(mut stream: TcpStream, state: Arc<FixtureState>) {
    let mut request = Vec::new();
    let mut chunk = [0; 2048];
    while !request.windows(4).any(|window| window == b"\r\n\r\n") {
        let Ok(read) = stream.read(&mut chunk).await else {
            return;
        };
        if read == 0 {
            return;
        }
        request.extend_from_slice(&chunk[..read]);
    }
    let Ok(text) = std::str::from_utf8(&request) else {
        return;
    };
    let Some(line) = text.lines().next() else {
        return;
    };
    let mut parts = line.split_whitespace();
    let Some(method) = parts.next() else { return };
    let Some(target) = parts.next() else { return };
    let Ok(url) = Url::parse(&format!("http://fixture{target}")) else {
        return;
    };
    let path = url.path().trim_start_matches('/').to_owned();
    let query: Vec<_> = url.query_pairs().into_owned().collect();

    let (status, body) = match method {
        "GET" => {
            state.get_requests.lock().unwrap().push(target.to_owned());
            let page = { state.pages.lock().unwrap().pop_front() };
            if let Some(page) = page {
                ("200 OK", page)
            } else if let Some(objects) = &state.objects {
                let prefix = query
                    .iter()
                    .find(|(key, _)| key == "prefix")
                    .map_or("", |(_, value)| value.as_str());
                let path = Path::parse(prefix.trim_end_matches('/')).ok();
                let mut entries = Vec::new();
                if let Some(path) = path.as_ref() {
                    let mut listing = objects.list(Some(path));
                    while let Ok(Some(meta)) = listing.try_next().await {
                        entries.push(meta.location.to_string());
                    }
                }
                let versions = entries
                    .iter()
                    .map(|key| {
                        format!("<Version><Key>{key}</Key><VersionId>current</VersionId></Version>")
                    })
                    .collect::<String>();
                (
                    "200 OK",
                    format!(
                        "<ListVersionsResult><IsTruncated>false</IsTruncated>{versions}</ListVersionsResult>"
                    ),
                )
            } else {
                ("500 Internal Server Error", String::new())
            }
        }
        "DELETE" => {
            state
                .delete_requests
                .lock()
                .unwrap()
                .push(target.to_owned());
            if state.pause_first_delete.swap(false, Ordering::SeqCst) {
                state.delete_entered.notify_one();
                state.release_delete.notified().await;
            }
            if let Some(objects) = &state.objects
                && let Ok(path) = Path::parse(&path)
            {
                let _ = objects.delete(&path).await;
            }
            ("204 No Content", String::new())
        }
        _ => ("405 Method Not Allowed", String::new()),
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
}

fn page(prefix: &str, truncated: bool, marker: Option<(&str, &str)>, versions: &str) -> String {
    let markers = marker.map_or_else(String::new, |(key, version)| {
        format!("<NextKeyMarker>{key}</NextKeyMarker><NextVersionIdMarker>{version}</NextVersionIdMarker>")
    });
    format!(
        "<ListVersionsResult><IsTruncated>{truncated}</IsTruncated>{markers}{versions}</ListVersionsResult>",
        versions = versions.replace("{{prefix}}", prefix)
    )
}

#[tokio::test]
async fn version_purge_forwards_markers_and_deletes_versions_and_markers() {
    let scope = Scope::new("protocol-pagination").unwrap();
    let prefix = format!("test/scopes/{}/", scope_hex(&scope));
    let mut pages = VecDeque::new();
    pages.push_back(page(
        &prefix,
        true,
        Some(("marker-key", "marker-version")),
        "<Version><Key>{{prefix}}one</Key><VersionId>v1</VersionId></Version>",
    ));
    pages.push_back(page(
        &prefix,
        false,
        None,
        "<Version><Key>{{prefix}}one</Key><VersionId>null</VersionId></Version><DeleteMarker><Key>{{prefix}}two</Key><VersionId>delete-2</VersionId></DeleteMarker>",
    ));
    let fixture = Fixture::new(FixtureState {
        pages: Mutex::new(pages),
        ..FixtureState::default()
    })
    .await;
    let objects = Arc::new(InMemory::new());
    let storage = S3Storage::with_store(objects, Some(fixture.signer()), "test").unwrap();
    storage
        .purge_scope_generation(&scope, ScopeGeneration(0))
        .await
        .unwrap();

    let requests = fixture.state.get_requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].contains("key-marker=marker-key"));
    assert!(requests[1].contains("version-id-marker=marker-version"));
    let deletes = fixture.state.delete_requests.lock().unwrap();
    assert_eq!(deletes.len(), 3);
    assert!(deletes[0].contains("versionId=v1"));
    assert!(deletes[1].contains("versionId=null"));
    assert!(deletes[2].contains("versionId=delete-2"));
}

#[tokio::test]
async fn repeated_version_marker_cycle_fails_before_an_extra_request() {
    let scope = Scope::new("protocol-marker-cycle").unwrap();
    let prefix = format!("test/scopes/{}/", scope_hex(&scope));
    let pages = VecDeque::from([
        page(&prefix, true, Some(("key-a", "v1")), ""),
        page(&prefix, true, Some(("key-b", "v2")), ""),
        page(&prefix, true, Some(("key-a", "v1")), ""),
    ]);
    let fixture = Fixture::new(FixtureState {
        pages: Mutex::new(pages),
        ..FixtureState::default()
    })
    .await;
    let storage =
        S3Storage::with_store(Arc::new(InMemory::new()), Some(fixture.signer()), "test").unwrap();
    assert_eq!(
        storage
            .purge_scope_generation(&scope, ScopeGeneration(0))
            .await
            .unwrap_err()
            .kind(),
        tinystoragedrivers_core::ErrorKind::Backend
    );
    assert_eq!(fixture.state.get_requests.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn signed_url_failure_does_not_expose_credentials_or_query_values() {
    let storage = S3Storage::with_store(
        Arc::new(InMemory::new()),
        Some(Arc::new(LeakyFailureSigner)),
        "test",
    )
    .unwrap();
    let error = storage
        .purge_scope_generation(
            &Scope::new("signing-error-scope").unwrap(),
            ScopeGeneration(0),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), tinystoragedrivers_core::ErrorKind::Backend);
    assert!(!format!("{error:?}").contains("secret"));
}

#[tokio::test]
async fn delayed_old_generation_delete_cannot_remove_replacement_blob() {
    let objects = Arc::new(InMemory::new());
    let fixture = Fixture::new(FixtureState {
        objects: Some(Arc::clone(&objects)),
        pause_first_delete: AtomicBool::new(true),
        ..FixtureState::default()
    })
    .await;
    let blobs = Arc::new(S3Storage::with_store(objects, Some(fixture.signer()), "test").unwrap());
    let records = Arc::new(MemoryStorage::new());
    let first = CompositeStorage::new(records.clone(), blobs.clone());
    let second = CompositeStorage::new(records, blobs);
    let scope = Scope::new("delayed-s3-generation-delete").unwrap();
    first
        .for_scope(&scope)
        .unwrap()
        .blobs()
        .put("same-key", vec![1], None)
        .await
        .unwrap();
    let request = PurgeRequest {
        operation_id: "delayed-generation-purge".into(),
        mode: PurgeMode::Quiescent,
    };
    let stale_runner = {
        let first = first;
        let scope = scope.clone();
        let request = request.clone();
        tokio::spawn(async move { first.for_scope(&scope).unwrap().purge(&request).await })
    };
    fixture.state.delete_entered.notified().await;

    let receipt = second
        .for_scope(&scope)
        .unwrap()
        .purge(&request)
        .await
        .unwrap();
    assert_eq!(
        receipt.status,
        tinystoragedrivers_core::PurgeStatus::Complete
    );
    let replacement = second
        .for_scope_generation(&scope, receipt.generation)
        .unwrap()
        .blobs()
        .clone();
    replacement.put("same-key", vec![9], None).await.unwrap();

    fixture.state.release_delete.notify_one();
    stale_runner.await.unwrap().unwrap();
    assert_eq!(
        replacement.get("same-key").await.unwrap().unwrap().bytes,
        [9]
    );
    let deletes = fixture.state.delete_requests.lock().unwrap();
    assert!(
        deletes
            .iter()
            .all(|request| !request.contains("scope-generations"))
    );
}

fn scope_hex(scope: &Scope) -> String {
    scope
        .as_str()
        .bytes()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
