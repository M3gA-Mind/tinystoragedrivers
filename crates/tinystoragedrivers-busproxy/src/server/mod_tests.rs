//! The server fails closed and never reads authority from a request.
use std::sync::Arc;

use serde_json::json;
use tinystoragedrivers_api::Storage;
use tinystoragedrivers_bus as wire;
use tinystoragedrivers_bus::StorageKind;
use tinystoragedrivers_testkit::memory_storage;
use tinystoragedrivers_types::{Scope, WatchCursor, WatchNotice, WatchReset};

use super::*;

fn server() -> StorageServer {
    StorageServer::new(memory_storage(&Scope::new("local").unwrap()))
}

async fn kind(server: &StorageServer, object: &str, selector: &str, request: Value) -> StorageKind {
    server
        .handle(Admission::Confidential, object, selector, request)
        .await
        .unwrap_err()
        .kind
}

#[tokio::test]
async fn unknown_objects_and_selectors_are_refused() {
    let server = server();
    assert!(format!("{server:?}").contains("StorageServer"));
    assert_eq!(
        kind(&server, "Nope", "Get", json!({})).await,
        StorageKind::InvalidInput
    );
    assert_eq!(
        kind(&server, "Documents", "Arbitrary", json!({})).await,
        StorageKind::InvalidInput
    );
    assert_eq!(
        kind(&server, "Kv", "Arbitrary", json!({})).await,
        StorageKind::InvalidInput
    );
    assert_eq!(
        kind(&server, "Kv", "Capabilities", json!({})).await,
        StorageKind::InvalidInput
    );
    // Streamed transfers are not request/reply selectors.
    assert_eq!(
        kind(&server, "Blobs", "PutStream", json!({"key":"a","len":0})).await,
        StorageKind::InvalidInput
    );
}

#[tokio::test]
async fn a_forged_scope_or_grant_field_is_a_decode_failure() {
    let server = server();
    for forged in ["scope", "grant", "generation"] {
        let mut request = json!({"collection": "notes", "document_id": "n1"});
        request[forged] = json!("bob");
        assert_eq!(
            kind(&server, "Documents", "Get", request).await,
            StorageKind::InvalidInput,
            "{forged}"
        );
    }
}

#[tokio::test]
async fn secrets_require_confidential_admission_and_forward_to_the_bound_store() {
    let server = server();
    let denied = server
        .handle(Admission::Private, "Secrets", "List", json!({"prefix": ""}))
        .await
        .unwrap_err();
    assert_eq!(denied.kind, StorageKind::Unsupported);
    assert_eq!(
        server
            .handle(
                Admission::Confidential,
                "Secrets",
                "List",
                json!({"prefix": ""})
            )
            .await
            .unwrap(),
        json!([])
    );
}

#[tokio::test]
async fn batches_and_inline_blobs_over_their_limits_are_refused() {
    let server = server();
    let ops: Vec<Value> = (0..=MAX_BATCH_OPS_PLUS)
        .map(|i| json!({"op":"put","collection":"c","document_id":format!("d{i}"),"document_json":"{}","precondition":{"kind":"none"}}))
        .collect();
    assert_eq!(
        kind(&server, "Documents", "AtomicBatch", json!({"ops": ops})).await,
        StorageKind::InvalidInput
    );
    let blob = vec![0u8; tinystoragedrivers_bus::MAX_INLINE_BLOB_BYTES + 1];
    assert_eq!(
        kind(&server, "Blobs", "Put", json!({"key":"k","bytes": blob})).await,
        StorageKind::InvalidInput
    );
    assert_eq!(
        kind(
            &server,
            "Blobs",
            "PresignGet",
            json!({"key":"k","ttl_secs": 3601})
        )
        .await,
        StorageKind::InvalidInput
    );
}

#[tokio::test]
async fn document_selectors_forward_queries_claims_and_collection_lifecycle() {
    let server = server();
    server
        .handle(
            Admission::Private,
            "Documents",
            "EnsureCollection",
            json!({"spec":{"name":"items", "indexes":[], "search":{"fields":["title"]}}}),
        )
        .await
        .unwrap();
    let put = |id: &str, title: &str| {
        json!({"collection":"items", "document_id":id,
            "document_json": format!("{{\"title\":\"{title}\",\"state\":\"queued\"}}"),
            "precondition":{"kind":"none"}})
    };
    for id in ["a", "b"] {
        server
            .handle(Admission::Private, "Documents", "Put", put(id, "hello"))
            .await
            .unwrap();
    }

    let get = server
        .handle(
            Admission::Private,
            "Documents",
            "Get",
            json!({"collection":"items", "document_id":"a"}),
        )
        .await
        .unwrap();
    assert_eq!(get["doc"]["title"], "hello");
    assert_eq!(get["version"], 1);

    let count = server
        .handle(
            Admission::Private,
            "Documents",
            "Count",
            json!({"collection":"items", "filter":{"op":"all"}}),
        )
        .await
        .unwrap();
    assert_eq!(count, 2);
    let page = server
        .handle(
            Admission::Private,
            "Documents",
            "Query",
            json!({"collection":"items", "query":{"filter":{"op":"all"}, "limit":1}}),
        )
        .await
        .unwrap();
    assert_eq!(page["items"].as_array().unwrap().len(), 1);
    assert!(page["next"].is_string());

    for (selector, request) in [
        (
            "AtomicBatch",
            json!({"ops":[{"op":"put", "collection":"items", "id":"batch", "doc":{"title":"hello"}, "precondition":{"kind":"none"}}]}),
        ),
        (
            "AtomicBatchChecked",
            json!({"checks":[{"collection":"items", "id":"batch", "expected":{"kind":"version", "version":1}}], "ops":[{"op":"put", "collection":"items", "id":"batch-checked", "doc":{"title":"hello"}, "precondition":{"kind":"none"}}]}),
        ),
    ] {
        let result = server
            .handle(Admission::Private, "Documents", selector, request)
            .await
            .unwrap();
        assert_eq!(result.as_array().unwrap().len(), 1);
    }

    assert_eq!(
        kind(
            &server,
            "Documents",
            "AtomicBatch",
            json!({"checks":[{"collection":"items", "id":"a", "expected":{"kind":"version", "version":1}}], "ops":[]}),
        )
        .await,
        StorageKind::InvalidInput
    );
}

#[tokio::test]
async fn document_claim_delete_search_and_collection_selectors_forward() {
    let server = server();
    server
        .handle(
            Admission::Private,
            "Documents",
            "EnsureCollection",
            json!({"spec":{"name":"items", "indexes":[], "search":{"fields":["title"]}}}),
        )
        .await
        .unwrap();
    for id in ["a", "b"] {
        server
            .handle(
                Admission::Private,
                "Documents",
                "Put",
                json!({"collection":"items", "document_id":id, "document_json":"{\"title\":\"hello\",\"state\":\"queued\"}", "precondition":{"kind":"none"}}),
            )
            .await
            .unwrap();
    }
    server
        .handle(
            Admission::Private,
            "Documents",
            "Put",
            json!({"collection":"items", "document_id":"c", "document_json":"{\"title\":\"other\",\"state\":\"done\"}", "precondition":{"kind":"none"}}),
        )
        .await
        .unwrap();
    let claimed = server
        .handle(
            Admission::Private,
            "Documents",
            "Claim",
            json!({"collection":"items", "filter":{"op":"eq", "field":"state", "value":"queued"}, "patch":"{\"state\":\"running\"}"}),
        )
        .await
        .unwrap();
    assert!(!claimed.is_null());
    let deleted = server
        .handle(
            Admission::Private,
            "Documents",
            "DeleteWhere",
            json!({"collection":"items", "filter":{"op":"eq", "field":"state", "value":"queued"}}),
        )
        .await
        .unwrap();
    assert_eq!(deleted, 1);
    assert!(
        server
            .handle(
                Admission::Private,
                "Documents",
                "Delete",
                json!({"collection":"items", "document_id":"c", "precondition":{"kind":"version", "version":1}}),
            )
            .await
            .unwrap()
            .as_bool()
            .unwrap()
    );
    let names = server
        .handle(
            Admission::Private,
            "Documents",
            "Collections",
            json!({"prefix":"it"}),
        )
        .await
        .unwrap();
    assert_eq!(names, json!(["items"]));
}

#[tokio::test]
async fn document_search_and_collection_schema_selectors_forward() {
    let server = server();
    let spec = json!({"name":"events", "indexes":[], "search":{"fields":["title"]}});
    server
        .handle(
            Admission::Private,
            "Documents",
            "EnsureCollection",
            json!({"spec":spec}),
        )
        .await
        .unwrap();
    server
        .handle(
            Admission::Private,
            "Documents",
            "Put",
            json!({"collection":"events", "document_id":"one", "document_json":"{\"title\":\"needle\"}", "precondition":{"kind":"none"}}),
        )
        .await
        .unwrap();
    let hits = server
        .handle(
            Admission::Private,
            "Documents",
            "Search",
            json!({"collection":"events", "text":"needle", "limit":1}),
        )
        .await
        .unwrap();
    assert_eq!(hits.as_array().unwrap().len(), 1);

    server
        .handle(
            Admission::Private,
            "Documents",
            "ValidateCollectionSpec",
            json!({"spec":spec}),
        )
        .await
        .unwrap();
    server
        .handle(
            Admission::Private,
            "Documents",
            "DropCollection",
            json!({"collection":"events"}),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn blob_selectors_forward_inline_reads_ranges_pages_and_deletes() {
    let server = server();
    server
        .handle(
            Admission::Private,
            "Blobs",
            "Put",
            json!({"key":"docs/a", "bytes":[1,2,3,4], "content_type":"application/octet-stream"}),
        )
        .await
        .unwrap();
    assert_eq!(
        server
            .handle(Admission::Private, "Blobs", "Head", json!({"key":"docs/a"}),)
            .await
            .unwrap()["len"],
        4
    );
    assert_eq!(
        server
            .handle(Admission::Private, "Blobs", "Get", json!({"key":"docs/a"}),)
            .await
            .unwrap()["bytes"],
        json!([1, 2, 3, 4])
    );
    assert_eq!(
        server
            .handle(
                Admission::Private,
                "Blobs",
                "GetRange",
                json!({"key":"docs/a", "start":1, "end":3}),
            )
            .await
            .unwrap(),
        json!([2, 3])
    );
}

#[tokio::test]
async fn blob_list_page_capabilities_and_delete_selectors_forward() {
    let server = server();
    server
        .handle(
            Admission::Private,
            "Blobs",
            "Put",
            json!({"key":"docs/a", "bytes":[1,2,3,4]}),
        )
        .await
        .unwrap();
    server
        .handle(
            Admission::Private,
            "Blobs",
            "Put",
            json!({"key":"docs/temporary", "bytes":[5], "ttl_ms":60000}),
        )
        .await
        .unwrap();
    let page = server
        .handle(
            Admission::Private,
            "Blobs",
            "ListPage",
            json!({"prefix":"docs/", "limit":1}),
        )
        .await
        .unwrap();
    assert_eq!(page["items"].as_array().unwrap().len(), 1);
    for (selector, request) in [
        ("GetRange", json!({"key":"docs/a", "start":3, "end":2})),
        ("ListPage", json!({"prefix":"docs/", "limit":0})),
        ("Capabilities", json!({"unexpected":true})),
    ] {
        assert_eq!(
            kind(&server, "Blobs", selector, request).await,
            StorageKind::InvalidInput
        );
    }
    assert_eq!(
        kind(
            &server,
            "Blobs",
            "PresignPut",
            json!({"key":"docs/a", "ttl_secs":60}),
        )
        .await,
        StorageKind::Unsupported
    );
    assert_eq!(
        server
            .handle(
                Admission::Private,
                "Blobs",
                "List",
                json!({"prefix":"docs/"}),
            )
            .await
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let capabilities = server
        .handle(Admission::Private, "Blobs", "Capabilities", Value::Null)
        .await
        .unwrap();
    assert!(capabilities["max_key_len"].as_u64().unwrap() > 0);
    assert!(
        server
            .handle(
                Admission::Private,
                "Blobs",
                "Delete",
                json!({"key":"docs/a"}),
            )
            .await
            .unwrap()
            .as_bool()
            .unwrap()
    );
    assert!(
        server
            .handle(Admission::Private, "Blobs", "Get", json!({"key":"docs/a"}),)
            .await
            .unwrap()
            .is_null()
    );
}

#[tokio::test]
async fn kv_selectors_forward_cas_listing_and_delete() {
    let server = server();
    server
        .handle(
            Admission::Private,
            "Kv",
            "Put",
            json!({"key":"item", "value":"first"}),
        )
        .await
        .unwrap();
    let cas = server
        .handle(
            Admission::Private,
            "Kv",
            "Cas",
            json!({"key":"item", "value":"second", "expected":{"kind":"version", "version":1}}),
        )
        .await
        .unwrap();
    assert_eq!(cas, json!(2));
    let found = server
        .handle(Admission::Private, "Kv", "Get", json!({"key":"item"}))
        .await
        .unwrap();
    assert_eq!(found["doc"], "second");
    let page = server
        .handle(
            Admission::Private,
            "Kv",
            "List",
            json!({"prefix":"it", "limit":10}),
        )
        .await
        .unwrap();
    assert_eq!(page["items"].as_array().unwrap().len(), 1);
    let capabilities = server
        .handle(Admission::Private, "Kv", "Capabilities", Value::Null)
        .await
        .unwrap();
    assert!(capabilities["capabilities"].is_array());
    assert!(
        server
            .handle(
                Admission::Private,
                "Kv",
                "Delete",
                json!({"key":"item", "expected":{"kind":"version", "version":2}}),
            )
            .await
            .unwrap()
            .as_bool()
            .unwrap()
    );
}

const MAX_BATCH_OPS_PLUS: usize = tinystoragedrivers_bus::MAX_BATCH_OPS;

#[tokio::test]
async fn capabilities_never_list_host_only_entries() {
    let server = server();
    let reply = server
        .handle(Admission::Private, "Documents", "Capabilities", Value::Null)
        .await
        .unwrap();
    let names: Vec<String> = serde_json::from_value(reply["capabilities"].clone()).unwrap();
    for host_only in [
        "fencing",
        "existing_schema",
        "maintenance",
        "scope_enumeration",
    ] {
        assert!(!names.iter().any(|n| n == host_only), "{host_only}");
    }
    let _ = Arc::new(());
}

#[tokio::test]
async fn closing_an_unknown_watch_is_not_found() {
    let server = server();
    assert_eq!(
        kind(
            &server,
            "Documents",
            "WatchClose",
            json!({"watch_id":"w999"})
        )
        .await,
        StorageKind::NotFound
    );
}

#[tokio::test]
async fn watch_signals_are_cursor_only_and_terminal_resets_release_the_slot() {
    let base = memory_storage(&Scope::new("watch-server").unwrap());
    let source = Arc::new(watching_fake::Source::default());
    let docs = Arc::new(watching_fake::Docs {
        inner: base.documents().clone(),
        source: source.clone(),
        fail_open: false,
    });
    let storage = Storage::new(
        docs,
        base.streams().clone(),
        base.kv().clone(),
        base.blobs().clone(),
        base.queues().clone(),
        base.secrets().clone(),
    );
    let server = StorageServer::new(storage);
    let mut signals = server.signals.subscribe();
    let opened = server
        .handle(
            Admission::Private,
            "Documents",
            "WatchOpen",
            json!({"collection":"items","filter":{"op":"all"},"interval_ms":100}),
        )
        .await
        .unwrap();
    let id: String = serde_json::from_value(opened["watch_id"].clone()).unwrap();
    let sender = source.0.lock().unwrap()[0].clone();
    sender
        .send(Ok(Some(WatchNotice::Invalidated {
            cursor: Some(WatchCursor::new(vec![4, 2]).unwrap()),
        })))
        .unwrap();
    let invalidated = signals.recv().await.unwrap();
    assert_eq!(invalidated.watch_id, id);
    assert_eq!(
        invalidated.notice,
        wire::WatchNotice::Invalidated {
            cursor: Some(vec![4, 2])
        }
    );
    sender
        .send(Ok(Some(WatchNotice::ResetRequired {
            reason: WatchReset::ResumeLost,
            source: None,
        })))
        .unwrap();
    let reset = signals.recv().await.unwrap();
    assert_eq!(
        reset.notice,
        wire::WatchNotice::ResetRequired {
            reason: wire::WatchReset::CursorExpired
        }
    );
    tokio::task::yield_now().await;
    assert_eq!(server.open_watches(), 0);
    assert_eq!(
        server
            .handle(
                Admission::Private,
                "Documents",
                "WatchClose",
                json!({"watch_id":id}),
            )
            .await
            .unwrap_err()
            .kind,
        StorageKind::NotFound
    );
}

#[tokio::test]
async fn watch_open_rejects_invalid_input_and_sanitizes_backend_failures() {
    let invalid = server();
    for request in [
        json!({"collection":"items","filter":{"op":"all"},"interval_ms":0}),
        json!({"collection":"items","filter":{"op":"all"},"interval_ms":100,"after":[]}),
    ] {
        assert_eq!(
            invalid
                .handle(Admission::Private, "Documents", "WatchOpen", request)
                .await
                .unwrap_err()
                .kind,
            StorageKind::InvalidInput
        );
    }

    let base = memory_storage(&Scope::new("watch-failed-open").unwrap());
    let docs = Arc::new(watching_fake::Docs {
        inner: base.documents().clone(),
        source: Arc::new(watching_fake::Source::default()),
        fail_open: true,
    });
    let storage = Storage::new(
        docs,
        base.streams().clone(),
        base.kv().clone(),
        base.blobs().clone(),
        base.queues().clone(),
        base.secrets().clone(),
    );
    let server = StorageServer::new(storage);
    assert_eq!(
        kind(
            &server,
            "Documents",
            "WatchOpen",
            json!({"collection":"items","filter":{"op":"all"},"interval_ms":100}),
        )
        .await,
        StorageKind::Backend
    );
    assert_eq!(server.open_watches(), 0);
}

#[tokio::test]
async fn native_watch_errors_emit_only_a_terminal_backend_category() {
    let base = memory_storage(&Scope::new("watch-error").unwrap());
    let source = Arc::new(watching_fake::Source::default());
    let docs = Arc::new(watching_fake::Docs {
        inner: base.documents().clone(),
        source: source.clone(),
        fail_open: false,
    });
    let storage = Storage::new(
        docs,
        base.streams().clone(),
        base.kv().clone(),
        base.blobs().clone(),
        base.queues().clone(),
        base.secrets().clone(),
    );
    let server = StorageServer::new(storage);
    let mut signals = server.signals.subscribe();
    let opened = server
        .handle(
            Admission::Private,
            "Documents",
            "WatchOpen",
            json!({"collection":"items","filter":{"op":"all"},"interval_ms":100}),
        )
        .await
        .unwrap();
    let id: String = serde_json::from_value(opened["watch_id"].clone()).unwrap();
    source.0.lock().unwrap()[0]
        .send(Err(tinystoragedrivers_types::StorageError::backend(
            "private diagnostic detail",
        )))
        .unwrap();
    let signal = signals.recv().await.unwrap();
    assert_eq!(signal.watch_id, id);
    assert_eq!(
        signal.notice,
        wire::WatchNotice::ResetRequired {
            reason: wire::WatchReset::Backend
        }
    );
    assert!(!format!("{signal:?}").contains("private diagnostic detail"));
    tokio::task::yield_now().await;
    assert_eq!(server.open_watches(), 0);
}

#[tokio::test]
async fn watch_registry_enforces_its_per_binding_capacity() {
    let base = memory_storage(&Scope::new("watch-capacity").unwrap());
    let docs = Arc::new(watching_fake::Docs {
        inner: base.documents().clone(),
        source: Arc::new(watching_fake::Source::default()),
        fail_open: false,
    });
    let storage = Storage::new(
        docs,
        base.streams().clone(),
        base.kv().clone(),
        base.blobs().clone(),
        base.queues().clone(),
        base.secrets().clone(),
    );
    let server = StorageServer::new(storage);
    let request = || json!({"collection":"items","filter":{"op":"all"},"interval_ms":100});
    let mut ids = Vec::new();
    for _ in 0..MAX_WATCHES {
        let opened = server
            .handle(Admission::Private, "Documents", "WatchOpen", request())
            .await
            .unwrap();
        ids.push(opened["watch_id"].as_str().unwrap().to_owned());
    }
    assert_eq!(server.open_watches(), MAX_WATCHES);
    assert_eq!(
        server
            .handle(Admission::Private, "Documents", "WatchOpen", request())
            .await
            .unwrap_err()
            .kind,
        StorageKind::InvalidInput
    );
    for id in ids {
        server
            .handle(
                Admission::Private,
                "Documents",
                "WatchClose",
                json!({"watch_id":id}),
            )
            .await
            .unwrap();
    }
    assert_eq!(server.open_watches(), 0);
}

async fn call(server: &StorageServer, object: &str, selector: &str, request: Value) -> Value {
    server
        .handle(Admission::Private, object, selector, request)
        .await
        .unwrap()
}

#[tokio::test]
async fn put_options_land_as_metadata_that_every_read_returns() {
    let server = server();
    let far = 4_102_444_800_000_i64;
    call(
        &server,
        "Documents",
        "Put",
        json!({"collection":"c","document_id":"a","document_json":"{\"n\":1}",
               "precondition":{"kind":"none"},"schema":3,"expires_ms":far}),
    )
    .await;
    let got = call(
        &server,
        "Documents",
        "Get",
        json!({"collection":"c","document_id":"a"}),
    )
    .await;
    assert_eq!(got["meta"]["schema"], 3);
    assert_eq!(got["meta"]["expires_ms"], far);
    assert!(got["meta"]["created_ms"].as_i64().unwrap() > 0);
    let page = call(
        &server,
        "Documents",
        "Query",
        json!({"collection":"c","query":{}}),
    )
    .await;
    assert_eq!(page["items"][0]["meta"]["schema"], 3);
    let claimed = call(
        &server,
        "Documents",
        "Claim",
        json!({"collection":"c","filter":{"op":"all"},"patch":"{\"n\":2}"}),
    )
    .await;
    assert_eq!(claimed["meta"]["schema"], 3);
}

mod watching_fake {
    //! A memory store whose documents report a native watch fed by a channel, so a
    //! test decides exactly which invalidations fire.
    use std::sync::{Arc, Mutex};

    use serde_json::Value;
    use tinystoragedrivers_api::{DocumentStore, DocumentWatch, async_trait};
    use tinystoragedrivers_types::{
        CollectionSpec, Filter, Page, Precondition, Query, Result, Sort, StorageError, Version,
        Versioned, WatchNotice, WatchOptions,
    };
    use tokio::sync::mpsc;

    /// The senders of every watch a [`Docs`] opened, in order.
    #[derive(Debug, Default)]
    pub(super) struct Source(pub Mutex<Vec<mpsc::UnboundedSender<Result<Option<WatchNotice>>>>>);

    #[derive(Debug)]
    pub(super) struct Docs {
        pub inner: Arc<dyn DocumentStore>,
        pub source: Arc<Source>,
        pub fail_open: bool,
    }

    #[derive(Debug)]
    struct Feed(mpsc::UnboundedReceiver<Result<Option<WatchNotice>>>);

    #[async_trait]
    impl DocumentWatch for Feed {
        async fn next(&mut self) -> Result<Option<WatchNotice>> {
            self.0.recv().await.unwrap_or(Ok(None))
        }
    }

    #[async_trait]
    impl DocumentStore for Docs {
        fn capabilities(&self) -> tinystoragedrivers_types::Capabilities {
            self.inner
                .capabilities()
                .with(tinystoragedrivers_types::Capability::Watch)
        }
        async fn watch(
            &self,
            _collection: &str,
            _filter: &Filter,
            _options: &WatchOptions,
        ) -> Result<Box<dyn DocumentWatch>> {
            if self.fail_open {
                return Err(StorageError::backend("private native watch failure"));
            }
            let (tx, rx) = mpsc::unbounded_channel();
            self.source.0.lock().unwrap().push(tx);
            Ok(Box::new(Feed(rx)))
        }
        async fn ensure_collection(&self, spec: &CollectionSpec) -> Result<()> {
            self.inner.ensure_collection(spec).await
        }
        async fn validate_collection_spec(&self, spec: &CollectionSpec) -> Result<()> {
            self.inner.validate_collection_spec(spec).await
        }
        async fn get(&self, c: &str, id: &str) -> Result<Option<Versioned<Value>>> {
            self.inner.get(c, id).await
        }
        async fn put(&self, c: &str, id: &str, doc: Value, p: Precondition) -> Result<Version> {
            self.inner.put(c, id, doc, p).await
        }
        async fn delete(&self, c: &str, id: &str, p: Precondition) -> Result<bool> {
            self.inner.delete(c, id, p).await
        }
        async fn query(&self, c: &str, q: &Query) -> Result<Page<Versioned<Value>>> {
            self.inner.query(c, q).await
        }
        async fn count(&self, c: &str, f: &Filter) -> Result<u64> {
            self.inner.count(c, f).await
        }
        async fn delete_where(&self, c: &str, f: &Filter) -> Result<u64> {
            self.inner.delete_where(c, f).await
        }
        async fn claim(
            &self,
            c: &str,
            f: &Filter,
            s: &[Sort],
            p: &Value,
        ) -> Result<Option<Versioned<Value>>> {
            self.inner.claim(c, f, s, p).await
        }
        async fn drop_collection(&self, c: &str) -> Result<()> {
            self.inner.drop_collection(c).await
        }
    }
}

#[tokio::test]
async fn a_put_without_options_is_the_pre_v4_request_and_a_forged_created_ms_is_refused() {
    let server = server();
    call(
        &server,
        "Documents",
        "Put",
        json!({"collection":"c","document_id":"a","document_json":"{}",
               "precondition":{"kind":"none"}}),
    )
    .await;
    assert_eq!(
        kind(
            &server,
            "Documents",
            "Put",
            json!({"collection":"c","document_id":"b","document_json":"{}",
                   "precondition":{"kind":"none"},"created_ms":1}),
        )
        .await,
        StorageKind::InvalidInput
    );
    // Nothing was written by the refused request.
    let got = call(
        &server,
        "Documents",
        "Get",
        json!({"collection":"c","document_id":"b"}),
    )
    .await;
    assert!(got.is_null());
}

#[tokio::test]
async fn seal_closes_a_stream_and_maintenance_selectors_do_not_exist() {
    let server = server();
    call(
        &server,
        "Streams",
        "Append",
        json!({"stream":"s","record_json":"1"}),
    )
    .await;
    let sealed = call(&server, "Streams", "Seal", json!({"stream":"s"})).await;
    assert_eq!(
        sealed,
        json!({"base":0,"len":1,"blob_bytes":sealed["blob_bytes"]})
    );
    assert_eq!(
        kind(
            &server,
            "Streams",
            "Append",
            json!({"stream":"s","record_json":"2"})
        )
        .await,
        StorageKind::Conflict
    );
    assert_eq!(
        kind(&server, "Streams", "Seal", json!({"stream":"never"})).await,
        StorageKind::NotFound
    );
    for selector in ["Maintain", "Checkpoint", "Scopes"] {
        for object in ["Documents", "Streams", "Blobs"] {
            assert_eq!(
                kind(&server, object, selector, json!({})).await,
                StorageKind::InvalidInput
            );
        }
    }
}

#[tokio::test]
async fn info_reports_extent_and_times_of_a_stream_and_null_for_a_missing_one() {
    let server = server();
    assert!(
        call(&server, "Streams", "Info", json!({"stream":"s"}))
            .await
            .is_null()
    );
    call(
        &server,
        "Streams",
        "Append",
        json!({"stream":"s","record_json":"1"}),
    )
    .await;
    let open = call(&server, "Streams", "Info", json!({"stream":"s"})).await;
    assert_eq!(
        (open["base"].clone(), open["len"].clone()),
        (json!(0), json!(1))
    );
    assert!(open["sealed_ms"].is_null());
    assert!(open["last_append_ms"].is_i64());

    call(&server, "Streams", "Seal", json!({"stream":"s"})).await;
    let sealed = call(&server, "Streams", "Info", json!({"stream":"s"})).await;
    assert!(sealed["sealed_ms"].is_i64());
    assert_eq!(sealed["last_append_ms"], open["last_append_ms"]);
}
