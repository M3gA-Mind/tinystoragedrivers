use super::*;
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::task::{Context, Poll, Waker};
use tinystoragedrivers_types::{
    Capabilities, Capability, Cursor, ErrorKind, Meta, WatchOptions, WriteOptions,
};

#[derive(Debug, Default)]
struct TestDocuments {
    value: Mutex<Option<Versioned<Value>>>,
    pages: Mutex<VecDeque<Page<Versioned<Value>>>>,
    cursors: Mutex<Vec<Option<Cursor>>>,
}

#[async_trait]
impl DocumentStore for TestDocuments {
    fn capabilities(&self) -> Capabilities {
        Capabilities::none()
    }

    async fn ensure_collection(&self, _: &CollectionSpec) -> Result<()> {
        Ok(())
    }
    async fn get(&self, _: &str, _: &str) -> Result<Option<Versioned<Value>>> {
        Ok(self.value.lock().unwrap().clone())
    }
    async fn put(&self, _: &str, _: &str, value: Value, _: Precondition) -> Result<Version> {
        *self.value.lock().unwrap() = Some(Versioned {
            id: "id".into(),
            version: Version::FIRST,
            doc: value,
            meta: Meta::unknown(),
        });
        Ok(Version::FIRST)
    }
    async fn delete(&self, _: &str, _: &str, _: Precondition) -> Result<bool> {
        Ok(self.value.lock().unwrap().take().is_some())
    }
    async fn query(&self, _: &str, query: &Query) -> Result<Page<Versioned<Value>>> {
        self.cursors.lock().unwrap().push(query.cursor.clone());
        Ok(self.pages.lock().unwrap().pop_front().unwrap_or(Page {
            items: Vec::new(),
            next: None,
        }))
    }
    async fn count(&self, _: &str, _: &Filter) -> Result<u64> {
        Ok(0)
    }
    async fn delete_where(&self, _: &str, _: &Filter) -> Result<u64> {
        Ok(0)
    }
    async fn claim(
        &self,
        _: &str,
        _: &Filter,
        _: &[Sort],
        _: &Value,
    ) -> Result<Option<Versioned<Value>>> {
        Ok(None)
    }
    async fn drop_collection(&self, _: &str) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
struct RepeatingCursor(AtomicUsize);

#[async_trait]
impl DocumentStore for RepeatingCursor {
    fn capabilities(&self) -> Capabilities {
        Capabilities::none()
    }

    async fn ensure_collection(&self, _: &CollectionSpec) -> Result<()> {
        unreachable!()
    }
    async fn get(&self, _: &str, _: &str) -> Result<Option<Versioned<Value>>> {
        unreachable!()
    }
    async fn put(&self, _: &str, _: &str, _: Value, _: Precondition) -> Result<Version> {
        unreachable!()
    }
    async fn delete(&self, _: &str, _: &str, _: Precondition) -> Result<bool> {
        unreachable!()
    }
    async fn query(&self, _: &str, _: &Query) -> Result<Page<Versioned<Value>>> {
        if self.0.fetch_add(1, Ordering::SeqCst) >= 2 {
            return Err(StorageError::backend("query paging did not advance"));
        }
        Ok(Page {
            items: vec![Versioned {
                id: "record".into(),
                version: Version::FIRST,
                doc: serde_json::json!({}),
                meta: Meta::unknown(),
            }],
            next: Some(Cursor("same-position".into())),
        })
    }
    async fn count(&self, _: &str, _: &Filter) -> Result<u64> {
        unreachable!()
    }
    async fn delete_where(&self, _: &str, _: &Filter) -> Result<u64> {
        unreachable!()
    }
    async fn claim(
        &self,
        _: &str,
        _: &Filter,
        _: &[Sort],
        _: &Value,
    ) -> Result<Option<Versioned<Value>>> {
        unreachable!()
    }
    async fn drop_collection(&self, _: &str) -> Result<()> {
        unreachable!()
    }
}

#[test]
fn query_all_rejects_a_repeated_continuation_cursor() {
    let store = RepeatingCursor(AtomicUsize::new(0));
    let result = run_ready(store.query_all("records", &Query::all()));
    let error = result.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
    assert_eq!(error.message(), "query paging did not advance");
    assert_eq!(store.0.load(Ordering::SeqCst), 2);
}

#[test]
fn document_defaults_report_capabilities_and_preserve_basic_write_behavior() {
    let store = TestDocuments::default();
    let filter = Filter::default();
    let spec = CollectionSpec::new("records");
    assert_eq!(
        run_ready(store.watch("records", &filter, &WatchOptions::default()))
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::Watch)
    );
    assert_eq!(
        run_ready(store.validate_collection_spec(&spec))
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::CollectionValidation)
    );
    assert_eq!(
        run_ready(store.put_with(
            "records",
            "id",
            serde_json::json!({}),
            Precondition::None,
            WriteOptions::default()
        ))
        .unwrap(),
        Version::FIRST
    );
    let metadata = WriteOptions {
        schema: 1,
        created_ms: Some(10),
        expires_ms: None,
    };
    assert_eq!(
        run_ready(store.put_with(
            "records",
            "id",
            serde_json::json!({}),
            Precondition::None,
            metadata
        ))
        .unwrap_err()
        .kind(),
        ErrorKind::Unsupported(Capability::RecordMetadata)
    );
    assert_eq!(
        run_ready(store.atomic_batch(vec![])).unwrap_err().kind(),
        ErrorKind::Unsupported(Capability::Transactions)
    );
    assert_eq!(
        run_ready(store.atomic_batch_checked(vec![], vec![]))
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::ValidatedBatch)
    );
    assert_eq!(
        run_ready(store.search("records", "text", 1))
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::FullText)
    );
    assert_eq!(
        run_ready(store.collections("records")).unwrap_err().kind(),
        ErrorKind::Unsupported(Capability::CollectionEnumeration)
    );
}

#[test]
fn document_extensions_encode_decode_and_follow_each_page_cursor() {
    let stored = TestDocuments::default();
    run_ready(stored.put_as(
        "records",
        "id",
        &serde_json::json!({"n": 7}),
        Precondition::None,
    ))
    .unwrap();
    let decoded = run_ready(stored.get_as::<serde_json::Value>("records", "id"))
        .unwrap()
        .unwrap();
    assert_eq!(decoded.doc, serde_json::json!({"n": 7}));

    *stored.pages.lock().unwrap() = VecDeque::from([Page {
        items: vec![Versioned {
            id: "typed".into(),
            version: Version::FIRST,
            doc: serde_json::json!({"n": 8}),
            meta: Meta::unknown(),
        }],
        next: None,
    }]);
    let typed = run_ready(stored.query_as::<serde_json::Value>("records", &Query::all())).unwrap();
    assert_eq!(typed.items[0].doc, serde_json::json!({"n": 8}));
    *stored.pages.lock().unwrap() = VecDeque::from([Page {
        items: vec![Versioned {
            id: "bad-type".into(),
            version: Version::FIRST,
            doc: serde_json::json!({"n": 8}),
            meta: Meta::unknown(),
        }],
        next: None,
    }]);
    assert_eq!(
        run_ready(stored.query_as::<u32>("records", &Query::all()))
            .unwrap_err()
            .kind(),
        ErrorKind::Serialization
    );

    let first = Cursor("page-one".into());
    let second = Cursor("page-two".into());
    stored.cursors.lock().unwrap().clear();
    *stored.pages.lock().unwrap() = VecDeque::from([
        Page {
            items: vec![Versioned {
                id: "one".into(),
                version: Version::FIRST,
                doc: serde_json::json!({"n": 1}),
                meta: Meta::unknown(),
            }],
            next: Some(first.clone()),
        },
        Page {
            items: vec![Versioned {
                id: "two".into(),
                version: Version::FIRST,
                doc: serde_json::json!({"n": 2}),
                meta: Meta::unknown(),
            }],
            next: Some(second.clone()),
        },
        Page {
            items: vec![],
            next: None,
        },
    ]);
    let all = run_ready(stored.query_all("records", &Query::all())).unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(
        *stored.cursors.lock().unwrap(),
        vec![None, Some(first), Some(second)]
    );
}

#[test]
fn document_extension_reports_serde_failures() {
    struct CannotSerialize;
    impl serde::Serialize for CannotSerialize {
        fn serialize<S>(&self, _: S) -> std::result::Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Err(serde::ser::Error::custom("test serialization failure"))
        }
    }

    let error = run_ready(TestDocuments::default().put_as(
        "records",
        "id",
        &CannotSerialize,
        Precondition::None,
    ))
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Serialization);
    assert!(error.message().contains("test serialization failure"));
}

fn run_ready<F: std::future::Future>(future: F) -> F::Output {
    let mut context = Context::from_waker(Waker::noop());
    let mut future = Box::pin(future);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("query unexpectedly pended"),
    }
}
