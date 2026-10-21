//! URL parsing, redaction and driver selection.

use super::*;
#[cfg(feature = "sqlite")]
use tinystoragedrivers_api::{DocumentStore, KvStore};
use tinystoragedrivers_types::{
    Capability, ErrorKind, Filter, Precondition, Scope, WriteCondition,
};

#[test]
fn parses_every_form() {
    assert_eq!(
        StorageConfig::parse(" memory ").unwrap(),
        StorageConfig::Memory
    );
    assert_eq!(
        StorageConfig::parse("memory:").unwrap(),
        StorageConfig::Memory
    );
    assert_eq!(
        StorageConfig::parse("sqlite:/var/lib/app").unwrap(),
        StorageConfig::Sqlite {
            path: "/var/lib/app".into()
        }
    );
    assert_eq!(
        StorageConfig::parse("sqlite:///tmp/x.db").unwrap(),
        StorageConfig::Sqlite {
            path: "/tmp/x.db".into()
        }
    );
    assert_eq!(
        StorageConfig::parse("./state").unwrap(),
        StorageConfig::Sqlite {
            path: "./state".into()
        }
    );
    assert_eq!(
        StorageConfig::parse("../up").unwrap(),
        StorageConfig::Sqlite {
            path: "../up".into()
        }
    );
    assert_eq!(
        StorageConfig::parse("file:data").unwrap(),
        StorageConfig::File { dir: "data".into() }
    );
    assert_eq!(
        StorageConfig::parse("mongodb+srv://h.example/app?retryWrites=true").unwrap(),
        StorageConfig::MongoDb {
            uri: "mongodb+srv://h.example/app?retryWrites=true".into(),
            database: "app".into()
        }
    );
}

#[test]
fn rejects_unusable_urls() {
    for bad in [
        "",
        "   ",
        "sqlite:",
        "file:",
        "postgres://h/db",
        "relative/path",
        "mongodb://h",
        "mongodb://h/",
        "mongodb://h/?x=1",
    ] {
        let error = StorageConfig::parse(bad).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput, "{bad:?}");
    }
}

#[test]
fn uri_redaction_helpers_handle_absent_schemes_and_overlapping_spans() {
    assert!(split_uri("not a URI").is_none());
    assert_eq!(
        split_uri("mongodb://user:pass@host/db?x=1").unwrap().path,
        "db"
    );
    assert_eq!(hex_value(b'0'), Some(0));
    assert_eq!(hex_value(b'9'), Some(9));
    assert_eq!(hex_value(b'a'), Some(10));
    assert_eq!(hex_value(b'F'), Some(15));
    assert_eq!(hex_value(b'G'), None);
    assert_eq!(redact_spans("safe", Vec::new()), "safe");
    assert_eq!(redact_spans("abcdef", vec![1..4, 3..5]), "a***f");
}

#[test]
fn never_prints_a_password() {
    let config = StorageConfig::parse("mongodb://app:s3cret@h:27017/db").unwrap();
    assert_eq!(config.to_string(), "mongodb://app:***@h:27017/db");
    assert!(!format!("{config:?}").contains("s3cret"));
    let error = StorageConfig::parse("mongodb://app:s3cret@h:27017").unwrap_err();
    assert!(!error.to_string().contains("s3cret"), "{error}");
    let error = StorageConfig::parse("redis://app:s3cret@h/0").unwrap_err();
    assert!(!error.to_string().contains("s3cret"), "{error}");
    assert_eq!(redact_url("mongodb://user@h/db"), "mongodb://user@h/db");
    assert_eq!(redact_url("mongodb://h/db"), "mongodb://h/db");
    assert_eq!(redact_url("no-scheme"), "no-scheme");
    assert_eq!(
        redact_url("no-scheme?password=secret"),
        "no-scheme?password=***"
    );
}

#[test]
fn names_its_driver_and_renders() {
    let cases = [
        ("memory", "memory", "memory"),
        ("sqlite:/d", "sqlite", "sqlite:/d"),
        ("file:/d", "file", "file:/d"),
        ("mongodb://h/db", "mongodb", "mongodb://h/db"),
    ];
    for (url, driver, shown) in cases {
        let config = StorageConfig::parse(url).unwrap();
        assert_eq!(config.driver(), driver);
        assert_eq!(config.to_string(), shown);
    }
}

#[tokio::test]
async fn opens_memory_and_names_missing_features() {
    assert!(open(&StorageConfig::Memory).await.is_ok());
    assert_eq!(StorageConfig::Memory.driver(), "memory");
    let mut missing = Vec::new();
    if cfg!(not(feature = "sqlite")) {
        missing.push("sqlite:/tmp/x");
    }
    if cfg!(not(feature = "mongodb")) {
        missing.push("mongodb://h/db");
    }
    if cfg!(not(feature = "file")) {
        missing.push("file:/tmp/x");
    }
    for url in missing {
        let config = StorageConfig::parse(url).unwrap();
        let error = open(&config).await.map(|_| ()).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert!(
            error
                .message()
                .contains(&format!("`{}` driver", config.driver())),
            "{error}"
        );
    }
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one regression verifies every guarded mutation family across a takeover"
)]
async fn sqlite_conditioned_ports_refuse_a_stale_holder_after_guard_takeover() {
    let dir = tempfile::tempdir().unwrap();
    let config = StorageConfig::Sqlite {
        path: dir.path().join("conditioned.db"),
    };
    let backend = open(&config).await.unwrap();
    let guard_scope = Scope::new("guard-owner").unwrap();
    let target_scope = Scope::new("target-owner").unwrap();
    let ordinary = backend.for_scope(&guard_scope).unwrap();
    ordinary
        .ensure_collection(&tinystoragedrivers_types::CollectionSpec::new("leases"))
        .await
        .unwrap();
    ordinary
        .ensure_collection(&tinystoragedrivers_types::CollectionSpec::new("items"))
        .await
        .unwrap();
    let guard = ordinary
        .documents()
        .put(
            "leases",
            "writer",
            serde_json::json!({"epoch": 1}),
            Precondition::Absent,
        )
        .await
        .unwrap();

    let condition = WriteCondition::new(
        guard_scope.clone(),
        "leases",
        "writer",
        Filter::eq("epoch", 1),
    )
    .unwrap();
    let held = backend
        .for_scope_conditioned(&target_scope, &condition)
        .unwrap();
    let capability = Capability::ConditionalWrites;
    assert!(held.documents().capabilities().contains(capability));
    assert!(held.streams().capabilities().contains(capability));
    assert!(held.blobs().capabilities().contains(capability));
    assert!(held.kv().capabilities().contains(Capability::Kv));
    assert!(
        held.kv()
            .capabilities()
            .contains(Capability::ConditionalWrites)
    );
    held.kv()
        .put("profile", serde_json::json!({"enabled": true}))
        .await
        .unwrap();
    held.queues()
        .enqueue(
            "jobs",
            &[tinystoragedrivers_types::NewMessage::new(
                serde_json::json!({"work": 1}),
            )],
        )
        .await
        .unwrap();

    held.documents()
        .put(
            "items",
            "before",
            serde_json::json!({"ok": true}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    held.streams()
        .append("events", serde_json::json!({"ok": true}))
        .await
        .unwrap();
    held.blobs()
        .put("payload", b"before".to_vec(), None)
        .await
        .unwrap();
    held.blobs()
        .put_sealed("_tsd/sealed/held", b"sealed-before".to_vec(), None)
        .await
        .unwrap();

    ordinary
        .documents()
        .put(
            "leases",
            "writer",
            serde_json::json!({"epoch": 2}),
            Precondition::Version(guard),
        )
        .await
        .unwrap();

    let stale_doc = held
        .documents()
        .put("items", "after", serde_json::json!({}), Precondition::None)
        .await
        .unwrap_err();
    assert_eq!(stale_doc.kind(), ErrorKind::Conflict);
    let stale_stream = held
        .streams()
        .append("events", serde_json::json!({"later": true}))
        .await
        .unwrap_err();
    assert_eq!(stale_stream.kind(), ErrorKind::Conflict);
    let stale_blob = held
        .blobs()
        .put("later", b"later".to_vec(), None)
        .await
        .unwrap_err();
    assert_eq!(stale_blob.kind(), ErrorKind::Conflict);
    assert_eq!(
        held.kv()
            .put("profile", serde_json::json!({"enabled": false}))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Conflict
    );
    assert_eq!(
        held.queues()
            .enqueue(
                "jobs",
                &[tinystoragedrivers_types::NewMessage::new(
                    serde_json::json!({"work": 2}),
                )],
            )
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Conflict
    );

    assert!(
        held.documents()
            .get("items", "after")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        held.kv().get("profile").await.unwrap().unwrap().doc,
        serde_json::json!({"enabled": true})
    );
    assert_eq!(
        held.blobs().delete("payload").await.unwrap_err().kind(),
        ErrorKind::Conflict
    );
    assert_eq!(
        held.blobs()
            .delete_sealed("_tsd/sealed/held")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Conflict
    );
    assert_eq!(
        held.blobs().sweep_expired().await.unwrap_err().kind(),
        ErrorKind::Conflict
    );
    assert_eq!(
        held.streams()
            .truncate_before("events", 0)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Conflict
    );
    assert_eq!(
        held.streams().seal("events").await.unwrap_err().kind(),
        ErrorKind::Conflict
    );
    assert_eq!(
        held.streams()
            .delete_stream("events")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Conflict
    );
    assert_eq!(
        held.streams()
            .delete_sealed_stream("events", 0)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Conflict
    );
    assert_eq!(held.streams().len("events").await.unwrap(), 1);
    assert!(held.blobs().get("later").await.unwrap().is_none());
    assert!(held.blobs().get("payload").await.unwrap().is_some());
    assert!(
        held.blobs()
            .get_sealed("_tsd/sealed/held")
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        ordinary
            .documents()
            .get("leases", "writer")
            .await
            .unwrap()
            .unwrap()
            .doc["epoch"],
        2
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_condition_checks_expiry_and_compressed_guard_bodies() {
    let dir = tempfile::tempdir().unwrap();
    let backend = open(&StorageConfig::Sqlite {
        path: dir.path().join("condition-body.db"),
    })
    .await
    .unwrap();
    let guard_scope = Scope::new("condition-body-guard").unwrap();
    let target_scope = Scope::new("condition-body-target").unwrap();
    let ordinary = backend.for_scope(&guard_scope).unwrap();
    ordinary
        .ensure_collection(&tinystoragedrivers_types::CollectionSpec::new("leases"))
        .await
        .unwrap();

    ordinary
        .documents()
        .put_with(
            "leases",
            "expired",
            serde_json::json!({"epoch": 1}),
            Precondition::Absent,
            tinystoragedrivers_types::WriteOptions::new().expires_ms(1),
        )
        .await
        .unwrap();
    let expired = WriteCondition::new(
        guard_scope.clone(),
        "leases",
        "expired",
        Filter::eq("epoch", 1),
    )
    .unwrap();
    let expired_ports = backend
        .for_scope_conditioned(&target_scope, &expired)
        .unwrap();
    assert_eq!(
        expired_ports
            .documents()
            .put(
                "items",
                "refused",
                serde_json::json!({}),
                Precondition::None
            )
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Conflict
    );

    ordinary
        .documents()
        .put(
            "leases",
            "compressed",
            serde_json::json!({"epoch": 2, "padding": "x".repeat(32_000)}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    let compressed =
        WriteCondition::new(guard_scope, "leases", "compressed", Filter::eq("epoch", 2)).unwrap();
    let compressed_ports = backend
        .for_scope_conditioned(&target_scope, &compressed)
        .unwrap();
    compressed_ports
        .documents()
        .put(
            "items",
            "accepted",
            serde_json::json!({}),
            Precondition::None,
        )
        .await
        .unwrap();
    assert!(
        compressed_ports
            .documents()
            .get("items", "accepted")
            .await
            .unwrap()
            .is_some()
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(
    clippy::too_many_lines,
    reason = "the lock, queued write, clock advance, and refusal are one deterministic race regression"
)]
async fn sqlite_condition_expiry_uses_clock_after_waiting_for_the_connection() {
    use std::future::Future;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, mpsc};
    use std::time::Duration;

    let dir = tempfile::tempdir().unwrap();
    let now = Arc::new(AtomicU64::new(100));
    let clock_value = Arc::clone(&now);
    let connection = tinystoragedrivers_connect::sqlite::SqliteConnection::open_with_clock(
        dir.path().join("condition-clock.db"),
        Arc::new(move || clock_value.load(Ordering::SeqCst)),
    )
    .unwrap();
    let guard_scope = Scope::new("clock-guard").unwrap();
    let target_scope = Scope::new("clock-target").unwrap();
    let guard_documents = tinystoragedrivers_document::sqlite::SqliteDocuments::new(
        connection.clone(),
        guard_scope.clone(),
    );
    guard_documents
        .ensure_collection(&tinystoragedrivers_types::CollectionSpec::new("leases"))
        .await
        .unwrap();
    guard_documents
        .put_with(
            "leases",
            "writer",
            serde_json::json!({"epoch": 1}),
            Precondition::Absent,
            tinystoragedrivers_types::WriteOptions::new().expires_ms(200),
        )
        .await
        .unwrap();
    let condition =
        WriteCondition::new(guard_scope, "leases", "writer", Filter::eq("epoch", 1)).unwrap();
    let target = tinystoragedrivers_document::sqlite::SqliteDocuments::conditioned(
        connection.clone(),
        target_scope,
        condition,
    );
    let target_check = target.clone();
    target
        .ensure_collection(&tinystoragedrivers_types::CollectionSpec::new("items"))
        .await
        .unwrap();

    let (lock_started_tx, lock_started_rx) = mpsc::sync_channel(0);
    let (release_lock_tx, release_lock_rx) = mpsc::sync_channel(0);
    let blocker_connection = connection.clone();
    let blocker = tokio::spawn(async move {
        blocker_connection
            .run(move |database| {
                let transaction = database
                    .transaction_with_behavior(
                        tinystoragedrivers_connect::sqlite::rusqlite::TransactionBehavior::Immediate,
                    )
                    .map_err(tinystoragedrivers_connect::sqlite::error::during(
                        "start a test write lock",
                    ))?;
                lock_started_tx.send(()).unwrap();
                release_lock_rx.recv().unwrap();
                transaction
                    .commit()
                    .map_err(tinystoragedrivers_connect::sqlite::error::during(
                        "finish a test write lock",
                    ))
            })
            .await
    });
    lock_started_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();

    let (queued_tx, queued_rx) = tokio::sync::oneshot::channel();
    let writer = tokio::spawn(async move {
        let mut operation = Box::pin(target.put(
            "items",
            "too-late",
            serde_json::json!({"value": true}),
            Precondition::None,
        ));
        let mut queued_tx = Some(queued_tx);
        std::future::poll_fn(|context| {
            let polled = operation.as_mut().poll(context);
            if polled.is_pending()
                && let Some(queued_tx) = queued_tx.take()
            {
                queued_tx.send(()).unwrap();
            }
            polled
        })
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), queued_rx)
        .await
        .unwrap()
        .unwrap();
    now.store(300, Ordering::SeqCst);
    release_lock_tx.send(()).unwrap();
    blocker.await.unwrap().unwrap();
    assert_eq!(
        writer.await.unwrap().unwrap_err().kind(),
        ErrorKind::Conflict
    );
    assert!(
        target_check
            .get("items", "too-late")
            .await
            .unwrap()
            .is_none()
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_conditioned_kv_checks_expiry_inside_its_write_transaction() {
    use std::future::Future;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, mpsc};
    use std::time::Duration;

    let dir = tempfile::tempdir().unwrap();
    let now = Arc::new(AtomicU64::new(100));
    let clock_value = Arc::clone(&now);
    let connection = tinystoragedrivers_connect::sqlite::SqliteConnection::open_with_clock(
        dir.path().join("conditioned-kv-clock.db"),
        Arc::new(move || clock_value.load(Ordering::SeqCst)),
    )
    .unwrap();
    let guard_scope = Scope::new("kv-clock-guard").unwrap();
    let target_scope = Scope::new("kv-clock-target").unwrap();
    let guard_documents = tinystoragedrivers_document::sqlite::SqliteDocuments::new(
        connection.clone(),
        guard_scope.clone(),
    );
    guard_documents
        .ensure_collection(&tinystoragedrivers_types::CollectionSpec::new("leases"))
        .await
        .unwrap();
    guard_documents
        .put_with(
            "leases",
            "writer",
            serde_json::json!({"epoch": 1}),
            Precondition::Absent,
            tinystoragedrivers_types::WriteOptions::new().expires_ms(200),
        )
        .await
        .unwrap();
    let condition =
        WriteCondition::new(guard_scope, "leases", "writer", Filter::eq("epoch", 1)).unwrap();
    let target =
        tinystoragedrivers_kv::SqliteKv::conditioned(connection.clone(), target_scope, condition)
            .unwrap();

    let (lock_started_tx, lock_started_rx) = mpsc::sync_channel(0);
    let (release_lock_tx, release_lock_rx) = mpsc::sync_channel(0);
    let blocker_connection = connection.clone();
    let blocker = tokio::spawn(async move {
        blocker_connection
            .run(move |database| {
                let transaction = database
                    .transaction_with_behavior(
                        tinystoragedrivers_connect::sqlite::rusqlite::TransactionBehavior::Immediate,
                    )
                    .map_err(tinystoragedrivers_connect::sqlite::error::during(
                        "start a KV test write lock",
                    ))?;
                lock_started_tx.send(()).unwrap();
                release_lock_rx.recv().unwrap();
                transaction
                    .commit()
                    .map_err(tinystoragedrivers_connect::sqlite::error::during(
                        "finish a KV test write lock",
                    ))
            })
            .await
    });
    lock_started_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();

    let (queued_tx, queued_rx) = tokio::sync::oneshot::channel();
    let writer = tokio::spawn(async move {
        let mut operation = Box::pin(target.put("state", serde_json::json!({"ok": true})));
        let mut queued_tx = Some(queued_tx);
        std::future::poll_fn(|context| {
            let polled = operation.as_mut().poll(context);
            if polled.is_pending()
                && let Some(queued_tx) = queued_tx.take()
            {
                queued_tx.send(()).unwrap();
            }
            polled
        })
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), queued_rx)
        .await
        .unwrap()
        .unwrap();
    now.store(300, Ordering::SeqCst);
    release_lock_tx.send(()).unwrap();
    blocker.await.unwrap().unwrap();
    assert_eq!(
        writer.await.unwrap().unwrap_err().kind(),
        ErrorKind::Conflict
    );
}

#[tokio::test]
async fn nonparticipating_backends_reject_conditioning_without_changing_ordinary_handles() {
    let backend = open(&StorageConfig::Memory).await.unwrap();
    let scope = Scope::new("ordinary-still-works").unwrap();
    let condition = WriteCondition::new(
        Scope::new("guard").unwrap(),
        "leases",
        "writer",
        Filter::eq("epoch", 1),
    )
    .unwrap();
    let error = backend
        .for_scope_conditioned(&scope, &condition)
        .unwrap_err();
    assert_eq!(
        error.kind(),
        ErrorKind::Unsupported(Capability::ConditionalWrites)
    );
    let ordinary = backend.for_scope(&scope).unwrap();
    ordinary
        .kv()
        .put("key", serde_json::json!(1))
        .await
        .unwrap();
    assert_eq!(
        ordinary.kv().get("key").await.unwrap().unwrap().doc,
        serde_json::json!(1)
    );
}

#[cfg(feature = "mongodb")]
#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one live replica-set regression covers the conditioned port matrix and stale takeover"
)]
async fn mongo_conditioned_ports_check_the_guard_in_transaction_and_fail_closed_elsewhere() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let database = format!(
        "tsd_condition_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let backend = open(&StorageConfig::MongoDb {
        uri: uri.clone(),
        database: database.clone(),
    })
    .await
    .unwrap();
    let guard_scope = Scope::new("condition-guard").unwrap();
    let target_scope = Scope::new("condition-target").unwrap();
    let ordinary = backend.for_scope(&guard_scope).unwrap();
    ordinary
        .ensure_collection(&tinystoragedrivers_types::CollectionSpec::new("leases"))
        .await
        .unwrap();
    ordinary
        .ensure_collection(&tinystoragedrivers_types::CollectionSpec::new("items"))
        .await
        .unwrap();
    ordinary
        .ensure_collection(&tinystoragedrivers_types::CollectionSpec::new("drop-items"))
        .await
        .unwrap();
    let original = ordinary
        .documents()
        .put(
            "leases",
            "writer",
            serde_json::json!({"epoch": 1}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    let original_row = ordinary
        .documents()
        .get("leases", "writer")
        .await
        .unwrap()
        .unwrap();
    let condition = WriteCondition::new(
        guard_scope.clone(),
        "leases",
        "writer",
        Filter::eq("epoch", 1),
    )
    .unwrap();
    let held = backend
        .for_scope_conditioned(&target_scope, &condition)
        .unwrap();
    assert!(
        held.documents()
            .capabilities()
            .contains(Capability::ConditionalWrites)
    );
    assert!(
        held.streams()
            .capabilities()
            .contains(Capability::ConditionalWrites)
    );
    assert!(!held.blobs().capabilities().contains(Capability::Blobs));
    assert!(held.kv().capabilities().contains(Capability::Kv));
    assert!(
        held.kv()
            .capabilities()
            .contains(Capability::ConditionalWrites)
    );
    assert_eq!(
        held.documents()
            .put(
                "items",
                "accepted",
                serde_json::json!({"value": 1}),
                Precondition::Absent
            )
            .await
            .unwrap(),
        tinystoragedrivers_types::Version::FIRST
    );
    held.streams()
        .append("events", serde_json::json!("accepted"))
        .await
        .unwrap();
    held.streams()
        .append("delete-before-takeover", serde_json::json!("accepted"))
        .await
        .unwrap();
    assert!(
        held.streams()
            .delete_stream("delete-before-takeover")
            .await
            .unwrap()
    );
    held.documents()
        .put(
            "drop-items",
            "before-takeover",
            serde_json::json!({"value": 1}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    held.documents()
        .drop_collection("drop-items")
        .await
        .unwrap();
    assert!(
        held.documents()
            .get("drop-items", "before-takeover")
            .await
            .unwrap()
            .is_none()
    );
    held.kv()
        .put("profile", serde_json::json!({"enabled": true}))
        .await
        .unwrap();
    held.queues()
        .enqueue(
            "jobs",
            &[tinystoragedrivers_types::NewMessage::new(
                serde_json::json!({"work": 1}),
            )],
        )
        .await
        .unwrap();

    let guard_after_accepted_writes = ordinary
        .documents()
        .get("leases", "writer")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(guard_after_accepted_writes.doc, original_row.doc);
    assert_eq!(guard_after_accepted_writes.version, original_row.version);
    assert_eq!(guard_after_accepted_writes.meta, original_row.meta);

    ordinary
        .documents()
        .put(
            "leases",
            "writer",
            serde_json::json!({"epoch": 2}),
            Precondition::Version(original),
        )
        .await
        .unwrap();
    let guard_after_takeover = ordinary
        .documents()
        .get("leases", "writer")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        held.documents()
            .put("items", "stale", serde_json::json!({}), Precondition::None)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Conflict
    );
    assert_eq!(
        held.streams()
            .append("events", serde_json::json!("stale"))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Conflict
    );
    assert_eq!(
        held.kv()
            .put("profile", serde_json::json!({"enabled": false}))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Conflict
    );
    assert_eq!(
        held.queues()
            .enqueue(
                "jobs",
                &[tinystoragedrivers_types::NewMessage::new(
                    serde_json::json!({"work": 2}),
                )],
            )
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Conflict
    );
    assert_eq!(
        held.streams()
            .truncate_before("events", 1)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Conflict
    );
    assert_eq!(held.streams().len("events").await.unwrap(), 1);
    assert!(
        held.documents()
            .get("items", "stale")
            .await
            .unwrap()
            .is_none()
    );
    let guard_after = ordinary
        .documents()
        .get("leases", "writer")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        guard_after_takeover.version,
        original_row.version.next().unwrap()
    );
    assert_eq!(guard_after.version, guard_after_takeover.version);
    assert_eq!(guard_after.meta, guard_after_takeover.meta);
    assert_eq!(guard_after.doc, serde_json::json!({"epoch": 2}));
    assert_eq!(
        held.streams()
            .delete_stream("events")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Conflict
    );
    assert_eq!(held.streams().len("events").await.unwrap(), 1);
    assert_eq!(
        held.documents()
            .drop_collection("items")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Conflict
    );
    assert_eq!(
        held.documents()
            .get("items", "accepted")
            .await
            .unwrap()
            .unwrap()
            .doc,
        serde_json::json!({"value": 1})
    );

    let connection = tinystoragedrivers_connect::MongoConnection::connect(&uri, &database)
        .await
        .unwrap();
    connection.database().drop().await.unwrap();
}

#[cfg(feature = "file")]
#[tokio::test]
async fn opens_a_file_url_with_the_file_feature() {
    let dir = tempfile::tempdir().unwrap();
    let config = StorageConfig::parse(&format!("file:{}", dir.path().display())).unwrap();
    let backend = open(&config).await.unwrap();
    let ports = backend
        .for_scope(&Scope::new("file-config-test").unwrap())
        .unwrap();
    ports.kv().put("probe", serde_json::json!(1)).await.unwrap();
    assert_eq!(
        ports.kv().get("probe").await.unwrap().unwrap().doc,
        serde_json::json!(1)
    );
    assert_eq!(config.driver(), "file");

    let file = dir.path().join("not-a-dir");
    std::fs::write(&file, b"x").unwrap();
    let blocked = StorageConfig::File { dir: file };
    let error = open(&blocked).await.map(|_| ()).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
}

#[test]
fn redaction_survives_unencoded_credentials_and_secret_options() {
    let slash = StorageConfig::parse("mongodb://app:p/a@ss@host/db").unwrap();
    let slash_shown = slash.to_string();
    assert!(!slash_shown.contains("p/a"), "{slash_shown}");
    assert!(!slash_shown.contains("ss"), "{slash_shown}");
    assert!(slash_shown.contains("***"), "{slash_shown}");
    assert_eq!(
        redact_url("mongodb://app:prefix&password=suffix@host/db"),
        "mongodb://app:***@host/db"
    );
    assert_eq!(
        redact_url("mongodb://app:credential@host/db?password=first@second"),
        "mongodb://app:***"
    );
    assert_eq!(
        redact_url("mongodb://host:27017/db?password=first@second"),
        "mongodb://host:***"
    );
    assert_eq!(
        redact_url("mongodb://host/db:part@name?x=1"),
        "mongodb://host/db:***@name?x=1"
    );
    let StorageConfig::MongoDb { database, .. } = &slash else {
        panic!("expected a MongoDB config")
    };
    assert_eq!(database, "db");

    let aws = StorageConfig::parse(
        "mongodb+srv://h/db?authMechanism=MONGODB-AWS&authMechanismProperties=AWS_SESSION_TOKEN:tok&retryWrites=true",
    )
    .unwrap();
    let shown = aws.to_string();
    assert!(!shown.contains("tok"), "{shown}");
    assert!(shown.contains("authMechanismProperties=***"), "{shown}");
    assert!(shown.contains("retryWrites=true"), "{shown}");

    assert_eq!(
        redact_url("mongodb://h/?tlsCertificateKeyFilePassword=pw&x"),
        "mongodb://h/?tlsCertificateKeyFilePassword=***&x"
    );
    assert_eq!(redact_url("mongodb://h?x=1"), "mongodb://h/?x=1");
    assert_eq!(
        redact_url("mongodb://h/db?appName=a@b&password=secret"),
        "mongodb://h/db?appName=a@b&password=***"
    );
    let ambiguous = redact_url("mongodb://app:p?assword@host/db?password=secret");
    assert!(!ambiguous.contains("p?assword"), "{ambiguous}");
    assert!(!ambiguous.contains("secret"), "{ambiguous}");
    assert!(ambiguous.contains("***"), "{ambiguous}");
    assert_eq!(
        redact_url("mongodb://h/db?password=first?second@host"),
        "mongodb://h/db?password=***"
    );
    assert_eq!(
        redact_url("mongodb://h/db?password=first?second&authMechanismProperties=token&x=1#frag"),
        "mongodb://h/db?password=***&authMechanismProperties=***&x=1#frag"
    );
    assert_eq!(
        redact_url(
            "mongodb+srv://app:p@ss@h/db?password=first?second&authMechanismProperties=token#frag"
        ),
        "mongodb+srv://app:***@h/db?password=***&authMechanismProperties=***"
    );
    let ambiguous = redact_url("mongodb://app:credential@host/db?appName=a:b@c&password=second");
    assert!(!ambiguous.contains("credential"), "{ambiguous}");
    assert!(!ambiguous.contains("second"), "{ambiguous}");
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn opens_sqlite_directories_and_files() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite:{}", dir.path().display());
    let backend = open(&StorageConfig::parse(&url).unwrap()).await.unwrap();
    let ports = backend
        .for_scope(&Scope::new("sqlite-directory-test").unwrap())
        .unwrap();
    ports.kv().put("probe", serde_json::json!(1)).await.unwrap();
    assert_eq!(
        ports.kv().get("probe").await.unwrap().unwrap().doc,
        serde_json::json!(1)
    );
    assert_eq!(StorageConfig::parse(&url).unwrap().driver(), "sqlite");
    assert!(dir.path().join("storage.db").exists());
    let file = dir.path().join("single.db");
    let backend = open(&StorageConfig::Sqlite { path: file.clone() })
        .await
        .unwrap();
    let ports = backend
        .for_scope(&Scope::new("sqlite-file-test").unwrap())
        .unwrap();
    ports.kv().put("probe", serde_json::json!(2)).await.unwrap();
    assert_eq!(
        ports.kv().get("probe").await.unwrap().unwrap().doc,
        serde_json::json!(2)
    );
    assert_eq!(
        StorageConfig::Sqlite { path: file.clone() }.driver(),
        "sqlite"
    );
    assert!(file.exists());
}

#[tokio::test]
async fn memory_reopening_a_scope_preserves_values_without_cross_scope_reads() {
    use serde_json::json;
    use tinystoragedrivers_types::{Precondition, Scope};

    let backend = open(&StorageConfig::Memory).await.unwrap();
    let local = Scope::new("local").unwrap();
    let other = Scope::new("other").unwrap();
    let first = backend.for_scope(&local).unwrap();
    first.kv().put("answer", json!(42)).await.unwrap();
    first
        .documents()
        .put(
            "notes",
            "one",
            json!({"body": "private"}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    first
        .blobs()
        .put("image", b"local".to_vec(), None)
        .await
        .unwrap();
    first.secrets().set("token", b"private").await.unwrap();

    let reopened = backend.for_scope(&local).unwrap();
    assert_eq!(
        reopened.kv().get("answer").await.unwrap().unwrap().doc,
        json!(42)
    );
    assert!(
        reopened
            .documents()
            .get("notes", "one")
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        reopened.blobs().get("image").await.unwrap().unwrap().bytes,
        b"local"
    );
    assert_eq!(
        reopened
            .secrets()
            .get("token")
            .await
            .unwrap()
            .unwrap()
            .as_slice(),
        b"private"
    );

    let isolated = backend.for_scope(&other).unwrap();
    assert!(isolated.kv().get("answer").await.unwrap().is_none());
    assert!(
        isolated
            .documents()
            .get("notes", "one")
            .await
            .unwrap()
            .is_none()
    );
    assert!(isolated.blobs().get("image").await.unwrap().is_none());
    assert!(isolated.secrets().get("token").await.unwrap().is_none());
}

#[test]
fn a_question_mark_in_a_password_stays_hidden() {
    let shown = redact_url("mongodb://app:p?assword@host/db");
    assert!(!shown.contains("assword"), "{shown}");
    assert!(shown.contains("***"), "{shown}");
    let error = StorageConfig::parse("mongodb://app:p?assword@host").unwrap_err();
    assert!(!error.to_string().contains("assword"), "{error}");
    let query_at = StorageConfig::parse("mongodb://h/db?appName=a@b").unwrap();
    let StorageConfig::MongoDb { database, .. } = &query_at else {
        panic!("expected a MongoDB config")
    };
    assert_eq!(database, "db", "an @ in a query value is not userinfo");
}

#[test]
fn redaction_merges_original_credential_spans_across_uri_components() {
    let cases = [
        (
            "mongodb://user:credential@host:27017/db:part@name?password=first@second&x=kept#frag",
            "mongodb://user:***&x=kept#frag",
        ),
        (
            "mongodb://user:prefix&password=suffix@host/db?appName=a@b&password=secret",
            "mongodb://user:***@b&password=***",
        ),
        (
            "mongodb+srv://user:credential@host/db?authMechanismProperties=token&tlsCertificateKeyFilePassword=other#safe",
            "mongodb+srv://user:***@host/db?authMechanismProperties=***&tlsCertificateKeyFilePassword=***",
        ),
        (
            "mongodb://host:27017/db?password=first?second@third&x=kept#frag",
            "mongodb://host:***&x=kept#frag",
        ),
    ];

    for (input, expected) in cases {
        let output = redact_url(input);
        assert_eq!(output, expected, "input: {input}");
        for sentinel in [
            "credential",
            "suffix",
            "secret",
            "token",
            "other",
            "first",
            "second",
        ] {
            assert!(
                !output.contains(sentinel),
                "{input} leaked {sentinel}: {output}"
            );
        }
    }
}

#[test]
fn redaction_keeps_ambiguous_reserved_delimiters_inside_credentials_hidden() {
    for input in [
        "mongodb://app:123?secret@host/db",
        "mongodb://app:123/secret@host/db",
        "mongodb://app:123#secret@host/db",
        "mongodb://app:314159@middle/secretRight@host/db",
        "mongodb://app:314159@middle?secretRight@host/db",
        "mongodb://app:314159@middle#secretRight@host/db",
    ] {
        assert_eq!(redact_url(input), "mongodb://app:***@host/db", "{input}");
    }
}

#[test]
fn redaction_hides_multiple_at_passwords_across_reserved_delimiters() {
    for delimiter in ["/", "?", "#"] {
        let input = format!(
            "mongodb://app:314159@middle{delimiter}secretRight@host/db?password=querySecret@other&x=kept"
        );
        let output = redact_url(&input);
        assert!(!output.contains("314159"), "{input}: {output}");
        assert!(!output.contains("secretRight"), "{input}: {output}");
        assert!(!output.contains("querySecret"), "{input}: {output}");
        assert!(output.contains("***"), "{input}: {output}");
    }
}

#[test]
fn redaction_keeps_secret_query_spans_through_fragment_markers() {
    let cases = [
        (
            "mongodb://h/db?password=first#secretRight",
            "mongodb://h/db?password=***",
        ),
        (
            "mongodb://h/db#fragment?password=secretRight",
            "mongodb://h/db#fragment?password=***",
        ),
    ];
    for (input, expected) in cases {
        let output = redact_url(input);
        assert_eq!(output, expected, "{input}");
        assert!(!output.contains("secretRight"), "{input}: {output}");
    }
}

#[test]
fn redaction_matches_percent_encoded_secret_option_names() {
    let input = "mongodb://h/db?tlsCertificateKeyFilePass%77ord=querySecret";
    let output = redact_url(input);
    assert_eq!(output, "mongodb://h/db?tlsCertificateKeyFilePass%77ord=***");
    assert!(!output.contains("querySecret"));
}

#[test]
fn proxy_password_is_redacted_from_every_public_url_representation() {
    for key in ["proxyPassword", "PROXYPASSWORD", "proxy%50assword"] {
        let input = format!("mongodb://h/db?{key}=proxySecretValue");
        let output = redact_url(&input);
        assert_eq!(output, format!("mongodb://h/db?{key}=***"));
        assert!(!output.contains("proxySecretValue"), "{output}");

        let config = StorageConfig::parse(&input).unwrap();
        assert!(!config.to_string().contains("proxySecretValue"));
        assert!(!format!("{config:?}").contains("proxySecretValue"));

        let missing_database = format!("mongodb://h/?{key}=proxySecretValue");
        let error = StorageConfig::parse(&missing_database).unwrap_err();
        assert!(!error.to_string().contains("proxySecretValue"), "{error}");
        assert!(
            !format!("{error:?}").contains("proxySecretValue"),
            "{error:?}"
        );
    }
}

#[test]
fn secret_query_scanning_has_linear_work_for_dense_query_markers() {
    let prefix = "mongodb://h/db";
    let input = format!("{prefix}{}password=linearSecret", "?".repeat(4_096));
    let mut scanned_bytes = 0;
    let spans = scan_secret_query_value_spans(&input, || scanned_bytes += 1);
    let value_start = input.len() - "linearSecret".len();
    assert_eq!(spans.len(), 1);
    assert_eq!(spans[0], value_start..input.len());
    assert!(
        scanned_bytes <= input.len() * 512,
        "visited {scanned_bytes} input bytes for {} URI bytes",
        input.len()
    );
}

#[test]
fn redaction_does_not_let_secret_query_scrubbing_expose_ambiguous_userinfo() {
    for input in [
        "mongodb://app:123?password=suffix@host/db",
        "mongodb://app:prefix?password=suffix@host/db",
    ] {
        let output = redact_url(input);
        assert!(output.starts_with("mongodb://app:***"), "{input}: {output}");
        assert!(!output.contains("123"), "{output}");
        assert!(!output.contains("prefix"), "{output}");
        assert!(!output.contains("suffix"), "{output}");
    }
}

#[cfg(feature = "mongodb")]
#[tokio::test]
async fn opens_a_mongodb_url_with_the_driver() {
    // A port that is not a number fails the driver's own URI parse before
    // any I/O, so this proves the URL reaches the driver without a network.
    let config = StorageConfig::parse("mongodb://app:s3cret@h:notaport/db").unwrap();
    let error = open(&config).await.map(|_| ()).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(
        !error.message().contains("not available in this build"),
        "the driver, not the missing-feature arm, answered: {error}"
    );
    assert!(!error.to_string().contains("s3cret"), "{error}");
}

#[cfg(feature = "mongodb")]
#[tokio::test]
async fn facade_opens_a_live_mongodb_backend_and_binds_scoped_ports() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let database = format!(
        "tsd_facade_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let config = StorageConfig::MongoDb {
        uri: uri.clone(),
        database: database.clone(),
    };
    let backend = open(&config).await.unwrap();
    let scope = Scope::new(&format!("facade-{}", std::process::id())).unwrap();
    let ports = backend.for_scope(&scope).unwrap();
    ports
        .kv()
        .put("facade-probe", serde_json::json!({"value": 7}))
        .await
        .unwrap();
    assert_eq!(
        ports.kv().get("facade-probe").await.unwrap().unwrap().doc,
        serde_json::json!({"value": 7})
    );
    let connection = tinystoragedrivers_connect::MongoConnection::connect(&uri, &database)
        .await
        .unwrap();
    connection.database().drop().await.unwrap();
}
