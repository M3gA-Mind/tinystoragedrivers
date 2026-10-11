//! Native change-stream regressions against an explicitly supplied fixture.
mod support;
use serde_json::json;
use std::time::Duration;
use tinystoragedrivers_core::{
    Filter, Precondition, Scope, StorageBackend, WatchNotice, WatchOptions,
};

#[tokio::test]
async fn live_watch_subscribe_before_read_sees_insert_and_filter_exit() {
    let Some(backend) = support::connect().await else {
        return support::skip("live_watch_subscribe_before_read");
    };
    let name = format!("watch_{}", std::process::id());
    let db = backend.database(&name).unwrap();
    let a = db.for_scope(&Scope::new("alice").unwrap()).unwrap();
    let independent = db.for_scope(&Scope::new("alice").unwrap()).unwrap();
    let bob = db.for_scope(&Scope::new("bob").unwrap()).unwrap();
    let options = WatchOptions {
        interval: Duration::from_secs(10),
        ..WatchOptions::default()
    };
    let mut watch = a
        .documents()
        .watch("tasks", &Filter::eq("state", "queued"), &options)
        .await
        .unwrap();
    assert!(a.documents().get("tasks", "new").await.unwrap().is_none());
    bob.documents()
        .put(
            "tasks",
            "foreign",
            json!({"state":"queued"}),
            Precondition::None,
        )
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), watch.next())
            .await
            .is_err()
    );
    independent
        .documents()
        .put(
            "tasks",
            "new",
            json!({"state":"queued"}),
            Precondition::None,
        )
        .await
        .unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(3), watch.next())
            .await
            .unwrap()
            .unwrap(),
        Some(WatchNotice::Invalidated { .. })
    ));
    independent
        .documents()
        .put("tasks", "new", json!({"state":"done"}), Precondition::None)
        .await
        .unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(3), watch.next())
            .await
            .unwrap()
            .unwrap(),
        Some(WatchNotice::Invalidated { .. })
    ));
    independent
        .documents()
        .delete("tasks", "new", Precondition::None)
        .await
        .unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(3), watch.next())
            .await
            .unwrap()
            .unwrap(),
        Some(WatchNotice::Invalidated { .. })
    ));
    a.documents().drop_collection("tasks").await.unwrap();
    bob.documents().drop_collection("tasks").await.unwrap();
}

#[tokio::test]
async fn live_watch_cursor_binding_refuses_cross_scope_database_filter_and_context() {
    use tinystoragedrivers_core::ErrorKind;
    let Some(backend) = support::connect().await else {
        return support::skip("live_watch_cursor_binding");
    };
    let db = backend
        .database(&format!("cursor_{}", std::process::id()))
        .unwrap();
    let a = db.for_scope(&Scope::new("alice").unwrap()).unwrap();
    let docs = a.documents();
    let filter = Filter::eq("state", "queued");
    let options = WatchOptions {
        interval: Duration::from_secs(10),
        ..WatchOptions::default()
    };
    let mut watch = docs.watch("tasks", &filter, &options).await.unwrap();
    docs.put(
        "tasks",
        "one",
        json!({"state":"queued"}),
        Precondition::None,
    )
    .await
    .unwrap();
    let notice = tokio::time::timeout(Duration::from_secs(3), watch.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let WatchNotice::Invalidated {
        cursor: Some(cursor),
    } = notice
    else {
        panic!("native cursor missing");
    };
    let resumed = WatchOptions {
        after: Some(cursor),
        ..options.clone()
    };
    let mut continuation = docs.watch("tasks", &filter, &resumed).await.unwrap();
    docs.put(
        "tasks",
        "two",
        json!({"state":"queued"}),
        Precondition::None,
    )
    .await
    .unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(3), continuation.next())
            .await
            .unwrap()
            .unwrap(),
        Some(WatchNotice::Invalidated { .. })
    ));
    let b = db.for_scope(&Scope::new("bob").unwrap()).unwrap();
    let other_db = backend
        .database(&format!("other_{}", std::process::id()))
        .unwrap()
        .for_scope(&Scope::new("alice").unwrap())
        .unwrap();
    for (handle, collection, query) in [
        (&b, "tasks", filter.clone()),
        (&other_db, "tasks", filter.clone()),
        (&a, "other", filter.clone()),
        (&a, "tasks", Filter::All),
    ] {
        assert_eq!(
            handle
                .documents()
                .watch(collection, &query, &resumed)
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidInput
        );
    }
    let fresh = support::connect().await.unwrap();
    let same_db_new_context = fresh
        .database(&format!("cursor_{}", std::process::id()))
        .unwrap()
        .for_scope(&Scope::new("alice").unwrap())
        .unwrap();
    assert_eq!(
        same_db_new_context
            .documents()
            .watch("tasks", &filter, &resumed)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    docs.drop_collection("tasks").await.unwrap();
}

#[tokio::test]
async fn live_watch_stale_fence_refuses_open_delivery_and_cursor_rebinding() {
    use tinystoragedrivers_core::{ErrorKind, Fence};
    let Some(backend) = support::connect().await else {
        return support::skip("live_watch_stale_fence");
    };
    let db = backend
        .database(&format!("fenced_watch_{}", std::process::id()))
        .unwrap();
    let cluster = db.for_scope(&Scope::local()).unwrap();
    cluster
        .documents()
        .put("leases", "profile", json!({"epoch":1}), Precondition::None)
        .await
        .unwrap();
    let fence = Fence::new(Scope::local(), "leases", "profile", Filter::eq("epoch", 1));
    let scoped = db
        .for_scope_fenced(&Scope::new("alice").unwrap(), &fence)
        .unwrap();
    let mut watch = scoped
        .documents()
        .watch("tasks", &Filter::All, &WatchOptions::default())
        .await
        .unwrap();
    scoped
        .documents()
        .put("tasks", "one", json!({"x":1}), Precondition::None)
        .await
        .unwrap();
    let WatchNotice::Invalidated {
        cursor: Some(cursor),
    } = watch.next().await.unwrap().unwrap()
    else {
        panic!("native cursor missing");
    };
    cluster
        .documents()
        .put("leases", "profile", json!({"epoch":2}), Precondition::None)
        .await
        .unwrap();
    assert_eq!(watch.next().await.unwrap_err().kind(), ErrorKind::Fenced);
    assert_eq!(
        scoped
            .documents()
            .watch("tasks", &Filter::All, &WatchOptions::default())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Fenced
    );
    let fresh = db
        .for_scope_fenced(
            &Scope::new("alice").unwrap(),
            &Fence::new(Scope::local(), "leases", "profile", Filter::eq("epoch", 2)),
        )
        .unwrap();
    assert_eq!(
        fresh
            .documents()
            .watch(
                "tasks",
                &Filter::All,
                &WatchOptions {
                    after: Some(cursor),
                    ..WatchOptions::default()
                }
            )
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    fresh.documents().drop_collection("tasks").await.unwrap();
    cluster.documents().drop_collection("leases").await.unwrap();
}

#[tokio::test]
async fn live_watch_logical_expiry_invalidates_before_physical_sweep() {
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };
    use tinystoragedrivers_core::CollectionSpec;
    let Some(backend) = support::connect().await else {
        return support::skip("live_watch_logical_expiry");
    };
    let clock = Arc::new(AtomicU64::new(1_000));
    let time = Arc::clone(&clock);
    let backend = backend.with_clock(Arc::new(move || time.load(Ordering::SeqCst)));
    let db = backend
        .database(&format!("ttl_watch_{}", std::process::id()))
        .unwrap();
    let a = db.for_scope(&Scope::new("alice").unwrap()).unwrap();
    a.documents()
        .ensure_collection(&CollectionSpec::new("tasks").ttl("expires_at"))
        .await
        .unwrap();
    let options = WatchOptions {
        interval: Duration::from_millis(10),
        ..WatchOptions::default()
    };
    a.documents()
        .put(
            "tasks",
            "one",
            json!({"expires_at":2000}),
            Precondition::None,
        )
        .await
        .unwrap();
    let mut watch = a
        .documents()
        .watch("tasks", &Filter::All, &options)
        .await
        .unwrap();
    assert!(a.documents().get("tasks", "one").await.unwrap().is_some());
    clock.store(2_000, Ordering::SeqCst);
    assert!(matches!(
        watch.next().await.unwrap(),
        Some(WatchNotice::Invalidated { .. })
    ));
    assert!(a.documents().get("tasks", "one").await.unwrap().is_none());
    a.documents().drop_collection("tasks").await.unwrap();
}

#[tokio::test]
async fn live_watch_lost_history_requires_terminal_reset_with_native_source() {
    use std::error::Error;
    use tinystoragedrivers_core::{WatchCursor, WatchReset};
    let Some(backend) = support::connect().await else {
        return support::skip("live_watch_lost_history");
    };
    let db = backend
        .database(&format!("resume_loss_{}", std::process::id()))
        .unwrap();
    let scoped = db.for_scope(&Scope::new("alice").unwrap()).unwrap();
    let options = WatchOptions {
        interval: Duration::from_secs(10),
        ..WatchOptions::default()
    };
    let mut watch = scoped
        .documents()
        .watch("tasks", &Filter::All, &options)
        .await
        .unwrap();
    scoped
        .documents()
        .put("tasks", "one", json!({"x":1}), Precondition::None)
        .await
        .unwrap();
    let WatchNotice::Invalidated {
        cursor: Some(cursor),
    } = watch.next().await.unwrap().unwrap()
    else {
        panic!("native cursor missing");
    };
    // Preserve the authentic collection UUID and native token format, but move
    // its timestamp outside retained history. This changes no server state.
    let mut wire: serde_json::Value = serde_json::from_slice(cursor.as_bytes()).unwrap();
    let data = wire["token"]["_data"].as_str().unwrap();
    assert!(
        data.starts_with("82") && data.len() > 18,
        "unexpected native token format"
    );
    wire["token"]["_data"] = json!(format!("{}00000001{}", &data[..2], &data[10..]));
    let options = WatchOptions {
        after: Some(WatchCursor::new(serde_json::to_vec(&wire).unwrap()).unwrap()),
        ..options
    };
    let mut lost = scoped
        .documents()
        .watch("tasks", &Filter::All, &options)
        .await
        .unwrap();
    let notice = tokio::time::timeout(Duration::from_secs(3), lost.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let WatchNotice::ResetRequired {
        reason: WatchReset::ResumeLost,
        source: Some(error),
    } = notice
    else {
        panic!("expected native history-loss reset");
    };
    assert!(
        error
            .source()
            .unwrap()
            .downcast_ref::<mongodb::error::Error>()
            .is_some()
    );
    assert!(
        !format!(
            "{:?}",
            WatchNotice::ResetRequired {
                reason: WatchReset::ResumeLost,
                source: Some(error)
            }
        )
        .contains("_data")
    );
    assert!(lost.next().await.unwrap().is_none());
    scoped.documents().drop_collection("tasks").await.unwrap();
}

#[tokio::test]
async fn live_watch_collection_drop_requires_terminal_reset_and_fresh_subscription() {
    use tinystoragedrivers_core::WatchReset;
    let Some(backend) = support::connect().await else {
        return support::skip("live_watch_collection_drop");
    };
    let url = support::url().unwrap();
    let name = format!("drop_watch_{}", std::process::id());
    let db = backend.database(&name).unwrap();
    let scoped = db.for_scope(&Scope::new("alice").unwrap()).unwrap();
    scoped
        .documents()
        .put("tasks", "one", json!({"x":1}), Precondition::None)
        .await
        .unwrap();
    let options = WatchOptions {
        interval: Duration::from_secs(10),
        ..WatchOptions::default()
    };
    let mut watch = scoped
        .documents()
        .watch("tasks", &Filter::All, &options)
        .await
        .unwrap();
    let client = mongodb::Client::with_uri_str(&url).await.unwrap();
    // Native namespace destruction, rather than scoped DocumentStore deletion.
    // This fixture owns this exact prefix and never drops the shared database.
    client
        .database(&support::database(&url))
        .collection::<mongodb::bson::Document>(&format!("{name}:tasks"))
        .drop()
        .await
        .unwrap();
    let notice = tokio::time::timeout(Duration::from_secs(3), watch.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(
        notice,
        WatchNotice::ResetRequired {
            reason: WatchReset::Invalidated,
            ..
        }
    ));
    assert!(watch.next().await.unwrap().is_none());
    drop(watch);
    let mut fresh = scoped
        .documents()
        .watch("tasks", &Filter::All, &options)
        .await
        .unwrap();
    scoped
        .documents()
        .put("tasks", "two", json!({"x":2}), Precondition::None)
        .await
        .unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(3), fresh.next())
            .await
            .unwrap()
            .unwrap(),
        Some(WatchNotice::Invalidated { .. })
    ));
    drop(fresh);
    scoped.documents().drop_collection("tasks").await.unwrap();
}

#[tokio::test]
async fn live_standalone_watch_is_typed_unsupported() {
    use tinystoragedrivers_core::{Capability, ErrorKind};
    let Ok(url) = std::env::var("TSD_MONGO_STANDALONE_URL") else {
        return;
    };
    let backend = tinystoragedrivers_mongodb::MongoStorage::connect(
        &url,
        &format!("watch_{}", std::process::id()),
    )
    .await
    .unwrap();
    assert!(!backend.capabilities().contains(Capability::Watch));
    let scoped = backend.for_scope(&Scope::new("alice").unwrap()).unwrap();
    assert!(
        !scoped
            .documents()
            .capabilities()
            .contains(Capability::Watch)
    );
    assert_eq!(
        scoped
            .documents()
            .watch("tasks", &Filter::All, &WatchOptions::default())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::Watch)
    );
}
