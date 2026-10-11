//! Native schema policy remains sticky and performs no lazy DDL.

use super::*;
use futures_util::TryStreamExt;
use mongodb::event::{EventHandler, command::CommandEvent};
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use tinystoragedrivers_core::{CollectionSpec, ErrorKind, Filter, Precondition};

#[tokio::test]
async fn existing_schema_refuses_declaration_before_backend_network() {
    let client = Client::with_uri_str("mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=10")
        .await
        .unwrap();
    let storage = MongoStorage {
        shared: Arc::new(Shared::new(
            client.clone(),
            client.database("no_network"),
            String::new(),
            Arc::new(|| 0),
            true,
            true,
        )),
        databases: Arc::default(),
    };
    let restricted = storage
        .existing_schema()
        .unwrap()
        .database("sessions")
        .unwrap();
    assert!(restricted.prepare_schema().await.is_err());
    let fence = Fence::new(Scope::local(), "leases", "l", Filter::All);
    let docs = restricted
        .for_scope_fenced(&Scope::local(), &fence)
        .unwrap()
        .documents()
        .clone();
    assert_eq!(
        docs.ensure_collection(&CollectionSpec::new("notes"))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::ExistingSchema)
    );
}

#[tokio::test]
async fn live_existing_schema_fresh_handles_do_not_issue_ddl() {
    let Ok(url) = std::env::var("TSD_MONGO_URL") else {
        eprintln!(
            "live_existing_schema_fresh_handles_do_not_issue_ddl: no local fixture configured"
        );
        return;
    };
    let ddl = Arc::new(AtomicU64::new(0));
    let observed = ddl.clone();
    let mut options = ClientOptions::parse(url).await.unwrap();
    options.command_event_handler = Some(EventHandler::callback(move |event| {
        if let CommandEvent::Started(event) = event
            && ["create", "createIndexes", "dropIndexes", "drop", "collMod"]
                .contains(&event.command_name.as_str())
        {
            observed.fetch_add(1, Ordering::SeqCst);
        }
    }));
    let client = Client::with_options(options).unwrap();
    let hello = client
        .database("admin")
        .run_command(doc! {"hello": 1})
        .await
        .unwrap();
    let transactions = supports_transactions(&hello);
    let db = client.database(&format!("existing_schema_{}", std::process::id()));
    db.drop().await.unwrap();
    let storage = MongoStorage {
        shared: Arc::new(Shared::new(
            client.clone(),
            db.clone(),
            String::new(),
            Arc::new(|| 0),
            transactions,
            transactions,
        )),
        databases: Arc::default(),
    };
    let named = storage.database("sessions").unwrap();
    named.prepare_schema().await.unwrap();
    let scope = Scope::new("alice").unwrap();
    let spec = CollectionSpec::new("notes")
        .ttl("until")
        .searchable(["body"]);
    let trusted = named.for_scope(&scope).unwrap();
    trusted.documents().ensure_collection(&spec).await.unwrap();
    trusted.streams().append("s", json!(0)).await.unwrap();
    trusted.blobs().put("b", vec![1, 2], None).await.unwrap();
    // Rebuild native caches so classic preparation cannot mask lazy DDL.
    let fresh = storage.rebuild(Arc::new(|| 0), transactions);
    let restricted = fresh
        .existing_schema()
        .unwrap()
        .database("sessions")
        .unwrap();
    let actor = restricted.for_scope(&scope).unwrap();
    ddl.store(0, Ordering::SeqCst);
    actor
        .documents()
        .validate_collection_spec(&spec)
        .await
        .unwrap();
    actor
        .documents()
        .put("notes", "n", json!({"body": "hello"}), Precondition::None)
        .await
        .unwrap();
    actor.documents().drop_collection("notes").await.unwrap();
    actor.streams().append("s", json!(1)).await.unwrap();
    assert_eq!(actor.blobs().get("b").await.unwrap().unwrap().bytes, [1, 2]);
    assert_eq!(ddl.load(Ordering::SeqCst), 0);
    assert_eq!(
        actor
            .documents()
            .ensure_collection(&spec)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::ExistingSchema)
    );
    db.drop().await.unwrap();
}

#[tokio::test]
async fn live_existing_schema_missing_and_corrupt_metadata_never_repairs() {
    let Ok(url) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let storage = MongoStorage::connect(&url, &format!("schema_negative_{}", std::process::id()))
        .await
        .unwrap();
    let restricted = storage.existing_schema().unwrap();
    let docs = restricted
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    assert_eq!(
        docs.validate_collection_spec(&CollectionSpec::new("absent"))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotFound
    );
    let spec = CollectionSpec::new("notes");
    storage
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .ensure_collection(&spec)
        .await
        .unwrap();
    storage
        .shared
        .raw("notes")
        .drop_index(crate::naming::SCOPE_KEY_INDEX)
        .await
        .unwrap();
    assert_eq!(
        docs.validate_collection_spec(&spec)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    // Validation must leave the missing index absent.
    let indexes: Vec<_> = storage
        .shared
        .raw("notes")
        .list_indexes()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert!(!indexes.iter().any(|index| {
        index
            .options
            .as_ref()
            .and_then(|options| options.name.as_deref())
            == Some(crate::naming::SCOPE_KEY_INDEX)
    }));
    storage
        .shared
        .raw(crate::naming::META)
        .update_one(doc! {"_id": "notes"}, doc! {"$set": {"spec": "not json"}})
        .await
        .unwrap();
    let error = docs.validate_collection_spec(&spec).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Serialization);
    assert!(std::error::Error::source(&error).is_some());
    assert_eq!(
        restricted
            .database("absent")
            .unwrap()
            .for_scope(&Scope::local())
            .unwrap()
            .streams()
            .append("s", json!(0))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotFound
    );
    storage.shared.db.drop().await.unwrap();
}

#[tokio::test]
async fn live_existing_schema_keeps_captured_native_grant() {
    let Ok(url) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let storage = MongoStorage::connect(&url, &format!("schema_fence_{}", std::process::id()))
        .await
        .unwrap();
    let cluster = Scope::new("cluster").unwrap();
    let scope = Scope::new("alice").unwrap();
    let leases = storage.for_scope(&cluster).unwrap().documents().clone();
    leases
        .ensure_collection(&CollectionSpec::new("leases"))
        .await
        .unwrap();
    leases
        .put("leases", "alice", json!({"epoch": 1}), Precondition::None)
        .await
        .unwrap();
    let named = storage.database("sessions").unwrap();
    named.prepare_schema().await.unwrap();
    named
        .for_scope(&scope)
        .unwrap()
        .documents()
        .ensure_collection(&CollectionSpec::new("notes"))
        .await
        .unwrap();
    let fence = Fence::new(cluster, "leases", "alice", Filter::eq("epoch", 1));
    let restricted = storage
        .existing_schema()
        .unwrap()
        .database("sessions")
        .unwrap();
    let actor = restricted
        .for_scope_fenced_in_database(
            &scope,
            &fence,
            &tinystoragedrivers_core::GuardDatabase::Default,
        )
        .unwrap();
    actor
        .documents()
        .put("notes", "n", json!({}), Precondition::None)
        .await
        .unwrap();
    leases
        .put("leases", "alice", json!({"epoch": 2}), Precondition::None)
        .await
        .unwrap();
    assert_eq!(
        actor
            .documents()
            .drop_collection("notes")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Fenced
    );
    assert_eq!(
        actor
            .documents()
            .atomic_batch(vec![])
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Fenced
    );
    assert_eq!(
        actor
            .streams()
            .append_batch_if_offset("s", 0, vec![])
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Fenced
    );
    assert_eq!(
        actor
            .blobs()
            .put("b", vec![1], None)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::Fencing)
    );
    storage.shared.db.drop().await.unwrap();
}

#[tokio::test]
async fn live_existing_schema_missing_validation_registry_is_not_created() {
    let Ok(url) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let storage = MongoStorage::connect(&url, &format!("schema_registry_{}", std::process::id()))
        .await
        .unwrap();
    if !storage.shared.transactions {
        return;
    }
    storage
        .shared
        .raw("_tsd_validated_batch")
        .drop()
        .await
        .unwrap();
    let docs = storage
        .existing_schema()
        .unwrap()
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    let result = docs.atomic_batch_checked(Vec::new(), Vec::new()).await;
    let absent = !storage
        .shared
        .collection_exists("_tsd_validated_batch")
        .await
        .unwrap();
    storage.shared.db.drop().await.unwrap();
    assert!(
        absent,
        "restricted mutation implicitly created internal schema"
    );
    assert_eq!(result.unwrap_err().kind(), ErrorKind::NotFound);
}
