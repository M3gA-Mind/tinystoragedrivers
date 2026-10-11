//! Native schema policy remains sticky and performs no lazy DDL.

use super::*;
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
        )),
        databases: Arc::default(),
    };
    let restricted = storage
        .existing_schema()
        .unwrap()
        .database("sessions")
        .unwrap();
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
        if let CommandEvent::Started(event) = event {
            if ["create", "createIndexes", "dropIndexes", "drop", "collMod"]
                .contains(&event.command_name.as_str())
            {
                observed.fetch_add(1, Ordering::SeqCst);
            }
        }
    }));
    let client = Client::with_options(options).unwrap();
    let db = client.database(&format!("existing_schema_{}", std::process::id()));
    db.drop().await.unwrap();
    let storage = MongoStorage {
        shared: Arc::new(Shared::new(
            client.clone(),
            db.clone(),
            String::new(),
            Arc::new(|| 0),
            true,
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
    let fresh = storage.rebuild(Arc::new(|| 0), true);
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
