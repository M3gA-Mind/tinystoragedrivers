use super::{MongoConnection, MongoConnectionOptions, supports_transactions, validate_database};
use mongodb::bson::doc;
use std::sync::Arc;

#[test]
fn mongo_database_names_match_server_constraints() {
    for name in ["openhuman", "db-name"] {
        assert!(validate_database(name).is_ok(), "{name:?}");
    }
    assert!(validate_database(&"a".repeat(63)).is_ok());
    for name in [
        "",
        "bad/name",
        "bad\\name",
        "bad.name",
        "bad name",
        "bad\0name",
        "bad$name",
        "bad*name",
        "bad<name",
        "bad>name",
        "bad:name",
        "bad|name",
        "bad?name",
    ] {
        assert!(validate_database(name).is_err(), "{name:?}");
    }
    assert!(validate_database(&"a".repeat(64)).is_err());
}

#[tokio::test]
async fn connects_to_replica_set_and_exposes_shared_database_and_clock() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let database = format!("tsd26_connect_{}", std::process::id());
    let connection = MongoConnection::connect_with_options(
        &uri,
        &database,
        MongoConnectionOptions {
            require_transactions: true,
        },
    )
    .await
    .unwrap();
    let clone = connection.clone();
    assert_eq!(connection.database().name(), database);
    assert_eq!(connection.client().database(&database).name(), database);
    assert!(format!("{connection:?}").contains("MongoConnection"));
    assert!(connection.transactions_available());
    assert!(clone.transactions_available());
    assert_eq!(connection.origin(), clone.origin());
    assert_eq!(connection.origin().to_hex().len(), 24);
    assert!(connection.options().require_transactions);

    let fixed = connection.clone().with_clock(Arc::new(|| 987));
    assert_eq!((fixed.clock())(), 987);
    assert!((connection.clock())() > 1_000);

    connection.database().drop().await.unwrap();
}

#[test]
fn transaction_support_requires_sessions_and_a_router_or_replica_set() {
    assert!(!supports_transactions(&doc! {}));
    assert!(!supports_transactions(&doc! {
        "logicalSessionTimeoutMinutes": 30,
    }));
    assert!(supports_transactions(&doc! {
        "logicalSessionTimeoutMinutes": 30,
        "setName": "rs0",
    }));
    assert!(supports_transactions(&doc! {
        "logicalSessionTimeoutMinutes": 30,
        "msg": "isdbgrid",
    }));
}

#[tokio::test]
async fn rejects_malformed_uri_before_connecting_and_bad_database_names() {
    let malformed = MongoConnection::connect("not a URI", "valid")
        .await
        .unwrap_err();
    assert_eq!(
        malformed.kind(),
        tinystoragedrivers_types::ErrorKind::InvalidInput
    );
    let invalid_database = MongoConnection::connect("mongodb://127.0.0.1:1", "bad/name")
        .await
        .unwrap_err();
    assert_eq!(
        invalid_database.kind(),
        tinystoragedrivers_types::ErrorKind::InvalidInput
    );
}

#[tokio::test]
async fn unreachable_server_errors_are_sanitized() {
    let error = MongoConnection::connect(
        "mongodb://user:secret@127.0.0.1:1/?directConnection=true&serverSelectionTimeoutMS=100",
        "valid",
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.kind(),
        tinystoragedrivers_types::ErrorKind::Unavailable
    );
    assert!(!error.to_string().contains("secret"));
    assert!(error.to_string().contains("reach the server"));
}
