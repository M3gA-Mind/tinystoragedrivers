//! Resume loss classification and payload-free watch projection.

use super::*;

#[tokio::test]
async fn watch_debug_and_terminal_states_do_not_expose_cursor_material() {
    let mut watch = MongoWatch {
        stream: None,
        pending: Some(WatchNotice::ResetRequired {
            reason: WatchReset::ResumeLost,
            source: None,
        }),
        cursor: None,
        binding: [7; 32],
        interval: std::time::Duration::from_millis(1),
        terminal: false,
    };
    let debug = format!("{watch:?}");
    assert!(debug.contains("terminal: false"));
    assert!(!debug.contains("binding"));
    assert!(matches!(
        watch.next().await.unwrap(),
        Some(WatchNotice::ResetRequired {
            reason: WatchReset::ResumeLost,
            ..
        })
    ));
    assert!(watch.terminal);
    assert!(watch.next().await.unwrap().is_none());
}

#[tokio::test]
async fn watch_without_pending_notice_requires_a_native_cursor() {
    let mut watch = MongoWatch {
        stream: None,
        pending: None,
        cursor: None,
        binding: [0; 32],
        interval: std::time::Duration::from_millis(1),
        terminal: false,
    };
    assert_eq!(
        watch.next().await.unwrap_err().kind(),
        tinystoragedrivers_types::ErrorKind::Backend
    );
}

#[test]
fn pipeline_scopes_document_keys_and_projects_no_values() {
    let pipeline = pipeline("scope");
    assert_eq!(
        pipeline[0],
        doc! {"$match": {"$or": [
            {"documentKey._id.s": "scope"},
            {"operationType": {"$in": ["drop", "rename", "dropDatabase", "invalidate"]}}
        ]}}
    );
    assert_eq!(
        pipeline[1],
        doc! {"$project": {"_id": 1, "operationType": 1}}
    );
}

#[test]
fn resume_loss_codes_are_distinguished_from_other_driver_errors() {
    for code in [136, 260, 280, 286, 91] {
        let command: mongodb::error::CommandError =
            serde_json::from_value(serde_json::json!({"code": code})).unwrap();
        let error: mongodb::error::Error = mongodb::error::ErrorKind::Command(command).into();
        assert_eq!(resume_lost(&error), code != 91);
    }
    assert!(!resume_lost(&mongodb::error::Error::custom("unrelated")));
}

#[test]
fn malformed_and_misbound_cursors_fail_without_disclosing_contents() {
    let invalid = WatchCursor::new(b"secret-invalid-token".to_vec()).unwrap();
    let error = decode(&invalid, [0; 32]).unwrap_err();
    assert_eq!(
        error.kind(),
        tinystoragedrivers_types::ErrorKind::InvalidInput
    );
    assert!(!error.to_string().contains("secret"));

    let oversized = WatchCursor::new(vec![
        1;
        tinystoragedrivers_types::MAX_WATCH_CURSOR_BYTES + 1
    ])
    .unwrap_err();
    assert_eq!(
        oversized.kind(),
        tinystoragedrivers_types::ErrorKind::InvalidInput
    );

    let oversized: WatchCursor = serde_json::from_value(serde_json::json!(vec![
        1;
        tinystoragedrivers_types::MAX_WATCH_CURSOR_BYTES
            + 1
    ]))
    .unwrap();
    assert_eq!(
        decode(&oversized, [0; 32]).unwrap_err().kind(),
        tinystoragedrivers_types::ErrorKind::InvalidInput
    );
}

#[tokio::test]
async fn watch_rejects_invalid_requests_and_unsupported_deployments_before_io() {
    let client = mongodb::Client::with_uri_str("mongodb://127.0.0.1:1")
        .await
        .unwrap();
    let docs = MongoDocuments {
        database: client.database("watch_validation"),
        client,
        scope: tinystoragedrivers_types::Scope::new("scope").unwrap(),
        clock: std::sync::Arc::new(|| 0),
        transactions: false,
        origin: mongodb::bson::oid::ObjectId::new(),
        condition: None,
    };

    assert_eq!(
        docs.open_watch("", &Filter::default(), &WatchOptions::default())
            .await
            .unwrap_err()
            .kind(),
        tinystoragedrivers_types::ErrorKind::InvalidInput
    );
    assert_eq!(
        docs.open_watch("events", &Filter::eq("", 1), &WatchOptions::default())
            .await
            .unwrap_err()
            .kind(),
        tinystoragedrivers_types::ErrorKind::InvalidInput
    );
    assert_eq!(
        docs.open_watch(
            "events",
            &Filter::default(),
            &WatchOptions {
                after: None,
                interval: std::time::Duration::ZERO,
            }
        )
        .await
        .unwrap_err()
        .kind(),
        tinystoragedrivers_types::ErrorKind::InvalidInput
    );
    assert_eq!(
        docs.open_watch("events", &Filter::default(), &WatchOptions::default())
            .await
            .unwrap_err()
            .kind(),
        tinystoragedrivers_types::ErrorKind::Unsupported(Capability::Watch)
    );
}
