//! Watch projection strips document payload and keeps deletion scope identity.
use super::*;

#[test]
fn pipeline_scopes_native_keys_and_projects_no_user_values() {
    let pipeline = pipeline("tenant");
    assert_eq!(
        pipeline[0],
        doc! {"$match": {"$or": [
            {"documentKey._id.s": "tenant"},
            {"operationType": {"$in": ["drop", "rename", "dropDatabase", "invalidate"]}}
        ]}}
    );
    assert_eq!(
        pipeline[1],
        doc! {"$project": {"_id": 1, "operationType": 1}}
    );
}

#[test]
fn malformed_cursor_refuses_without_native_token_details() {
    let error = decode(
        &WatchCursor::new(b"secret-invalid-token".to_vec()).unwrap(),
        [0; 32],
    )
    .unwrap_err();
    assert_eq!(
        error.kind(),
        tinystoragedrivers_core::ErrorKind::InvalidInput
    );
    assert!(!error.to_string().contains("secret"));
}

#[tokio::test]
async fn cancellation_during_admission_preserves_consumed_native_hint() {
    use std::future::Future;
    use std::task::Poll;
    let mut delivery = PendingDelivery {
        pending: Some(Ok(WatchNotice::Invalidated { cursor: None })),
    };
    let mut waiting = Box::pin(delivery.take_after(std::future::pending()));
    assert!(matches!(
        std::future::poll_fn(|cx| Poll::Ready(waiting.as_mut().poll(cx))).await,
        Poll::Pending
    ));
    drop(waiting);
    assert!(matches!(
        delivery.take_after(async { Ok(()) }).await.unwrap(),
        Some(WatchNotice::Invalidated { cursor: None })
    ));
}

#[tokio::test]
async fn refused_admission_preserves_pending_hint_and_native_error_source() {
    use std::error::Error;
    let mut delivery = PendingDelivery {
        pending: Some(Ok(WatchNotice::Invalidated { cursor: None })),
    };
    let error = delivery
        .take_after(async {
            Err(StorageError::unavailable("native guard unavailable")
                .with_source(std::io::Error::other("guard-source")))
        })
        .await
        .unwrap_err();
    assert!(
        error
            .source()
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .is_some()
    );
    assert!(matches!(
        delivery.take_after(async { Ok(()) }).await.unwrap(),
        Some(WatchNotice::Invalidated { cursor: None })
    ));
}

#[tokio::test]
async fn stored_native_error_is_delivered_once_after_admission() {
    let mut delivery = PendingDelivery {
        pending: Some(Err(StorageError::backend("native watch failure"))),
    };
    assert_eq!(
        delivery
            .take_after(async { Ok(()) })
            .await
            .unwrap_err()
            .kind(),
        tinystoragedrivers_core::ErrorKind::Backend
    );
    assert!(
        delivery
            .take_after(async { Ok(()) })
            .await
            .unwrap()
            .is_none()
    );
}

#[test]
fn native_history_loss_codes_are_distinct_from_other_failures() {
    for code in [136, 260, 280, 286, 91] {
        let command: mongodb::error::CommandError =
            serde_json::from_value(serde_json::json!({"code":code})).unwrap();
        let error: mongodb::error::Error = mongodb::error::ErrorKind::Command(command).into();
        assert_eq!(resume_lost(&error), code != 91);
    }
    assert!(!resume_lost(&mongodb::error::Error::custom("unrelated")));
}
