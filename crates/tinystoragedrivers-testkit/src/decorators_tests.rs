use super::*;
use std::time::Duration;
use tinystoragedrivers_types::ErrorKind;

#[test]
fn conflict_once_fails_exactly_the_first_checked_operation() {
    let decorator = ConflictOnce::new("adapter");
    assert_eq!(decorator.check().unwrap_err().kind(), ErrorKind::Conflict);
    assert!(decorator.check().is_ok());
    assert!(decorator.check().is_ok());
}

#[tokio::test(start_paused = true)]
async fn slow_runs_the_operation_after_its_configured_delay_hook() {
    let decorator = Slow::new(7, Duration::from_millis(20));
    let operation = tokio::spawn(async move { decorator.run(async { decorator.inner + 1 }).await });
    tokio::task::yield_now().await;
    assert!(
        !operation.is_finished(),
        "delay hook waits before delegation"
    );
    tokio::time::advance(Duration::from_millis(20)).await;
    assert_eq!(operation.await.unwrap(), 8);
}

#[tokio::test]
async fn conflict_once_call_injects_then_delegates() {
    let decorator = ConflictOnce::new(());
    assert_eq!(
        decorator.call(async { Ok(1) }).await.unwrap_err().kind(),
        ErrorKind::Conflict
    );
    assert_eq!(decorator.call(async { Ok(2) }).await.unwrap(), 2);
}
