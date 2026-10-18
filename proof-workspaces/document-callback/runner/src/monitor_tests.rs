//! A live public-signal barrier makes the confidential exclusion oracle observable.
use super::*;
use tinybus::Message;
use tokio::sync::broadcast;

#[derive(Debug, PartialEq, Eq)]
enum MonitorFailure {
    Closed,
    Lagged,
    Timeout,
    ConfidentialBody,
}
fn inspect(message: &Message, marker: &str) -> Result<bool, MonitorFailure> {
    let body = message.body.to_string();
    if [
        "private-cache-key",
        "private-env",
        "private-audit",
        "private-document",
    ]
    .iter()
    .any(|sentinel| {
        body.contains(sentinel)
            || body.contains(&serde_json::to_string(sentinel.as_bytes()).unwrap())
    }) {
        return Err(MonitorFailure::ConfidentialBody);
    }
    Ok(message.body == json!([marker, "public-signal-positive-control"]))
}
async fn observe_barrier(
    monitor: &mut broadcast::Receiver<Message>,
    marker: &str,
) -> Result<(), MonitorFailure> {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let message = monitor.recv().await.map_err(|error| match error {
                broadcast::error::RecvError::Closed => MonitorFailure::Closed,
                broadcast::error::RecvError::Lagged(_) => MonitorFailure::Lagged,
            })?;
            if inspect(&message, marker)? {
                return Ok(());
            }
        }
    })
    .await
    .map_err(|_| MonitorFailure::Timeout)?
}
pub(super) async fn monitor_barrier(
    publisher: &Connection,
    monitor: &mut broadcast::Receiver<Message>,
    marker: &str,
) -> Result<(), String> {
    publisher
        .emit(
            "/proof/barrier".parse().unwrap(),
            "ai.tinyhumans.storage.ProofBarrier1".parse().unwrap(),
            "Barrier".parse().unwrap(),
            (marker, "public-signal-positive-control"),
        )
        .await
        .unwrap();
    observe_barrier(monitor, marker)
        .await
        .map_err(|failure| format!("monitor barrier failed: {failure:?}"))
}
fn message(value: serde_json::Value) -> Message {
    Message::signal(
        "/proof/barrier".parse().unwrap(),
        "ai.tinyhumans.storage.ProofBarrier1".parse().unwrap(),
        "Barrier".parse().unwrap(),
        value,
    )
}
#[tokio::test]
async fn closed_monitor_cannot_satisfy_confidential_exclusion() {
    let (sender, mut monitor) = broadcast::channel(1);
    drop(sender);
    assert_eq!(
        observe_barrier(&mut monitor, "after").await,
        Err(MonitorFailure::Closed)
    );
}
#[tokio::test]
async fn monitor_oracle_rejects_leaks_and_lag_and_observes_public_control() {
    let (sender, mut monitor) = broadcast::channel(4);
    sender
        .send(message(json!(["unrelated-public-event"])))
        .unwrap();
    sender
        .send(message(json!(["after", "public-signal-positive-control"])))
        .unwrap();
    assert_eq!(observe_barrier(&mut monitor, "after").await, Ok(()));
    for sentinel in [
        "private-cache-key",
        "private-env",
        "private-audit",
        "private-document",
    ] {
        sender.send(message(json!([sentinel]))).unwrap();
        assert_eq!(
            observe_barrier(&mut monitor, "after").await,
            Err(MonitorFailure::ConfidentialBody)
        );
        sender.send(message(json!([sentinel.as_bytes()]))).unwrap();
        assert_eq!(
            observe_barrier(&mut monitor, "after").await,
            Err(MonitorFailure::ConfidentialBody)
        );
    }
    let (sender, mut monitor) = broadcast::channel(1);
    sender.send(message(json!(["first"]))).unwrap();
    sender.send(message(json!(["second"]))).unwrap();
    assert_eq!(
        observe_barrier(&mut monitor, "after").await,
        Err(MonitorFailure::Lagged)
    );
}
