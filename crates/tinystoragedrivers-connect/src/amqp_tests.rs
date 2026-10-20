use super::*;

#[tokio::test]
async fn opens_shared_connection_and_creates_channels_when_broker_is_configured() {
    let Ok(url) = std::env::var("TSD_RABBITMQ_URL") else {
        return;
    };
    let connection = AmqpConnection::connect(&url).await.unwrap();
    assert!(connection.is_connected());
    let channel = connection.create_channel().await.unwrap();
    assert!(connection.is_connected());
    assert!(format!("{connection:?}").contains("AmqpConnection"));
    drop(channel);
}

#[tokio::test]
async fn closed_connection_maps_channel_creation_errors() {
    let Ok(url) = std::env::var("TSD_RABBITMQ_URL") else {
        return;
    };
    let connection = AmqpConnection::connect(&url).await.unwrap();
    connection
        .connection
        .close(200, "close for channel error test".into())
        .await
        .unwrap();
    let error = connection.create_channel().await.unwrap_err();
    assert_eq!(
        error.kind(),
        tinystoragedrivers_types::ErrorKind::Unavailable
    );
    assert!(error.to_string().contains("amqp channel creation failed"));
}

#[tokio::test]
async fn connection_errors_do_not_echo_the_broker_url() {
    let secret_url = "amqp://user:secret@127.0.0.1:1/%2f";
    let error = AmqpConnection::connect(secret_url).await.unwrap_err();
    assert_eq!(
        error.kind(),
        tinystoragedrivers_types::ErrorKind::Unavailable
    );
    assert!(!error.to_string().contains("secret"));
}
