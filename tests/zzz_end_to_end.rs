#![cfg(not(feature = "skip-end-to-end"))]

use std::time::Duration;

use futures::stream::StreamExt;
use stomping::*;
use tokio::time::timeout;
use tracing::{debug, info};
use uuid::Uuid;

#[tokio::test]
async fn can_round_trip_text() {
    tracing_subscriber::fmt::try_init().unwrap_or_default();
    let (conn, mut client) = connect(
        ("localhost", 61613),
        Some(("guest", "guest")),
        None,
        Default::default(),
    )
    .await
    .expect("connect");
    let conn_task = tokio::spawn(async {
        debug!("Starting connection");
        let res = conn.await;
        debug!("Connection terminated: {:?}", res);
    });

    let body = b"42";
    let queue = format!("/queue/can_round_trip_text-{}", Uuid::new_v4());

    info!("Subscribing to queue");
    let mut sub: Subscription = client
        .subscribe(&queue, "one", AckMode::Auto, Default::default())
        .await
        .expect("subscribe");

    info!("Publishing to queue");
    client.publish(&queue, body).await.expect("publish");

    info!("Consuming from queue");
    let frame: Frame = sub.next().await.expect("consume_next");
    info!("Consumed item");

    assert_eq!(body, &*frame.body);
    client.disconnect().await.expect("disconnect");
    let res = conn_task.await;
    assert!(res.is_ok(), "Conection exited normally");
}

#[tokio::test]
async fn can_round_trip_binary_blobs() {
    tracing_subscriber::fmt::try_init().unwrap_or_default();
    let (conn, mut client) = connect(
        ("localhost", 61613),
        Some(("guest", "guest")),
        None,
        Default::default(),
    )
    .await
    .expect("connect");
    let conn_task = tokio::spawn(conn);

    let body = b"\x00\x01\x02\x03";
    let queue = format!("/queue/can_round_trip_binary_blobs-{}", Uuid::new_v4());

    let mut sub = client
        .subscribe(&queue, "one", AckMode::Auto, Default::default())
        .await
        .expect("subscribe");
    client.publish(&queue, body).await.expect("publish");

    let frame = sub.next().await.expect("consume_next");
    assert_eq!(frame.body, body);
    client.disconnect().await.expect("disconnect");
    let res = conn_task.await;
    assert!(res.is_ok(), "Conection exited normally");
}

#[tokio::test]
async fn client_acks_should_allow_redelivery() {
    tracing_subscriber::fmt::try_init().unwrap_or_default();
    let (conn, mut client) = connect(
        ("localhost", 61613),
        Some(("guest", "guest")),
        None,
        Default::default(),
    )
    .await
    .expect("connect");
    let conn_task = tokio::spawn(conn);

    let body = b"42";
    let queue = format!(
        "/queue/client_acks_should_allow_redelivery-{}",
        Uuid::new_v4()
    );

    let mut sub = client
        .subscribe(&queue, "one", AckMode::ClientIndividual, Default::default())
        .await
        .expect("subscribe");
    client.publish(&queue, body).await.expect("publish");

    let frame = sub.next().await.expect("consume_next");
    assert_eq!(frame.body, body);

    debug!("Disconnecting without acking");
    // Disconnect
    drop(sub);
    client.disconnect().await.expect("disconnect");
    debug!("Dropped client; awaiting connection");
    let res = conn_task.await;
    assert!(res.is_ok(), "Conection exited normally");
    debug!("First connection done");

    let (conn, mut client) = connect(
        ("localhost", 61613),
        Some(("guest", "guest")),
        None,
        Default::default(),
    )
    .await
    .expect("connect");
    let conn_task = tokio::spawn(conn);

    let mut sub = client
        .subscribe(&queue, "one", AckMode::ClientIndividual, Default::default())
        .await
        .expect("subscribe");
    info!("Subscribed on second connection; awaiting next");
    let frame = sub.next().await.expect("consume_next");
    assert_eq!(frame.body, body);
    client.disconnect().await.expect("disconnect");
    let res = conn_task.await;
    assert!(res.is_ok(), "Conection exited normally");
}

#[tokio::test]
async fn can_encode_headers_correctly() {
    tracing_subscriber::fmt::try_init().unwrap_or_default();
    debug!("Connecting");
    let (conn, mut client) = connect(
        ("localhost", 61613),
        Some(("guest", "guest")),
        None,
        Default::default(),
    )
    .await
    .expect("connect");
    let conn_task = tokio::spawn(conn);

    let body = b"42";
    let queue = format!("/queue/can_encode_headers_correctly:{}", Uuid::new_v4());

    let mut sub = client
        .subscribe(&queue, "one", AckMode::Auto, Default::default())
        .await
        .expect("subscribe");
    client.publish(&queue, body).await.expect("publish");

    let frame = sub.next().await.expect("consume_next");
    println!("h: {:?}", frame.headers);
    assert_eq!(frame.headers["destination"], queue);
    client.disconnect().await.expect("disconnect");
    let res = conn_task.await;
    assert!(res.is_ok(), "Conection exited normally");
}

#[tokio::test]
async fn should_allow_acking_individual_messages() {
    tracing_subscriber::fmt::try_init().unwrap_or_default();
    let (conn, mut client) = connect(
        ("localhost", 61613),
        Some(("guest", "guest")),
        None,
        Default::default(),
    )
    .await
    .expect("connect");
    let conn_task = tokio::spawn(conn);

    let queue = format!(
        "/queue/should_allow_acking_individual_messages-{}",
        Uuid::new_v4()
    );

    let mut sub = client
        .subscribe(&queue, "one", AckMode::ClientIndividual, Default::default())
        .await
        .expect("subscribe");
    client.publish(&queue, b"first").await.expect("publish");
    client.publish(&queue, b"second").await.expect("publish");
    client.publish(&queue, b"third").await.expect("publish");

    let frame = sub.next().await.expect("consume_next");
    assert_eq!(frame.body, b"first");
    debug!("Consumed first");

    let frame = sub.next().await.expect("consume_next");
    assert_eq!(frame.body, b"second");
    debug!("Consumed second");

    client.ack(&frame.headers).await.expect("ack");
    debug!("Acked second");

    // If we don't consume all of the items on the channel, we risk a deadlock.
    // The sender half of the network ends up blocking on trying to deliver the
    // third message to the client, meaning it's unable to observe the receipt
    // frame from the disconnect.
    // We should find
    let frame = sub.next().await.expect("consume_next");
    assert_eq!(frame.body, b"third");
    debug!("Observed third");

    info!("Disconnecting…");
    // Disconnect
    client.disconnect().await.expect("disconnect");
    debug!("Drop subscription");
    drop(sub);
    info!("Disconnected…");

    let res = conn_task.await;
    assert!(res.is_ok(), "Conection exited normally");
    info!("Disconnected first connection; reconnecting");

    let (conn, mut client) = connect(
        ("localhost", 61613),
        Some(("guest", "guest")),
        None,
        Default::default(),
    )
    .await
    .expect("connect");
    let conn_task = tokio::spawn(conn);

    let mut sub = client
        .subscribe(&queue, "one", AckMode::ClientIndividual, Default::default())
        .await
        .expect("subscribe");
    let frame = sub.next().await.expect("consume_next");
    assert_eq!(std::str::from_utf8(&frame.body).unwrap(), "first");
    let frame = sub.next().await.expect("consume_next");
    assert_eq!(std::str::from_utf8(&frame.body).unwrap(), "third");

    client.disconnect().await.expect("disconnect");
    let res = conn_task.await;
    assert!(res.is_ok(), "Conection exited normally");
}

// This test never actually terminates.
#[tokio::test]
#[ignore]
async fn thing_to_test_timeouts() {
    tracing_subscriber::fmt::try_init().unwrap_or_default();
    let (conn, mut client) = connect(
        ("localhost", 61613),
        Some(("guest", "guest")),
        Some(Duration::from_millis(500)),
        Default::default(),
    )
    .await
    .expect("connect");
    let conn_task = tokio::spawn(conn);

    let queue = format!("/queue/thing_to_test_timeouts-{}", Uuid::new_v4());

    let mut sub = client
        .subscribe(&queue, "one", AckMode::ClientIndividual, Default::default())
        .await
        .expect("subscribe");

    let frame = sub.next().await.expect("consume_next");
    assert_eq!(frame.body, b"first");
    client.disconnect().await.expect("disconnect");
    let res = conn_task.await;
    assert!(res.is_ok(), "Conection exited normally");
}

#[tokio::test]
async fn should_allow_disconnect_by_dropping_with_pending_deliveries() {
    tracing_subscriber::fmt::try_init().unwrap_or_default();
    let (conn, mut client) = connect(
        ("localhost", 61613),
        Some(("guest", "guest")),
        None,
        Default::default(),
    )
    .await
    .expect("connect");
    let conn_task = tokio::spawn(conn);

    let queue = format!(
        "/queue/should_allow_disconnect_by_dropping_with_pending_deliveries-{}",
        Uuid::new_v4()
    );

    let mut sub = client
        .subscribe(&queue, "one", AckMode::ClientIndividual, Default::default())
        .await
        .expect("subscribe");
    client.publish(&queue, b"first").await.expect("publish");
    client.publish(&queue, b"second").await.expect("publish");
    client.publish(&queue, b"third").await.expect("publish");

    // A good-enough proxy for checking that we've had some deliveries.
    let frame = sub.next().await.expect("consume_next");
    assert_eq!(frame.body, b"first");
    debug!("Consumed first");

    info!("Disconnecting…");
    // Disconnect by dropping client
    drop(client);
    info!("Disconnected…");

    let res = conn_task.await;
    assert!(res.is_ok(), "Conection exited normally");
    info!("Disconnected first connection; reconnecting");
}

#[tokio::test]
async fn should_fail_when_we_force_an_error() {
    tracing_subscriber::fmt::try_init().unwrap_or_default();
    let (conn, mut client) = connect(
        ("localhost", 61613),
        Some(("guest", "guest")),
        None,
        Default::default(),
    )
    .await
    .expect("connect");
    let conn_task = tokio::spawn(async {
        debug!("Starting connection");
        let res = conn.await;
        debug!("Connection terminated: {:?}", res);
        res
    });

    let queue = format!("/invalid-thing/can_round_trip_text-{}", Uuid::new_v4());

    info!("Subscribing to queue");
    // there's no reply to a subscribe frame to wait for, so we won't know
    // it fails until we get the error frame.
    let mut _sub: Subscription = client
        .subscribe(&queue, "one", AckMode::Auto, Default::default())
        .await
        .expect("subscribe succeeds ok");

    let res = timeout(Duration::from_millis(1000), conn_task)
        .await
        .expect("no timeout")
        .expect("joins okay");

    match res.expect_err("has error") {
        StompError::StompError { .. } => {}
        e => panic!("Unexpected error: Got: {:?}", e),
    }
}
