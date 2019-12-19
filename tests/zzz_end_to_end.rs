#![cfg(not(feature = "skip-end-to-end"))]
use env_logger;

#[macro_use]
extern crate log;

use std::time::{Duration, SystemTime};
use stomping::*;
use uuid::Uuid;

#[test]
fn can_round_trip_text() {
    env_logger::try_init().unwrap_or_default();
    let mut client =
        Client::connect(("localhost", 61613), Some(("guest", "guest")), None).expect("connect");
    let body = b"42";
    let queue = format!("/queue/can_round_trip_text-{}", Uuid::new_v4());

    client
        .subscribe(&queue, "one", AckMode::Auto)
        .expect("subscribe");
    client.publish(&queue, body).expect("publish");

    let frame = client.consume_next().expect("consume_next");
    assert_eq!(&*body, &*frame.body);
    client.disconnect().expect("disconnect");
}

#[test]
fn can_round_trip_binary_blobs() {
    env_logger::try_init().unwrap_or_default();
    let mut client =
        Client::connect(("localhost", 61613), Some(("guest", "guest")), None).expect("connect");
    let body = b"\x00\x01\x02\x03";
    let queue = format!("/queue/can_round_trip_binary_blobs-{}", Uuid::new_v4());

    client
        .subscribe(&queue, "one", AckMode::Auto)
        .expect("subscribe");
    client.publish(&queue, body).expect("publish");

    let frame = client.consume_next().expect("consume_next");
    assert_eq!(frame.body, body);
    client.disconnect().expect("disconnect");
}

#[test]
fn client_acks_should_allow_redelivery() {
    env_logger::try_init().unwrap_or_default();
    let mut client =
        Client::connect(("localhost", 61613), Some(("guest", "guest")), None).expect("connect");
    let body = b"42";
    let queue = format!(
        "/queue/client_acks_should_allow_redelivery-{}",
        Uuid::new_v4()
    );

    client
        .subscribe(&queue, "one", AckMode::ClientIndividual)
        .expect("subscribe");
    client.publish(&queue, body).expect("publish");

    let frame = client.consume_next().expect("consume_next");
    assert_eq!(frame.body, body);

    // Disconnect
    drop(client);

    let mut client =
        Client::connect(("localhost", 61613), Some(("guest", "guest")), None).expect("connect");
    client
        .subscribe(&queue, "one", AckMode::ClientIndividual)
        .expect("subscribe");
    let frame = client.consume_next().expect("consume_next");
    assert_eq!(frame.body, body);
    client.disconnect().expect("disconnect");
}

#[test]
fn can_encode_headers_correctly() {
    env_logger::try_init().unwrap_or_default();
    let mut client =
        Client::connect(("localhost", 61613), Some(("guest", "guest")), None).expect("connect");
    let body = b"42";
    let queue = format!("/queue/can_encode_headers_correctly\\:{}", Uuid::new_v4());

    client
        .subscribe(&queue, "one", AckMode::Auto)
        .expect("subscribe");
    client.publish(&queue, body).expect("publish");

    let frame = client.consume_next().expect("consume_next");
    println!("h: {:?}", frame.headers);
    assert_eq!(frame.headers["destination"], queue);
    client.disconnect().expect("disconnect");
}

#[test]
fn should_allow_acking_individual_messages() {
    env_logger::try_init().unwrap_or_default();
    let mut client =
        Client::connect(("localhost", 61613), Some(("guest", "guest")), None).expect("connect");
    let queue = format!(
        "/queue/client_acks_should_allow_redelivery-{}",
        Uuid::new_v4()
    );

    client
        .subscribe(&queue, "one", AckMode::ClientIndividual)
        .expect("subscribe");
    client.publish(&queue, b"first").expect("publish");
    client.publish(&queue, b"second").expect("publish");
    client.publish(&queue, b"third").expect("publish");

    let frame = client.consume_next().expect("consume_next");
    assert_eq!(frame.body, b"first");
    let frame = client.consume_next().expect("consume_next");
    assert_eq!(frame.body, b"second");
    client.ack(&frame.headers).expect("ack");

    // Disconnect
    drop(client);

    let mut client =
        Client::connect(("localhost", 61613), Some(("guest", "guest")), None).expect("connect");
    client
        .subscribe(&queue, "one", AckMode::ClientIndividual)
        .expect("subscribe");
    let frame = client.consume_next().expect("consume_next");
    assert_eq!(frame.body, b"first");
    let frame = client.consume_next().expect("consume_next");
    assert_eq!(frame.body, b"third");
    client.disconnect().expect("disconnect");
}

#[test]
fn should_allow_timeout_on_consume() {
    env_logger::try_init().unwrap_or_default();
    let mut client =
        Client::connect(("localhost", 61613), Some(("guest", "guest")), None).expect("connect");
    let queue = format!(
        "/queue/client_acks_should_allow_redelivery-{}",
        Uuid::new_v4()
    );

    client
        .subscribe(&queue, "one", AckMode::ClientIndividual)
        .expect("subscribe");
    let timeout = Duration::from_millis(500);
    let cons_start = SystemTime::now();
    debug!("Starting consume at {:?}", cons_start);
    let resp = client.maybe_consume_next(timeout).expect("consume_next");
    let duration = cons_start.elapsed().expect("elapsed");
    debug!("consume done in {:?}", duration);
    assert!(resp.is_none());
    assert!(duration >= timeout);

    client.publish(&queue, b"first").expect("publish");
    let resp = client.maybe_consume_next(timeout).expect("consume_next");
    let frame = resp.expect("a message");
    assert_eq!(frame.body, b"first");
}
// This test never actually terminates.
#[test]
#[ignore]
fn thing_to_test_timeouts() {
    env_logger::try_init().unwrap_or_default();
    let mut client = Client::connect(
        ("localhost", 61613),
        Some(("guest", "guest")),
        Some(Duration::from_millis(500)),
    )
    .expect("connect");
    let queue = format!("/queue/thing_to_test_timeouts-{}", Uuid::new_v4());

    client
        .subscribe(&queue, "one", AckMode::ClientIndividual)
        .expect("subscribe");

    let frame = client.consume_next().expect("consume_next");
    assert_eq!(frame.body, b"first");
    client.disconnect().expect("disconnect");
}
