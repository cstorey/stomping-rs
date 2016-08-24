extern crate stomping;
extern crate env_logger;
extern crate log;
extern crate uuid;

use stomping::*;
use uuid::Uuid;

#[test]
fn can_round_trip_text() {
    env_logger::init().unwrap_or(());
    let mut client = Client::connect(("localhost", 61613), Some(("guest", "guest"))).expect("connect");
    let body = b"42";
    let queue = format!("/queue/can_round_trip_text-{}", Uuid::new_v4());

    client.subscribe(&queue, "one", AckMode::Auto).expect("subscribe");
    client.publish(&queue, body).expect("publish");

    let (_headers, msg) = client.consume_next().expect("consume_next");
    assert_eq!(msg, body);
}

#[test]
fn can_round_trip_binary_blobs() {
    env_logger::init().unwrap_or(());
    let mut client = Client::connect(("localhost", 61613), Some(("guest", "guest"))).expect("connect");
    let body = b"\x00\x01\x02\x03";
    let queue = format!("/queue/can_round_trip_binary_blobs-{}", Uuid::new_v4());

    client.subscribe(&queue, "one", AckMode::Auto).expect("subscribe");
    client.publish(&queue, body).expect("publish");

    let (_headers, msg) = client.consume_next().expect("consume_next");
    assert_eq!(msg, body);
}
