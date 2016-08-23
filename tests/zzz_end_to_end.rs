extern crate stomping;
extern crate env_logger;
extern crate log;

use stomping::*;

#[test]
fn can_round_trip_text() {
    env_logger::init().unwrap_or(());
    let mut client = Client::connect(("localhost", 61613), Some(("guest", "guest"))).expect("connect");
    let body = b"42";

    client.subscribe("/queue/can_blah", "one", AckMode::Auto).expect("subscribe");
    client.publish("/queue/can_blah", body).expect("publish");

    let (headers, msg) = client.consume_next().expect("consume_next");
    assert_eq!(msg, body);
}

#[test]
fn can_round_trip_binary_blobs() {
    env_logger::init().unwrap_or(());
    let mut client = Client::connect(("localhost", 61613), Some(("guest", "guest"))).expect("connect");
    let body = b"\x00\x01\x02\x03";

    client.subscribe("/queue/can_blah", "one", AckMode::Auto).expect("subscribe");
    client.publish("/queue/can_blah", body).expect("publish");

    let (headers, msg) = client.consume_next().expect("consume_next");
    assert_eq!(msg, body);
}
