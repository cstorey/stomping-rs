extern crate stomping;
extern crate env_logger;
extern crate log;

use stomping::*;

#[test]

fn can_round_trip_text() {
    env_logger::init().unwrap_or(());
    let mut client = Client::connect(("localhost", 61613)).expect("connect");
    client.authenticate("guest", "guest").expect("authenticate");
    
    client.subscribe("/queue/can_blah", "one", AckMode::Auto).expect("subscribe");
    client.publish("/queue/can_blah", b"42").expect("publish");

    let (headers, msg) = client.consume_next().expect("consume_next");
    assert_eq!(msg, b"42");
}
