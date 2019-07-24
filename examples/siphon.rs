extern crate env_logger;
extern crate stomping;
#[macro_use]
extern crate clap;
extern crate url;

use std::time::Duration;

use clap::{App, Arg};
use url::Url;

use stomping::*;

fn main() {
    let matches = App::new("listener")
        .version("?")
        .author("Ceri Storey")
        .arg(
            Arg::with_name("url")
                .help("Target url")
                .index(1)
                .required(true),
        )
        .arg(
            Arg::with_name("heartbeat")
                .short("k")
                .help("Heartbeat interval in seconds")
                .takes_value(true),
        )
        .get_matches();

    env_logger::init();

    let url = Url::parse(matches.value_of("url").expect("url parameter")).expect("parsing as URL");
    let heartbeat = if matches.is_present("heartbeat") {
        let secs = value_t!(matches, "heartbeat", u64).unwrap_or_else(|e| e.exit());
        Some(Duration::new(secs, 0))
    } else {
        None
    };

    println!("user: {:?}; pass:{:?}", url.username(), url.password());
    let creds = url.password().map(|p| (url.username(), p));
    let hostport = (
        url.host_str().unwrap_or("localhost"),
        url.port().unwrap_or(61613),
    );
    let mut client = Client::connect(hostport, creds, heartbeat).expect("connect");

    client
        .subscribe(url.path(), "0", AckMode::Auto)
        .expect("subscribe");

    loop {
        let (headers, msg) = client.consume_next().expect("consume_next");
        println!("{:?}", headers);
        println!("{:?}", String::from_utf8_lossy(&msg));
    }
}
