extern crate stomping;
extern crate env_logger;
#[macro_use]
extern crate clap;
extern crate url;

use clap::{Arg, App};
use url::Url;

use stomping::*;

fn main(){
    let matches = App::new("listener")
        .version("?")
        .author("Ceri Storey")
        .arg(Arg::with_name("url").help("Target url").index(1).required(true))
        .get_matches();

    env_logger::init().expect("init-logger");

    let url = Url::parse(matches.value_of("url").expect("url parameter")).expect("parsing as URL");

    let addr = url.with_default_port(|_| Ok(61613)).expect("host-port");

    println!("user: {:?}; pass:{:?}", url.username(), url.password());
    let creds = url.password().map(|p| (url.username(), p));
    let mut client = Client::connect(addr, creds).expect("connect");

    client.subscribe(url.path(), "0", AckMode::Auto).expect("subscribe");

    loop {
        let (headers, msg) = client.consume_next().expect("consume_next");
        println!("{:?}", headers);
        println!("{:?}", String::from_utf8_lossy(&msg));
    }

}

