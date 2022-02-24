use std::time::Duration;

use clap::{Arg, Command};
use futures::stream::StreamExt;
use percent_encoding::percent_decode_str;
use url::Url;

use stomping::*;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let matches = Command::new("listener")
        .version("?")
        .author("Ceri Storey")
        .arg(Arg::new("url").help("Target url").index(1).required(true))
        .arg(
            Arg::new("heartbeat")
                .short('k')
                .help("Heartbeat interval in seconds")
                .takes_value(true),
        )
        .get_matches();

    env_logger::init();

    let url = Url::parse(matches.value_of("url").expect("url parameter")).expect("parsing as URL");
    let heartbeat = if matches.is_present("heartbeat") {
        let secs = matches.value_of_t_or_exit("heartbeat");
        Some(Duration::new(secs, 0))
    } else {
        None
    };

    println!("user: {:?}; pass:{:?}", url.username(), url.password());

    let hostport: std::net::SocketAddr = tokio::net::lookup_host((
        url.host_str().unwrap_or("localhost"),
        url.port().unwrap_or(61613),
    ))
    .await
    .expect("name lookup")
    .next()
    .expect("some address");

    let (conn, mut client) = if let Some(pass) = url.password() {
        let username = percent_decode_str(url.username())
            .decode_utf8()
            .expect("decode username");
        let password = percent_decode_str(pass)
            .decode_utf8()
            .expect("decode password");

        connect(
            hostport,
            Some((&*username, &*password)),
            heartbeat,
            Default::default(),
        )
        .await
        .expect("connect")
    } else {
        connect(hostport, None, heartbeat, Default::default())
            .await
            .expect("connect")
    };

    tokio::spawn(conn);

    let mut sub = client
        .subscribe(
            url.path(),
            "0",
            AckMode::ClientIndividual,
            Default::default(),
        )
        .await
        .expect("subscribe");

    loop {
        let frame = sub.next().await.expect("consume_next");
        for (i, (k, v)) in frame.headers.iter().enumerate() {
            if i != 0 {
                print!(", ");
            }
            print!("{}={}", k, v);
        }
        println!();
        println!("{:?}", std::str::from_utf8(&frame.body));
        println!();
        client.ack(&frame.headers).await.expect("ack");
    }
}
