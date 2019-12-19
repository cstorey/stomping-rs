#[macro_use]
extern crate log;
use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

use futures::{sink::SinkExt, stream::StreamExt};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_util::codec::Framed;

mod errors;
use crate::errors::*;

mod connection;
mod parser;
mod unparser;

pub struct Client {
    inner: Framed<TcpStream, connection::StompCodec>,
}

pub enum AckMode {
    Auto,
    ClientIndividual,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Command {
    // Client Commands
    Connect,
    Send,
    Subscribe,
    Unsubscribe,
    Disconnect,
    Ack,

    // Server commands
    Connected,
    Message,
    Receipt,
    Error,
}

#[derive(Clone, Eq, PartialEq, Debug, Hash)]
pub struct Frame {
    pub command: Command,
    pub headers: Headers,
    pub body: Vec<u8>,
}

#[derive(Clone, Eq, PartialEq, Debug, Hash)]
pub enum FrameOrKeepAlive {
    Frame(Frame),
    KeepAlive,
}

impl AckMode {
    fn as_str(&self) -> &'static str {
        match self {
            &AckMode::Auto => "auto",
            &AckMode::ClientIndividual => "client-individual",
        }
    }
}

pub type Headers = BTreeMap<String, String>;

fn parse_keepalive(headervalue: Option<&str>) -> Result<(Option<Duration>, Option<Duration>)> {
    if let Some(sxsy) = headervalue {
        info!("heartbeat: theirs:{:?}", sxsy);
        let mut it = sxsy.trim().splitn(2, ',');
        let sx = Duration::from_millis(it.next().ok_or(StompError::ProtocolError)?.parse()?);
        let sy = Duration::from_millis(it.next().ok_or(StompError::ProtocolError)?.parse()?);
        info!("heartbeat: theirs:{:?}", (&sx, &sy));

        Ok((some_non_zero(sx), some_non_zero(sy)))
    } else {
        Ok((None, None))
    }
}

fn some_non_zero(dur: Duration) -> Option<Duration> {
    if dur == Duration::from_millis(0) {
        None
    } else {
        Some(dur)
    }
}

impl Client {
    pub async fn connect<A: ToSocketAddrs>(
        a: A,
        credentials: Option<(&str, &str)>,
        keepalive: Option<Duration>,
    ) -> Result<Self> {
        let conn = TcpStream::connect(a).await?;

        let mut conn = connection::wrap(conn);

        let mut conn_headers = BTreeMap::new();
        conn_headers.insert("accept-version".to_string(), "1.2".to_string());
        if let &Some(ref duration) = &keepalive {
            let millis = duration.as_secs() * 1000 + duration.subsec_nanos() as u64 / 1000_000;
            conn_headers.insert("heart-beat".to_string(), format!("{},{}", millis, millis));
        }
        if let Some((user, pass)) = credentials {
            conn_headers.insert("login".to_string(), user.to_string());
            conn_headers.insert("passcode".to_string(), pass.to_string());
        }
        trace!("Sending connect frame");
        conn.send(FrameOrKeepAlive::Frame(Frame {
            command: Command::Connect,
            headers: conn_headers,
            body: vec![],
        }))
        .await?;

        trace!("Awaiting connected frame");
        let frame = conn.next().await.transpose()?.ok_or_else(|| {
            warn!("Connection closed before first frame received");
            StompError::ProtocolError
        })?;

        let frame = match frame {
            FrameOrKeepAlive::Frame(frame) => frame,
            FrameOrKeepAlive::KeepAlive => {
                warn!("Keepalive sent before first frame received");
                return Err(StompError::ProtocolError);
            }
        };

        if frame.command == Command::Error {
            warn!(
                "Error response from server: {:?}: {:?}",
                frame.command, frame.headers
            );
            return Err(StompError::StompError(frame).into());
        } else if frame.command != Command::Connected {
            warn!(
                "Bad response from server: {:?}: {:?}",
                frame.command, frame.headers
            );
            return Err(StompError::ProtocolError.into());
        }

        let (sx, sy) = parse_keepalive(frame.headers.get("heart-beat").map(|s| &**s))?;
        debug!("Proposed keepalives: {:?}/{:?}", sx, sy);

        // TODO: Keepalives and such
        let client = Client { inner: conn };
        Ok(client)
    }

    pub async fn subscribe(&mut self, destination: &str, id: &str, mode: AckMode) -> Result<()> {
        let mut h = BTreeMap::new();
        h.insert("destination".to_string(), destination.to_string());
        h.insert("id".to_string(), id.to_string());
        h.insert("ack".to_string(), mode.as_str().to_string());
        self.send(Frame {
            command: Command::Subscribe,
            headers: h,
            body: Vec::new(),
        })
        .await?;
        Ok(())
    }
    pub async fn publish(&mut self, destination: &str, body: &[u8]) -> Result<()> {
        let mut h = BTreeMap::new();
        h.insert("destination".to_string(), destination.to_string());
        h.insert("content-length".to_string(), format!("{}", body.len()));
        self.send(Frame {
            command: Command::Send,
            headers: h,
            body: body.into(),
        })
        .await?;
        Ok(())
    }

    pub async fn ack(&mut self, headers: &Headers) -> Result<()> {
        let mut h = BTreeMap::new();
        let mid = headers.get("ack").ok_or(StompError::NoAckHeader)?;
        h.insert("id".to_string(), mid.to_string());
        self.send(Frame {
            command: Command::Ack,
            headers: h,
            body: Vec::new(),
        })
        .await?;
        Ok(())
    }

    pub async fn consume_next(&mut self) -> Result<Frame> {
        loop {
            let msg = self.inner.next().await.transpose()?.ok_or_else(|| {
                warn!("Connection dropped while awaiting frame");
                StompError::ProtocolError
            })?;
            match msg {
                FrameOrKeepAlive::KeepAlive => continue,
                FrameOrKeepAlive::Frame(
                    frame @ Frame {
                        command: Command::Message,
                        ..
                    },
                ) => return Ok(frame),
                FrameOrKeepAlive::Frame(frame) => {
                    warn!(
                        "Bad message from server: {:?}: {:?}",
                        frame.command, frame.headers
                    );
                    return Err(StompError::ProtocolError.into());
                }
            }
        }
    }

    pub async fn disconnect(mut self) -> Result<()> {
        let mut h = BTreeMap::new();
        h.insert("receipt".to_string(), "42".to_string());
        self.send(Frame {
            command: Command::Disconnect,
            headers: h,
            body: Vec::new(),
        })
        .await?;

        loop {
            let msg = self.inner.next().await.transpose()?.ok_or_else(|| {
                warn!("Connection dropped while awaiting frame");
                StompError::ProtocolError
            })?;
            match msg {
                FrameOrKeepAlive::KeepAlive => continue,
                FrameOrKeepAlive::Frame(Frame {
                    command: Command::Receipt,
                    ..
                }) => break,
                FrameOrKeepAlive::Frame(frame) => {
                    warn!(
                        "Unexpected message from server: {:?}: {:?}",
                        frame.command, frame.headers
                    );
                }
            }
        }

        Ok(())
    }

    async fn send(&mut self, frame: Frame) -> Result<()> {
        self.inner.send(FrameOrKeepAlive::Frame(frame)).await?;
        Ok(())
    }
}

impl Command {
    fn as_str(&self) -> &'static str {
        match self {
            Command::Connect => "CONNECT",
            Command::Send => "SEND",
            Command::Subscribe => "SUBSCRIBE",
            Command::Unsubscribe => "UNSUBSCRIBE",
            Command::Disconnect => "DISCONNECT",
            Command::Ack => "ACK",
            Command::Connected => "CONNECTED",
            Command::Message => "MESSAGE",
            Command::Receipt => "RECEIPT",
            Command::Error => "ERROR",
        }
    }
}

impl std::str::FromStr for Command {
    type Err = StompError;
    fn from_str(input: &str) -> Result<Self> {
        match input {
            "CONNECT" => Ok(Command::Connect),
            "SEND" => Ok(Command::Send),
            "SUBSCRIBE" => Ok(Command::Subscribe),
            "UNSUBSCRIBE" => Ok(Command::Unsubscribe),
            "DISCONNECT" => Ok(Command::Disconnect),
            "ACK" => Ok(Command::Ack),
            "CONNECTED" => Ok(Command::Connected),
            "MESSAGE" => Ok(Command::Message),
            "RECEIPT" => Ok(Command::Receipt),
            "ERROR" => Ok(Command::Error),
            _ => Err(StompError::ProtocolError),
        }
    }
}

#[derive(Debug, Clone)]
struct PaceMaker {
    client_to_server: Option<Duration>,
    server_to_client: Option<Duration>,
    last_observed_read: SystemTime,
    last_observed_write: SystemTime,
}

#[cfg(test)]
mod test {
    use super::*;
    use env_logger;
    use std::time::Duration;

    #[test]
    fn keepalives_parse_zero_as_none_0() {
        env_logger::try_init().unwrap_or_default();
        assert_eq!(
            parse_keepalive(Some("0,0")).expect("parse_keepalive"),
            (None, None)
        );
    }

    #[test]
    fn keepalives_parse_zero_as_none_1() {
        env_logger::try_init().unwrap_or_default();
        assert_eq!(
            parse_keepalive(Some("0,42")).expect("parse_keepalive"),
            (None, Some(Duration::from_millis(42)))
        );
    }

    #[test]
    fn keepalives_parse_zero_as_none_2() {
        env_logger::try_init().unwrap_or_default();
        assert_eq!(
            parse_keepalive(Some("42,0")).expect("parse_keepalive"),
            (Some(Duration::from_millis(42)), None)
        );
    }

    impl FrameOrKeepAlive {
        pub(crate) fn unwrap_frame(self) -> Frame {
            match self {
                FrameOrKeepAlive::Frame(f) => f,
                FrameOrKeepAlive::KeepAlive => panic!("Expcted a frame, got keepalive"),
            }
        }
    }
}
