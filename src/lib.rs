#[macro_use]
extern crate log;
use std::cmp;
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

const EPSILON: Duration = Duration::from_micros(1);
const ZERO: Duration = Duration::from_micros(0);

impl AckMode {
    fn as_str(&self) -> &'static str {
        match self {
            &AckMode::Auto => "auto",
            &AckMode::ClientIndividual => "client-individual",
        }
    }
}

fn decode_header(s: &str) -> String {
    let mut out = String::new();
    let mut it = s.chars();
    while let Some(c) = it.next() {
        match c {
            '\\' => match it.next() {
                Some('r') => out.push('\r'),
                Some('n') => out.push('\n'),
                Some('c') => out.push(':'),
                Some('\\') => out.push('\\'),
                other => warn!("Unrecognised escape: \\{:?}", other),
            },
            c => out.push(c),
        }
    }
    out
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

        /*
        let (sx, sy) = parse_keepalive(frame.headers.get("heart-beat").map(|s| &**s))?;
        client.pace = PaceMaker::new(keepalive, sx, sy, start);

        client.reset_timeouts(None)?;
        Ok(client)
        */
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

#[derive(Debug, Clone, PartialEq, Eq)]
enum BeatAction {
    Retry,
    PeerFailed,
    SendClientHeart,
}

impl PaceMaker {
    fn new(
        keepalive: Option<Duration>,
        sx: Option<Duration>,
        sy: Option<Duration>,
        now: SystemTime,
    ) -> Self {
        debug!(
            "heart-beat: cx, cy:{:?}; server-transmit:{:?}; server-receive:{:?}; now:{:?}",
            keepalive, sx, sy, now
        );
        let client_to_server = sy.and_then(|sy| keepalive.map(|cx| cmp::max(cx, sy)));
        let server_to_client = sx.and_then(|sx| keepalive.map(|cy| cmp::max(cy, sx)));

        PaceMaker {
            client_to_server: client_to_server,
            server_to_client: server_to_client,
            last_observed_read: now,
            last_observed_write: now,
        }
    }

    fn read_timeout(&self, now: SystemTime) -> Option<Duration> {
        trace!("read_timeout:{:?}", self);
        let until_read = self.server_to_client.map(|s2c| {
            let deadline = self.last_observed_read + s2c;
            let res = deadline.duration_since(now);
            trace!("deadline:{:?} since {:?} → {:?}", deadline, now, res);
            trace!("s2c timeout:{:?}; due in {:?}", s2c, res);
            res.map(|d| d * 2).unwrap_or(ZERO)
        });
        let until_write = self.client_to_server.map(|c2s| {
            let deadline = self.last_observed_write + c2s;
            let res = deadline.duration_since(now);
            trace!("deadline:{:?} since {:?} → {:?}", deadline, now, res);
            trace!("c2s timeout:{:?}; due in {:?}", c2s, res);
            res.map(|d| d / 2).unwrap_or(ZERO)
        });
        debug!("until_read:{:?}; until_write:{:?}", until_read, until_write);
        let timeout = until_read.into_iter().chain(until_write.into_iter()).min();
        debug!("overall due in {:?}", timeout);
        return timeout.map(|t| cmp::max(t, EPSILON));
    }

    fn read_observed(&mut self, at: SystemTime) {
        self.last_observed_read = at;
        debug!("last_observed_read now: {:?}", at);
    }
    fn write_observed(&mut self, at: SystemTime) {
        self.last_observed_write = at;
        debug!("last_observed_write now: {:?}", at);
    }

    fn handle_read_timeout(&mut self, at: SystemTime) -> Result<BeatAction> {
        debug!("handle_read_timeout: {:?} at {:?}", self, &at);
        if let (mark, Some(interval)) = (self.last_observed_write, self.client_to_server) {
            let duration = at.duration_since(mark)?;
            debug!(
                "consider sending heartbeat after: {:?} - {:?} -> {:?}",
                mark, at, duration
            );
            if duration >= interval {
                debug!("Should send beat");
                return Ok(BeatAction::SendClientHeart);
            }
        }

        if let (mark, Some(interval)) = (self.last_observed_read, self.server_to_client) {
            let duration = at.duration_since(mark)?;
            debug!(
                "considering if alive after: {:?} - {:?} -> {:?}",
                mark, at, duration
            );
            if duration < interval * 2 {
                debug!("Should retry");
                Ok(BeatAction::Retry)
            } else {
                debug!("Peer dead");
                Ok(BeatAction::PeerFailed)
            }
        } else {
            // Uh, no idea.
            debug!("No heartbeats");
            Ok(BeatAction::Retry)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use env_logger;
    use std::time::{Duration, SystemTime};

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

    #[test]
    fn pacemaker_blah_blah_blah() {
        env_logger::try_init().unwrap_or_default();
        let start = SystemTime::now();
        let pm = PaceMaker::new(None, None, None, start);
        assert_eq!(pm.read_timeout(start), None);
    }

    #[test]
    fn pacemaker_inf_read_timeout_when_server_unsupported() {
        env_logger::try_init().unwrap_or_default();
        let start = SystemTime::now();
        let pm = PaceMaker::new(Some(Duration::from_millis(20)), None, None, start);
        println!("pm: {:?}", pm);
        println!("read_timeout: {:?}", pm.read_timeout(start));
        let start = SystemTime::now();
        assert_eq!(pm.read_timeout(start), None);
    }

    #[test]
    fn pacemaker_read_timeout_max_of_ours_and_server_send_rate_times_two() {
        env_logger::try_init().unwrap_or_default();
        let start = SystemTime::now();
        let pm = PaceMaker::new(
            Some(Duration::from_millis(20)),
            Some(Duration::from_millis(10)),
            None,
            start,
        );
        println!("pm: {:?}", pm);
        println!("read_timeout: {:?}", pm.read_timeout(start));
        assert_eq!(pm.read_timeout(start), Some(Duration::from_millis(40)));
    }

    #[test]
    fn pacemaker_read_timeout_max_of_ours_and_server_send_rate_times_two_2() {
        env_logger::try_init().unwrap_or_default();
        let start = SystemTime::now();
        let pm = PaceMaker::new(
            Some(Duration::from_millis(20)),
            Some(Duration::from_millis(30)),
            None,
            start,
        );
        println!("pm: {:?}", pm);
        println!("read_timeout: {:?}", pm.read_timeout(start));
        assert_eq!(pm.read_timeout(start), Some(Duration::from_millis(60)));
    }

    #[test]
    fn pacemaker_read_timeout_should_be_half_client_heartbeat() {
        env_logger::try_init().unwrap_or_default();
        let start = SystemTime::now();
        let pm = PaceMaker::new(
            Some(Duration::from_millis(10)),
            None,
            Some(Duration::from_millis(30)),
            start,
        );
        println!("pm: {:?}", pm);
        println!("read_timeout: {:?}", pm.read_timeout(start));
        assert_eq!(pm.read_timeout(start), Some(Duration::from_millis(15)));
    }

    #[test]
    fn pacemaker_read_timeout_should_be_min_of_client_and_twice_server_heartbeat_1() {
        env_logger::try_init().unwrap_or_default();
        // Client wants to send/receive heartbeats every 10ms
        // Server wants to send every 10ms, receive every 30ms.
        // -> We need to send one every 30ms, we expect one every 10ms.
        // So if we don't see any reads after 20ms, we consider the peer dead.
        let start = SystemTime::now();
        let pm = PaceMaker::new(
            Some(Duration::from_millis(10)),
            Some(Duration::from_millis(10)),
            Some(Duration::from_millis(30)),
            start,
        );
        println!("pm: {:?}", pm);
        println!("read_timeout: {:?}", pm.read_timeout(start));
        assert_eq!(pm.client_to_server, Some(Duration::from_millis(30)));
        assert_eq!(pm.server_to_client, Some(Duration::from_millis(10)));
        assert_eq!(pm.read_timeout(start), Some(Duration::from_millis(15)));
    }

    #[test]
    fn pacemaker_read_timeout_should_be_min_of_client_and_twice_server_heartbeat_2() {
        env_logger::try_init().unwrap_or_default();
        let start = SystemTime::now();
        let pm = PaceMaker::new(
            Some(Duration::from_millis(10)),
            Some(Duration::from_millis(10)),
            Some(Duration::from_millis(2)),
            start,
        );
        println!("pm: {:?}", pm);
        println!("read_timeout: {:?}", pm.read_timeout(start));
        assert_eq!(pm.client_to_server, Some(Duration::from_millis(10)));
        assert_eq!(pm.server_to_client, Some(Duration::from_millis(10)));
        assert_eq!(pm.read_timeout(start), Some(Duration::from_millis(5)));
    }

    #[test]
    fn pacemaker_read_timeout_should_be_min_of_client_and_twice_server_heartbeat_3() {
        // Client wants to send/receive heartbeats every 2ms
        // Server wants to send every 10ms, receive every 2ms.
        // -> We need to send one every 2ms, we expect one every 10ms.
        // So if we don't see any reads after 2ms, wakeup and send frame.
        env_logger::try_init().unwrap_or_default();
        let start = SystemTime::now();
        let pm = PaceMaker::new(
            Some(Duration::from_millis(2)),
            Some(Duration::from_millis(10)),
            Some(Duration::from_millis(2)),
            start,
        );
        println!("pm: {:?}", pm);
        println!("read_timeout: {:?}", pm.read_timeout(start));
        assert_eq!(pm.client_to_server, Some(Duration::from_millis(2)));
        assert_eq!(pm.server_to_client, Some(Duration::from_millis(10)));
        assert_eq!(pm.read_timeout(start), Some(Duration::from_millis(1)));
    }

    #[test]
    fn pacemaker_read_timeout_should_yield_epsilon_after_timeout() {
        // Client wants to send/receive heartbeats every 2ms
        // Server wants to send every 10ms, receive every 2ms.
        // -> We need to send one every 2ms, we expect one every 10ms.
        // So if we don't see any reads after 2ms, wakeup and send frame.
        env_logger::try_init().unwrap_or_default();
        let start = SystemTime::now();
        // It's now a _long_ time after the timeouts have expired.
        let pm = PaceMaker::new(
            Some(Duration::from_millis(2)),
            Some(Duration::from_millis(10)),
            Some(Duration::from_millis(2)),
            start,
        );
        println!("pm: {:?}", pm);
        let now = start + Duration::from_secs(1);
        let res = pm.read_timeout(now).expect("some finite timeout");
        println!("read_timeout at {:?}: {:?}", now, res);
        let max_eps = Duration::from_millis(1);
        assert!(
            ZERO < res && res < max_eps,
            "{:?} < {:?} < {:?}",
            ZERO,
            res,
            max_eps
        );
    }

    #[test]
    fn pacemaker_should_yield_failure_after_twice_server_heartbeat_interval() {
        env_logger::try_init().unwrap_or_default();
        let start = SystemTime::now();
        let mut pm = PaceMaker::new(
            Some(Duration::from_millis(10)),
            Some(Duration::from_millis(10)),
            None,
            start,
        );
        pm.read_observed(start);
        assert_eq!(
            pm.handle_read_timeout(start + Duration::from_millis(19))
                .expect("handle_read_timeout"),
            BeatAction::Retry
        );
        assert_eq!(
            pm.handle_read_timeout(start + Duration::from_millis(20))
                .expect("handle_read_timeout"),
            BeatAction::PeerFailed
        );
    }

    #[test]
    fn pacemaker_should_yield_client_heartbeat_after_client_heartbeat_interval() {
        env_logger::try_init().unwrap_or_default();
        let start = SystemTime::now();
        let mut pm = PaceMaker::new(
            Some(Duration::from_millis(10)),
            None,
            Some(Duration::from_millis(10)),
            start,
        );
        pm.write_observed(start);
        assert_eq!(
            pm.handle_read_timeout(start + Duration::from_millis(9))
                .expect("handle_read_timeout"),
            BeatAction::Retry
        );
        assert_eq!(
            pm.handle_read_timeout(start + Duration::from_millis(10))
                .expect("handle_read_timeout"),
            BeatAction::SendClientHeart
        );
    }

    #[test]
    fn can_decode_headers_correctly() {
        assert_eq!(&decode_header("moo-\\r\\n\\c\\\\-fish"), "moo-\r\n:\\-fish");
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
