extern crate failure;
#[macro_use]
extern crate failure_derive;
#[macro_use]
extern crate log;
use failure::Fallible;
use std::cmp;
use std::collections::BTreeMap;
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::{Duration, SystemTime};

mod errors;
use errors::*;

pub struct Client {
    wr: BufWriter<TcpStream>,
    rdr: BufReader<TcpStream>,
    pace: PaceMaker,
}

pub enum AckMode {
    Auto,
    ClientIndividual,
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

fn encode_header(s: &str) -> String {
    let mut out = String::new();
    for c in s.chars() {
        match c {
            '\r' => out.push_str("\\r"),
            '\n' => out.push_str("\\n"),
            '\\' => out.push_str("\\\\"),
            ':' => out.push_str("\\c"),
            c => out.push(c),
        }
    }
    out
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

fn parse_keepalive(headervalue: Option<&str>) -> Fallible<(Option<Duration>, Option<Duration>)> {
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
    pub fn connect<A: ToSocketAddrs>(
        a: A,
        credentials: Option<(&str, &str)>,
        keepalive: Option<Duration>,
    ) -> Fallible<Self> {
        let start = SystemTime::now();
        let wr = TcpStream::connect(a)?;
        debug!("connected to: {:?}", wr.peer_addr()?);
        wr.set_read_timeout(keepalive.map(|d| d * 3))?;
        let rdr = wr.try_clone()?;
        let mut conn_headers = BTreeMap::new();
        let mut client = Client {
            wr: BufWriter::new(wr),
            rdr: BufReader::new(rdr),
            pace: PaceMaker::new(None, None, None, start),
        };
        conn_headers.insert("accept-version".to_string(), "1.2".to_string());
        if let &Some(ref duration) = &keepalive {
            let millis = duration.as_secs() * 1000 + duration.subsec_nanos() as u64 / 1000_000;
            conn_headers.insert("heart-beat".to_string(), format!("{},{}", millis, millis));
        }
        if let Some((user, pass)) = credentials {
            conn_headers.insert("login".to_string(), user.to_string());
            conn_headers.insert("passcode".to_string(), pass.to_string());
        }
        client.send("CONNECT", conn_headers, &[])?;

        let (cmd, hdrs, body) = client.read_frame()?;
        if &cmd == "ERROR" {
            let body = String::from_utf8_lossy(&body).into_owned();
            warn!("Error response from server: {:?}: {:?}", cmd, hdrs);
            return Err(StompError::StompError(cmd, hdrs, body).into());
        } else if &cmd != "CONNECTED" {
            warn!("Bad response from server: {:?}: {:?}", cmd, hdrs);
            return Err(StompError::ProtocolError.into());
        }

        let (sx, sy) = parse_keepalive(hdrs.get("heart-beat").map(|s| &**s))?;
        client.pace = PaceMaker::new(keepalive, sx, sy, start);

        client.reset_timeouts(None)?;
        Ok(client)
    }

    fn reset_timeouts(&mut self, deadline: Option<SystemTime>) -> Fallible<()> {
        let now = SystemTime::now();
        let remaining = deadline.and_then(|dl| dl.duration_since(now).ok());
        debug!("#reset_timeouts: remaining: {:?}", remaining);
        let timeout = match (remaining, self.pace.read_timeout(now)) {
            (Some(t), Some(rt)) => Some(cmp::min(t, rt)),
            (Some(t), None) => Some(t),
            (None, Some(rt)) => Some(rt),
            (None, None) => None,
        };

        self.wr.get_mut().set_read_timeout(timeout)?;
        self.rdr.get_mut().set_read_timeout(timeout)?;
        Ok(())
    }

    pub fn subscribe(&mut self, destination: &str, id: &str, mode: AckMode) -> Fallible<()> {
        let mut h = BTreeMap::new();
        h.insert("destination".to_string(), destination.to_string());
        h.insert("id".to_string(), id.to_string());
        h.insert("ack".to_string(), mode.as_str().to_string());
        self.send("SUBSCRIBE", h, b"")?;
        Ok(())
    }
    pub fn publish(&mut self, destination: &str, body: &[u8]) -> Fallible<()> {
        let mut h = BTreeMap::new();
        h.insert("destination".to_string(), destination.to_string());
        h.insert("content-length".to_string(), format!("{}", body.len()));
        self.send("SEND", h, body)?;
        Ok(())
    }
    pub fn ack(&mut self, headers: &Headers) -> Fallible<()> {
        let mut h = BTreeMap::new();
        let mid = headers.get("ack").ok_or(StompError::NoAckHeader)?;
        h.insert("id".to_string(), mid.to_string());
        self.send("ACK", h, &[])?;
        Ok(())
    }

    pub fn consume_next(&mut self) -> Fallible<(Headers, Vec<u8>)> {
        let (cmd, hdrs, body) = self.read_frame()?;
        if &cmd != "MESSAGE" {
            warn!("Bad message from server: {:?}: {:?}", cmd, hdrs);
            return Err(StompError::ProtocolError.into());
        }

        Ok((hdrs, body))
    }

    pub fn maybe_consume_next(
        &mut self,
        timeout: Duration,
    ) -> Fallible<Option<(Headers, Vec<u8>)>> {
        if let Some((cmd, hdrs, body)) = self.maybe_read_frame(timeout)? {
            if &cmd != "MESSAGE" {
                warn!("Bad message from server: {:?}: {:?}", cmd, hdrs);
                return Err(StompError::ProtocolError.into());
            }

            Ok(Some((hdrs, body)))
        } else {
            Ok(None)
        }
    }

    pub fn disconnect(mut self) -> Fallible<()> {
        let mut h = BTreeMap::new();
        h.insert("receipt".to_string(), "42".to_string());
        self.send("DISCONNECT", h, b"")?;

        loop {
            let (cmd, hdrs, _) = self.read_frame()?;
            if &cmd == "RECEIPT" {
                break;
            } else {
                warn!("Unexpected message from server: {:?}: {:?}", cmd, hdrs);
            }
        }

        Ok(())
    }

    fn send(
        &mut self,
        command: &str,
        headers: BTreeMap<String, String>,
        body: &[u8],
    ) -> Fallible<()> {
        writeln!(self.wr, "{}", command)?;
        for (k, v) in headers {
            writeln!(self.wr, "{}:{}", encode_header(&k), encode_header(&v))?;
        }
        writeln!(self.wr, "")?;

        self.wr.write_all(body)?;
        self.wr.write(b"\0")?;
        self.wr.flush()?;
        self.pace.write_observed(SystemTime::now());
        self.reset_timeouts(None)?;
        Ok(())
    }

    fn read_line(&mut self, buf: &mut String, timeout: Option<SystemTime>) -> Fallible<Option<()>> {
        loop {
            let deadline_passed = timeout.map(|to| to <= SystemTime::now()).unwrap_or(false);
            if deadline_passed {
                return Ok(None);
            }

            self.reset_timeouts(timeout)?;
            let result = self.rdr.read_line(buf);
            debug!("read line result: {:?}", result);
            match result {
                Ok(_) => {
                    self.pace.read_observed(SystemTime::now());
                    self.reset_timeouts(None)?;
                    return Ok(Some(()));
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    let action = self.pace.handle_read_timeout(SystemTime::now())?;
                    debug!("Would block: {:?}; timeout? {:?}", action, timeout);
                    match action {
                        BeatAction::Retry => self.reset_timeouts(None)?,
                        BeatAction::SendClientHeart => {
                            self.wr.write_all(b"\n")?;
                            self.wr.flush()?;
                            self.pace.write_observed(SystemTime::now());
                            self.reset_timeouts(None)?;
                            debug!("Sent heartbeat");
                        }
                        BeatAction::PeerFailed => return Err(StompError::PeerFailed.into()),
                    };
                }
                Err(e) => {
                    warn!("Read returned: kind:{:?}", e.kind());
                    return Err(e.into());
                }
            };
        }
    }
    fn maybe_read_frame(
        &mut self,
        timeout: Duration,
    ) -> Fallible<Option<(String, Headers, Vec<u8>)>> {
        let mut buf = String::new();
        let start = SystemTime::now();
        let deadline = start + timeout;
        while buf.trim().is_empty() {
            buf.clear();
            let elapsed = start.elapsed()?;
            if elapsed >= timeout {
                debug!("Timeout expired");
                return Ok(None);
            }
            debug!("Timeout Remaining: {:?}", timeout - elapsed);
            if let Some(()) = self.read_line(&mut buf, Some(deadline))? {
                trace!("Read command line: {:?}", buf);
                assert!(!buf.is_empty());
            } else {
                return Ok(None);
            }
        }
        let command = buf.trim().to_string();
        self.read_frame_headers_body(command).map(Some)
    }

    fn read_frame(&mut self) -> Fallible<(String, Headers, Vec<u8>)> {
        let mut buf = String::new();
        while buf.trim().is_empty() {
            buf.clear();
            self.read_line(&mut buf, None)?;
            trace!("Read command line: {:?}", buf);
            assert!(!buf.is_empty());
        }
        let command = buf.trim().to_string();
        self.read_frame_headers_body(command)
    }

    fn read_frame_headers_body(&mut self, command: String) -> Fallible<(String, Headers, Vec<u8>)> {
        let mut buf = String::new();
        let mut headers = BTreeMap::new();

        // Given we are half way into a frame, we can be reasonably sure that
        // more data is coming soon, so reset our timeouts.
        self.reset_timeouts(None)?;
        loop {
            buf.clear();
            self.rdr.read_line(&mut buf)?;
            trace!("Read header line: {:?}", buf);
            if buf == "\n" {
                break;
            }
            let mut it = buf.trim().splitn(2, ':');
            let name = it.next().ok_or(StompError::ProtocolError)?;
            let value = it.next().ok_or(StompError::ProtocolError)?;
            headers.insert(decode_header(name), decode_header(value));
        }
        trace!("Reading body");
        let mut buf = Vec::new();
        if let Some(lenstr) = headers.get("content-length") {
            let nbytes: u64 = lenstr.parse()?;
            trace!("Read bytes: {}", nbytes);
            self.rdr.by_ref().take(nbytes + 1).read_to_end(&mut buf)?;
        } else {
            trace!("Read until nul");
            self.rdr.read_until(b'\0', &mut buf)?;
        }
        trace!("Read body: {:?}", buf);
        if buf.pop() != Some(b'\0') {
            warn!("No null at end of body");
            return Err(StompError::ProtocolError.into());
        }

        let frame = (command, headers, buf);
        trace!("read frame: {:?}", frame);
        Ok(frame)
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

    fn handle_read_timeout(&mut self, at: SystemTime) -> Fallible<BeatAction> {
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
    extern crate env_logger;
    use super::*;
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
    fn can_encode_headers_correctly() {
        assert_eq!(&encode_header("\r\n:\\"), "\\r\\n\\c\\\\");
        assert_eq!(&encode_header("foobar"), "foobar")
    }

    #[test]
    fn can_decode_headers_correctly() {
        assert_eq!(&decode_header("moo-\\r\\n\\c\\\\-fish"), "moo-\r\n:\\-fish");
    }
}
