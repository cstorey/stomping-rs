use std::cmp;
use std::collections::BTreeMap;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::channel::{
    mpsc::{channel, Receiver, Sender},
    oneshot,
};
use futures::{
    sink::SinkExt,
    stream::{Stream, StreamExt},
};
use log::*;
use tokio::net::{TcpStream, ToSocketAddrs};

use crate::connection::{self, Connection};
use crate::errors::*;
use crate::protocol::{AckMode, Command, Frame, FrameOrKeepAlive, Headers};

#[derive(Debug)]
pub struct Client {
    c2s: Sender<ClientReq>,
}

#[derive(Debug)]
pub struct Subscription {
    s2c: Receiver<Frame>,
}

#[derive(Debug)]
pub(crate) enum ClientReq {
    Disconnect {
        done: oneshot::Sender<()>,
    },
    Subscribe {
        destination: String,
        id: Vec<u8>,
        ack_mode: AckMode,
        messages: Sender<Frame>,
    },
    Publish {
        destination: String,
        body: Vec<u8>,
    },
    Ack {
        message_id: Vec<u8>,
    },
}

pub async fn connect<A: ToSocketAddrs>(
    a: A,
    credentials: Option<(&str, &str)>,
    keepalive: Option<Duration>,
) -> Result<(Connection, Client)> {
    let conn = TcpStream::connect(a).await?;

    let mut conn = connection::wrap(conn);

    let mut conn_headers = BTreeMap::new();
    conn_headers.insert(
        "accept-version".as_bytes().to_vec(),
        "1.2".as_bytes().to_vec(),
    );
    if let &Some(ref duration) = &keepalive {
        let millis = duration.as_millis();
        conn_headers.insert(
            "heart-beat".as_bytes().to_vec(),
            format!("{},{}", millis, millis).as_bytes().to_vec(),
        );
    }
    if let Some((user, pass)) = credentials {
        conn_headers.insert("login".as_bytes().to_vec(), user.as_bytes().to_vec());
        conn_headers.insert("passcode".as_bytes().to_vec(), pass.as_bytes().to_vec());
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
            frame.command,
            frame.stringify_headers(),
        );
        return Err(StompError::ProtocolError.into());
    }

    let (sx, sy) = parse_keepalive(frame.headers.get("heart-beat".as_bytes()).map(|s| &**s))?;

    debug!(
        "heart-beat: cx, cy:{:?}; server-transmit:{:?}; server-receive:{:?}",
        keepalive, sx, sy,
    );
    let c2s_ka = cmp::max(keepalive, sy);
    let s2c_ka = cmp::max(keepalive, sx);

    let (c2s_tx, c2s_rx) = channel(1);
    let client = Client { c2s: c2s_tx };
    let mux = Connection::new(conn, c2s_rx, c2s_ka, s2c_ka);
    Ok((mux, client))
}

fn parse_keepalive(headervalue: Option<&[u8]>) -> Result<(Option<Duration>, Option<Duration>)> {
    if let Some(sxsy) = headervalue {
        let sxsy = std::str::from_utf8(sxsy)?;
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
    pub async fn subscribe(
        &mut self,
        destination: &str,
        id: &str,
        mode: AckMode,
    ) -> Result<Subscription> {
        let (tx, rx) = channel(0);
        self.c2s
            .send(ClientReq::Subscribe {
                destination: destination.to_string(),
                id: id.as_bytes().to_vec(),
                ack_mode: mode,
                messages: tx,
            })
            .await?;
        Ok(Subscription { s2c: rx })
    }
    pub async fn publish(&mut self, destination: &str, body: &[u8]) -> Result<()> {
        self.c2s
            .send(ClientReq::Publish {
                destination: destination.to_string(),
                body: body.to_vec(),
            })
            .await?;
        trace!("Published frame");
        Ok(())
    }

    pub async fn disconnect(mut self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.c2s.send(ClientReq::Disconnect { done: tx }).await?;

        rx.await?;

        Ok(())
    }

    pub async fn ack(&mut self, headers: &Headers) -> Result<()> {
        let message_id = headers
            .get("ack".as_bytes())
            .map(|v| v.to_vec())
            .ok_or(StompError::NoAckHeader)?;
        self.c2s.send(ClientReq::Ack { message_id }).await?;
        Ok(())
    }
}

impl Stream for Subscription {
    type Item = Frame;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.s2c).poll_next(cx)
    }
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
            parse_keepalive(Some(b"0,0")).expect("parse_keepalive"),
            (None, None)
        );
    }

    #[test]
    fn keepalives_parse_zero_as_none_1() {
        env_logger::try_init().unwrap_or_default();
        assert_eq!(
            parse_keepalive(Some(b"0,42")).expect("parse_keepalive"),
            (None, Some(Duration::from_millis(42)))
        );
    }

    #[test]
    fn keepalives_parse_zero_as_none_2() {
        env_logger::try_init().unwrap_or_default();
        assert_eq!(
            parse_keepalive(Some(b"42,0")).expect("parse_keepalive"),
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
