use std::{cmp, collections::BTreeMap, future::Future, time::Duration};

use bytes::BytesMut;
use futures::{
    channel::{
        mpsc::{channel, Receiver, Sender},
        oneshot,
    },
    future::TryFutureExt,
    lock::BiLock,
    sink::{Sink, SinkExt},
    stream::{Stream, StreamExt},
};
use log::*;
use maplit::btreemap;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    time::timeout,
};
use tokio_util::codec::{Decoder, Encoder, Framed};

use crate::errors::*;
use crate::parser::parse_frame;
use crate::protocol::{AckMode, Command, Frame, FrameOrKeepAlive, Headers};
use crate::unparser::encode_frame;

pub(crate) struct StompCodec;

#[derive(Debug)]
pub(crate) struct DisconnectReq {
    pub(crate) id: String,
    pub(crate) done: oneshot::Sender<()>,
}

#[derive(Debug)]
pub(crate) struct SubscribeReq {
    pub(crate) destination: String,
    pub(crate) id: String,
    pub(crate) ack_mode: AckMode,
    pub(crate) messages: Sender<Frame>,
    pub(crate) headers: Headers,
}

#[derive(Debug)]
pub(crate) struct PublishReq {
    pub(crate) destination: String,
    pub(crate) body: Vec<u8>,
}

#[derive(Debug)]
pub(crate) struct AckReq {
    pub(crate) message_id: String,
}
#[derive(Debug)]
pub(crate) struct ConnectReq {
    pub(crate) credentials: Option<(String, String)>,
    pub(crate) keepalive: Option<Duration>,
    pub(crate) headers: Headers,
}

#[derive(Debug)]
pub(crate) enum ClientReq {
    Disconnect(DisconnectReq),
    Subscribe(SubscribeReq),
    Publish(PublishReq),
    Ack(AckReq),
}

#[derive(Debug, Default)]
struct ConnectionState {
    subscriptions: BTreeMap<String, Sender<Frame>>,
    receipts: BTreeMap<String, oneshot::Sender<()>>,
}

pub(crate) fn wrap<T: AsyncRead + AsyncWrite>(inner: T) -> Framed<T, StompCodec> {
    Framed::new(inner, StompCodec)
}

impl Encoder<FrameOrKeepAlive> for StompCodec {
    type Error = StompError;
    fn encode(&mut self, item: FrameOrKeepAlive, buf: &mut BytesMut) -> Result<()> {
        encode_frame(buf, &item)
    }
}

impl Decoder for StompCodec {
    type Item = FrameOrKeepAlive;
    type Error = StompError;
    fn decode(&mut self, input: &mut BytesMut) -> Result<Option<FrameOrKeepAlive>> {
        Ok(parse_frame(input)?)
    }
}

async fn run_connection<T: AsyncRead + AsyncWrite + Send + 'static>(
    inner: Framed<T, StompCodec>,
    c2s_rx: Receiver<ClientReq>,
    c2s_ka: Option<Duration>,
    s2c_ka: Option<Duration>,
) -> Result<()> {
    let (a, b) = inner.split();
    let (subs_a, subs_b) = BiLock::new(ConnectionState::default());
    let c2s = run_c2s(a, subs_a, c2s_rx, c2s_ka)
        .inspect_ok(|&()| info!("c2s exited ok"))
        .inspect_err(|e| error!("c2s exited with: {}", e));
    let s2c = run_s2c(b, subs_b, s2c_ka)
        .inspect_ok(|&()| info!("s2c exited ok"))
        .inspect_err(|e| error!("s2c exited with: {}", e));
    debug!("Built connection process");

    // Exit, and cancel the other, when _any one_ of these processes exits.
    tokio::select! {
        res = c2s => res,
        res = s2c => res,
    }
}

async fn run_c2s(
    mut inner: impl Sink<FrameOrKeepAlive, Error = StompError> + Unpin,
    subs: BiLock<ConnectionState>,
    mut c2s_rx: Receiver<ClientReq>,
    keepalive: Option<Duration>,
) -> Result<()> {
    trace!(
        "Awaiting client messages; keepalive interval: {:?} s",
        keepalive.map(|ka| ka.as_secs_f64()),
    );
    loop {
        let it = if let Some(keepalive) = keepalive {
            timeout(keepalive, c2s_rx.next()).await
        } else {
            Ok(c2s_rx.next().await)
        };
        trace!("c2s frame: {:?}", it);

        match it {
            Ok(Some(ClientReq::Disconnect(req))) => {
                let frame = req.to_frame();
                trace!("Sending to server {:?}/{:?}", frame.command, frame.headers);
                {
                    let mut state = subs.lock().await;
                    state.receipts.insert(req.id, req.done);
                };

                inner.send(FrameOrKeepAlive::Frame(frame)).await?;
                trace!("Send Done");
            }
            Ok(Some(ClientReq::Subscribe(req))) => {
                let frame = req.to_frame();
                {
                    let mut state = subs.lock().await;
                    state.subscriptions.insert(req.id, req.messages);
                };
                inner.send(FrameOrKeepAlive::Frame(frame)).await?;
            }
            Ok(Some(ClientReq::Publish(req))) => {
                let frame = req.to_frame();
                inner.send(FrameOrKeepAlive::Frame(frame)).await?;
            }
            Ok(Some(ClientReq::Ack(req))) => {
                let frame = req.to_frame();
                inner.send(FrameOrKeepAlive::Frame(frame)).await?;
            }
            Ok(None) => return Ok(()),
            Err(e) => {
                trace!("Timeout elapsed, sending keepalive: {:?}", e);
                inner.send(FrameOrKeepAlive::KeepAlive).await?;
            }
        }
    }
}

// This function can potentially cause a deadlock. All subscriptions must
// be polled up until both of the reader and writer halves have exited.
// Otherwise, we may end up blocking trying to write to a subscription that
// will never be read. this means, that a connection may be closed, but never
// be observed, as we're blocked attempting to send.
async fn run_s2c(
    mut inner: impl Stream<Item = Result<FrameOrKeepAlive>> + Unpin,
    subs: BiLock<ConnectionState>,
    keepalive: Option<Duration>,
) -> Result<()> {
    let ka_factor = 2;

    trace!(
        "Awaiting server messages; keepalive interval: {:?} s (factor: {})",
        keepalive.map(|ka| ka.as_secs_f64()),
        ka_factor,
    );
    loop {
        let it = if let Some(keepalive) = keepalive {
            timeout(keepalive * ka_factor, inner.next())
                .map_err(|_| StompError::PeerFailed)
                .await?
                .transpose()?
        } else {
            inner.next().await.transpose()?
        };
        trace!("s2c frame: {:?}", it);

        match it {
            Some(FrameOrKeepAlive::KeepAlive) => {
                debug!("Received keepalive.");
            }

            Some(FrameOrKeepAlive::Frame(frame)) => {
                match frame.command {
                    Command::Message => {
                        let subscription_id =
                            frame.headers.get("subscription").cloned().ok_or_else(|| {
                                warn!("MESSAGE frame missing subscription header!");
                                StompError::ProtocolError
                            })?;
                        trace!("Lookup subscription: {:?}", subscription_id);
                        let txp = {
                            let state = subs.lock().await;
                            state.subscriptions.get(&subscription_id).cloned()
                        };

                        if let Some(mut tx) = txp {
                            trace!("Sending to client {:?}/{:?}", frame.command, frame.headers);
                            tx.send(frame).await?;
                            trace!("Send Done");
                        } else {
                            warn!(
                                "Received message for unknown subscription: {:?}",
                                &subscription_id
                            );
                        }
                    }
                    Command::Receipt => {
                        //
                        let receipt_id =
                            frame.headers.get("receipt-id").cloned().ok_or_else(|| {
                                warn!("RECEIPT frame missing receipt header!");
                                StompError::ProtocolError
                            })?;
                        trace!("Lookup receipt: {:?}", receipt_id);
                        let txp = {
                            let mut state = subs.lock().await;
                            state.receipts.remove(&receipt_id)
                        };
                        if let Some(tx) = txp {
                            let _ = tx.send(());
                            trace!("Acked receipt: {:?}", receipt_id)
                        }
                    }
                    Command::Error => {
                        warn!("Error frame received: {:?}", frame.headers);
                        return Err(StompError::StompError(frame.into()));
                    }
                    _ => warn!("Unhandled frame type from server: {:?}", frame.command),
                }
            }
            None => return Ok(()),
        }
    }
}

pub(crate) async fn connect<T: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
    conn: T,
    connect: ConnectReq,
) -> Result<(impl Future<Output = Result<()>>, Sender<ClientReq>)> {
    let mut conn = wrap(conn);

    let connect_frame = connect.to_frame();
    trace!("Sending connect frame");
    conn.send(FrameOrKeepAlive::Frame(connect_frame)).await?;

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
        return Err(StompError::StompError(frame.into()).into());
    } else if frame.command != Command::Connected {
        warn!(
            "Bad response from server: {:?}: {:?}",
            frame.command, frame.headers,
        );
        return Err(StompError::ProtocolError.into());
    }

    let (sx, sy) = parse_keepalive(frame.headers.get("heart-beat").map(|s| &**s))?;

    debug!(
        "heart-beat: cx, cy:{:?}; server-transmit:{:?}; server-receive:{:?}",
        connect.keepalive, sx, sy,
    );
    let c2s_ka = cmp::max(connect.keepalive, sy);
    let s2c_ka = cmp::max(connect.keepalive, sx);

    let (c2s_tx, c2s_rx) = channel(1);

    let mux_fut = run_connection(conn, c2s_rx, c2s_ka, s2c_ka);
    Ok((mux_fut, c2s_tx))
}

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

impl DisconnectReq {
    fn to_frame(&self) -> Frame {
        Frame {
            command: Command::Disconnect,
            headers: btreemap! {
                "receipt".into() => self.id.clone()
            },
            body: Vec::new(),
        }
    }
}

impl SubscribeReq {
    fn to_frame(&self) -> Frame {
        let mut headers = self.headers.clone();

        headers.insert("destination".into(), self.destination.clone());
        headers.insert("id".into(), self.id.clone());
        headers.insert("ack".into(), self.ack_mode.as_str().into());

        Frame {
            command: Command::Subscribe,
            headers: headers,
            body: Vec::new(),
        }
    }
}

impl PublishReq {
    fn to_frame(&self) -> Frame {
        Frame {
            command: Command::Send,
            headers: btreemap! {
                "destination".into() => self.destination.clone(),
                "content-length".into() => self.body.len().to_string(),
            },
            body: self.body.clone(),
        }
    }
}

impl AckReq {
    fn to_frame(&self) -> Frame {
        Frame {
            command: Command::Ack,
            headers: btreemap! {
                "id".into() => self.message_id.clone(),
            },
            body: Vec::new(),
        }
    }
}

impl ConnectReq {
    fn to_frame(&self) -> Frame {
        let mut conn_headers = self.headers.clone();
        conn_headers.insert("accept-version".into(), "1.2".into());
        if let Some(ref duration) = self.keepalive.as_ref() {
            let millis = duration.as_millis();
            conn_headers.insert("heart-beat".into(), format!("{},{}", millis, millis));
        }
        if let Some((user, pass)) = self.credentials.as_ref() {
            conn_headers.insert("login".into(), user.into());
            conn_headers.insert("passcode".into(), pass.into());
        }
        Frame {
            command: Command::Connect,
            headers: conn_headers,
            body: vec![],
        }
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
    fn connect_req_includes_headers() {
        let req = ConnectReq {
            credentials: None,
            keepalive: None,
            headers: btreemap! {
                "x-canary".into() => "Hi!".into(),
            },
        };
        let fr = req.to_frame();

        assert_eq!(fr.headers.get("x-canary").cloned(), Some("Hi!".to_string()),)
    }
    #[test]
    fn subscribe_req_includes_headers() {
        let (messages, _) = channel(0);
        let req = SubscribeReq {
            ack_mode: AckMode::Auto,
            destination: Default::default(),
            id: Default::default(),
            messages: messages,
            headers: btreemap! {
                "x-canary".into() => "Hi!".into(),
            },
        };
        let fr = req.to_frame();

        assert_eq!(fr.headers.get("x-canary").cloned(), Some("Hi!".to_string()),)
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
