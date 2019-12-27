use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use bytes::BytesMut;
use futures::{
    channel::{
        mpsc::{Receiver, Sender},
        oneshot,
    },
    future::{BoxFuture, FutureExt, TryFutureExt},
    lock::BiLock,
    sink::{Sink, SinkExt},
    stream::{Stream, StreamExt},
};
use log::*;
use maplit::btreemap;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::codec::{Decoder, Encoder, Framed};

use crate::client::{ClientReq, Command, Frame, FrameOrKeepAlive};
use crate::errors::*;
use crate::parser::parse_frame;
use crate::unparser::encode_frame;

pub(crate) struct StompCodec;

#[must_use = "The connection future must be polled to make progress"]
pub struct Connection {
    s2c: BoxFuture<'static, Result<()>>,
    c2s: BoxFuture<'static, Result<()>>,
}

#[derive(Debug, Default)]
struct ConnectionState {
    subscriptions: BTreeMap<Vec<u8>, Sender<Frame>>,
    receipts: BTreeMap<Vec<u8>, oneshot::Sender<()>>,
}

pub(crate) fn wrap<T: AsyncRead + AsyncWrite>(inner: T) -> Framed<T, StompCodec> {
    Framed::new(inner, StompCodec)
}

impl Encoder for StompCodec {
    type Item = FrameOrKeepAlive;
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

impl Connection {
    pub(crate) fn new(
        inner: Framed<TcpStream, StompCodec>,
        c2s_rx: Receiver<ClientReq>,
        c2s_ka: Option<Duration>,
        s2c_ka: Option<Duration>,
    ) -> Self {
        let (a, b) = inner.split();
        let (subs_a, subs_b) = BiLock::new(ConnectionState::default());
        let c2s = Self::run_c2s(a, subs_a, c2s_rx, c2s_ka).boxed();
        let s2c = Self::run_s2c(b, subs_b, s2c_ka).boxed();
        debug!("Built connection process");
        Connection { s2c, c2s }
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

            match it {
                Ok(Some(ClientReq::Disconnect { done })) => {
                    let id = "42".as_bytes().to_vec();
                    let frame = Frame {
                        command: Command::Disconnect,
                        headers: btreemap! {
                            "receipt".as_bytes().to_vec()=> id.clone()
                        },
                        body: Vec::new(),
                    };
                    trace!(
                        "Sending to server {:?}/{:?}",
                        frame.command,
                        frame.stringify_headers()
                    );
                    {
                        let mut state = subs.lock().await;
                        state.receipts.insert(id, done);
                    };

                    inner.send(FrameOrKeepAlive::Frame(frame)).await?;
                    trace!("Send Done");
                }
                Ok(Some(ClientReq::Subscribe {
                    destination,
                    id,
                    ack_mode,
                    messages,
                })) => {
                    let frame = Frame {
                        command: Command::Subscribe,
                        headers: btreemap! {
                            "destination".as_bytes().to_vec() => destination.as_bytes().to_vec(),
                            "id".as_bytes().to_vec() => id.clone(),
                            "ack".as_bytes().to_vec() => ack_mode.as_str().as_bytes().to_vec(),
                        },
                        body: Vec::new(),
                    };
                    {
                        let mut state = subs.lock().await;
                        state.subscriptions.insert(id, messages);
                    };
                    inner.send(FrameOrKeepAlive::Frame(frame)).await?;
                }
                Ok(Some(ClientReq::Publish { destination, body })) => {
                    let frame = Frame {
                        command: Command::Send,
                        headers: btreemap! {
                            "destination".as_bytes().to_vec() => destination.as_bytes().to_vec(),
                            "content-length".as_bytes().to_vec() => body.len().to_string().into_bytes(),
                        },
                        body: body,
                    };
                    inner.send(FrameOrKeepAlive::Frame(frame)).await?;
                }
                Ok(Some(ClientReq::Ack { message_id })) => {
                    let frame = Frame {
                        command: Command::Ack,
                        headers: btreemap! {
                            "id".as_bytes().to_vec() => message_id,
                        },
                        body: Vec::new(),
                    };
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

            match it {
                Some(FrameOrKeepAlive::KeepAlive) => {
                    debug!("Received keepalive.");
                }

                Some(FrameOrKeepAlive::Frame(frame)) => {
                    match frame.command {
                        Command::Message => {
                            let subscription_id = frame
                                .headers
                                .get("subscription".as_bytes())
                                .cloned()
                                .ok_or_else(|| {
                                    warn!("MESSAGE frame missing subscription header!");
                                    StompError::ProtocolError
                                })?;
                            trace!(
                                "Lookup subscription: {:?}",
                                String::from_utf8_lossy(&subscription_id)
                            );
                            let txp = {
                                let state = subs.lock().await;
                                state.subscriptions.get(&subscription_id).cloned()
                            };

                            if let Some(mut tx) = txp {
                                trace!(
                                    "Sending to client {:?}/{:?}",
                                    frame.command,
                                    frame.stringify_headers()
                                );
                                tx.send(frame).await?;
                                trace!("Send Done");
                            } else {
                                warn!(
                                    "Received message for unknown subscription: {:?}",
                                    String::from_utf8_lossy(&subscription_id)
                                );
                            }
                        }
                        Command::Receipt => {
                            //
                            let receipt_id = frame
                                .headers
                                .get("receipt-id".as_bytes())
                                .cloned()
                                .ok_or_else(|| {
                                    warn!("RECEIPT frame missing receipt header!");
                                    StompError::ProtocolError
                                })?;
                            trace!("Lookup receipt: {:?}", String::from_utf8_lossy(&receipt_id));
                            let txp = {
                                let mut state = subs.lock().await;
                                state.receipts.remove(&receipt_id)
                            };
                            if let Some(tx) = txp {
                                let _ = tx.send(());
                                trace!("Acked receipt: {:?}", String::from_utf8_lossy(&receipt_id))
                            }
                        }
                        _ => warn!("Unhandled frame type from server: {:?}", frame.command),
                    }
                }
                None => return Ok(()),
            }
        }
    }
}

impl Future for Connection {
    type Output = Result<()>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        trace!("Poll client to server");
        if let Poll::Ready(val) = self.c2s.as_mut().poll(cx) {
            info!("Client to server process finished: {:?}", val);
            return Poll::Ready(val);
        }

        trace!("Poll server to client");
        if let Poll::Ready(val) = self.s2c.as_mut().poll(cx) {
            info!("Server to client process finished: {:?}", val);
            return Poll::Ready(val);
        }

        return Poll::Pending;
    }
}
