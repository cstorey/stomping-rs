use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use bytes::BytesMut;
use futures::channel::mpsc::{Receiver, Sender};
use futures::{
    future::{BoxFuture, FutureExt, TryFutureExt},
    sink::{Sink, SinkExt},
    stream::{Stream, StreamExt},
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::codec::{Decoder, Encoder, Framed};

use crate::errors::*;
use crate::parser::parse_frame;
use crate::unparser::encode_frame;
use crate::{Frame, FrameOrKeepAlive};

pub(crate) struct StompCodec;

#[must_use = "The connection future must be polled to make progress"]
pub struct Connection {
    s2c: BoxFuture<'static, Result<()>>,
    c2s: BoxFuture<'static, Result<()>>,
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
        c2s_rx: Receiver<Frame>,
        s2c_tx: Sender<Frame>,
        c2s_ka: Option<Duration>,
        s2c_ka: Option<Duration>,
    ) -> Self {
        let (a, b) = inner.split();
        let c2s = Self::run_c2s(a, c2s_rx, c2s_ka).boxed();
        let s2c = Self::run_s2c(b, s2c_tx, s2c_ka).boxed();
        debug!("Built connection process");
        Connection { s2c, c2s }
    }

    async fn run_c2s(
        mut inner: impl Sink<FrameOrKeepAlive, Error = StompError> + Unpin,
        mut c2s_rx: Receiver<Frame>,
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
                Ok(Some(frame)) => {
                    trace!(
                        "Sending to server {:?}/{:?}",
                        frame.command,
                        frame.stringify_headers()
                    );
                    inner.send(FrameOrKeepAlive::Frame(frame)).await?;
                    trace!("Send Done");
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
        mut s2c_tx: Sender<Frame>,
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
                    trace!(
                        "Sending to client {:?}/{:?}",
                        frame.command,
                        frame.stringify_headers()
                    );
                    s2c_tx.send(frame).await?;
                    trace!("Send Done");
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
