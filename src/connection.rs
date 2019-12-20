use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::BytesMut;
use futures::channel::mpsc::{Receiver, Sender};
use futures::{
    future::{BoxFuture, FutureExt},
    sink::{Sink, SinkExt},
    stream::{Stream, StreamExt},
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
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
    ) -> Self {
        let (a, b) = inner.split();
        let c2s = Self::run_c2s(a, c2s_rx).boxed();
        let s2c = Self::run_s2c(b, s2c_tx).boxed();
        debug!("Built connection process");
        Connection { s2c, c2s }
    }

    async fn run_c2s(
        mut inner: impl Sink<FrameOrKeepAlive, Error = StompError> + Unpin,
        mut c2s_rx: Receiver<Frame>,
    ) -> Result<()> {
        // We can't just use `send_all` here, as we need BiLock to share
        // the connection between sub-processes.
        trace!("Awaiting client messages");
        while let Some(frame) = c2s_rx.next().await {
            trace!(
                "Sending to server {:?}/{:?}",
                frame.command,
                frame.stringify_headers()
            );
            inner.send(FrameOrKeepAlive::Frame(frame)).await?;
            trace!("Send Done");
        }
        return Ok(());
    }

    async fn run_s2c(
        mut inner: impl Stream<Item = Result<FrameOrKeepAlive>> + Unpin,
        mut s2c_tx: Sender<Frame>,
    ) -> Result<()> {
        trace!("Awaiting server messages");
        while let Some(item) = inner.next().await.transpose()? {
            match item {
                FrameOrKeepAlive::KeepAlive => {
                    debug!("A keepalive???");
                }
                FrameOrKeepAlive::Frame(frame) => {
                    trace!(
                        "Sending to client {:?}/{:?}",
                        frame.command,
                        frame.stringify_headers()
                    );
                    s2c_tx.send(frame).await?;
                    trace!("Send Done");
                }
            }
        }
        return Ok(());
    }
}

impl Future for Connection {
    type Output = Result<()>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        trace!("Poll client to server");
        let c2s = self.c2s.as_mut().poll(cx);

        match c2s {
            Poll::Pending => {
                trace!("Client to server pending");
            }
            Poll::Ready(val) => {
                info!("Client to server process finished: {:?}", val);
                return Poll::Ready(val);
            }
        }

        trace!("Poll server to client");
        let s2c = self.s2c.as_mut().poll(cx);
        match s2c {
            Poll::Pending => {
                trace!("Server to client pending");
            }
            Poll::Ready(val) => {
                info!("Server to client process finished: {:?}", val);
                return Poll::Ready(val);
            }
        }

        return Poll::Pending;
        /*
        trace!("Poll client to server");
        let res = self.c2s.as_mut().poll(cx);
        trace!("Connection poll done: {:?}", res);
        res
        */
    }
}
