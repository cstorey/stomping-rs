use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::BytesMut;
use futures::channel::mpsc::{Receiver, SendError, Sender};
use futures::{sink::Sink, stream::Stream};
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
    inner: Framed<TcpStream, StompCodec>,
    s2c_tx: Sender<Frame>,
    c2s_rx: Receiver<Frame>,
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
        debug!("Built connection process");
        Connection {
            inner,
            s2c_tx,
            c2s_rx,
        }
    }

    fn poll_s2c(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        loop {
            // Can the client receive any new frames?
            match self.s2c_tx.poll_ready(cx) {
                // If the client isn't ready for more data, don't try and read anything.
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => {
                    trace!("Cient connection dropped; exiting: {:?}", e);
                    let _: SendError = e;
                    return Poll::Ready(Ok(()));
                }
                Poll::Ready(Ok(())) => match Pin::new(&mut self.inner).poll_next(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(None) => {
                        info!("Server disconnected; exiting");
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Ready(Some(Err(e))) => {
                        return Poll::Ready(Err(e.into()));
                    }
                    Poll::Ready(Some(Ok(FrameOrKeepAlive::Frame(frame)))) => {
                        Pin::new(&mut self.s2c_tx).start_send(frame)?;
                    }
                    Poll::Ready(Some(Ok(FrameOrKeepAlive::KeepAlive))) => info!("A keepalive?"),
                },
            };
            futures::ready!(Pin::new(&mut self.s2c_tx).poll_flush(cx))?;
        }
    }

    fn poll_c2s(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        loop {
            // Can the client receive any new frames?
            match Sink::poll_ready(Pin::new(&mut self.inner), cx) {
                // If the client isn't ready for more data, don't try and read anything.
                Poll::Pending => {
                    trace!("c2s: inner pending");
                    return Poll::Pending;
                }
                Poll::Ready(Err(e)) => {
                    trace!("c2s: Err: {:?}", e);
                    return Poll::Ready(Err(e.into()));
                }
                Poll::Ready(Ok(())) => match Pin::new(&mut self.c2s_rx).poll_next(cx) {
                    Poll::Pending => {
                        trace!("c2s: client pending");
                        return Poll::Pending;
                    }
                    Poll::Ready(None) => {
                        info!("Server disconnected; exiting");
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Ready(Some(frame)) => {
                        trace!(
                            "c2s: Sending frame: {:?}: {:?}",
                            frame.command,
                            frame.stringify_headers()
                        );
                        Sink::start_send(
                            Pin::new(&mut self.inner),
                            FrameOrKeepAlive::Frame(frame),
                        )?;
                    }
                },
            };
            futures::ready!(Pin::new(&mut self.inner).poll_flush(cx))?;
        }
    }
}

impl Future for Connection {
    type Output = Result<()>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        trace!("Poll client to server");
        match self.poll_c2s(cx) {
            Poll::Pending => {
                trace!("Client to server pending");
            }
            Poll::Ready(val) => {
                info!("Client to server process finished: {:?}", val);
                return Poll::Ready(val);
            }
        }

        trace!("Poll server to client");
        match self.poll_s2c(cx) {
            Poll::Pending => {
                trace!("Server to client pending");
            }
            Poll::Ready(val) => {
                info!("Server to client process finished: {:?}", val);
                return Poll::Ready(val);
            }
        }

        Poll::Pending
    }
}
