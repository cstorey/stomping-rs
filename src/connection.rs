use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{Decoder, Encoder, Framed};

use crate::errors::*;
use crate::parser::parse_frame;
use crate::unparser::encode_frame;
use crate::FrameOrKeepAlive;

pub(crate) struct StompCodec;

#[must_use = "The connection future must be polled to make progress"]
pub struct Connection {}

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

impl Future for Connection {
    type Output = Result<()>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        unimplemented!()
    }
}
