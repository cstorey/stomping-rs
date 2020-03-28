use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::channel::{
    mpsc::{channel, Receiver, Sender},
    oneshot,
};
use futures::{future::FutureExt, sink::SinkExt, stream::Stream};

use log::*;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::time::{timeout_at, Instant};

use crate::connection::{
    self, AckReq, ClientReq, ConnectReq, DisconnectReq, PublishReq, SubscribeReq,
};
use crate::errors::*;
use crate::protocol::{AckMode, Frame, Headers};

#[derive(Debug, Clone)]
pub struct Client {
    c2s: Sender<ClientReq>,
}

#[derive(Debug)]
pub struct Subscription {
    s2c: Receiver<Frame>,
}

pub async fn connect<A: ToSocketAddrs>(
    a: A,
    credentials: Option<(&str, &str)>,
    keepalive: Option<Duration>,
    headers: Headers,
) -> Result<(impl Future<Output = Result<()>>, Client)> {
    let req = ConnectReq {
        credentials: credentials.map(|(u, p)| (u.to_string(), p.to_string())),
        keepalive,
        headers,
    };

    let deadline = keepalive.map(|ka| Instant::now() + ka);

    let (mux, c2s_tx) = maybe_timeout_at(deadline, async {
        let conn = TcpStream::connect(a).await?;
        connection::connect(conn, req).await
    })
    .await?;

    let client = Client { c2s: c2s_tx };
    Ok((mux, client))
}

async fn maybe_timeout_at<T, F: Future<Output = Result<T>>>(
    deadline: Option<Instant>,
    fut: F,
) -> Result<T> {
    let res = if let Some(dl) = deadline {
        timeout_at(dl, fut).left_future()
    } else {
        fut.map(Ok).right_future()
    }
    .await??;

    Ok(res)
}

impl Client {
    pub async fn subscribe(
        &mut self,
        destination: &str,
        id: &str,
        mode: AckMode,
        headers: Headers,
    ) -> Result<Subscription> {
        let (tx, rx) = channel(0);
        let req = SubscribeReq {
            destination: destination.to_string(),
            id: id.into(),
            ack_mode: mode,
            messages: tx,
            headers,
        };
        self.c2s.send(ClientReq::Subscribe(req)).await?;
        Ok(Subscription { s2c: rx })
    }
    pub async fn publish(&mut self, destination: &str, body: &[u8]) -> Result<()> {
        let req = PublishReq {
            destination: destination.to_string(),
            body: body.to_vec(),
        };
        self.c2s.send(ClientReq::Publish(req)).await?;
        trace!("Published frame");
        Ok(())
    }

    pub async fn disconnect(mut self) -> Result<()> {
        let (done, rx) = oneshot::channel();
        let id = "42".into();

        let req = DisconnectReq { done, id };
        self.c2s.send(ClientReq::Disconnect(req)).await?;

        rx.await?;

        Ok(())
    }

    pub async fn ack(&mut self, headers: &Headers) -> Result<()> {
        let message_id = headers.get("ack").cloned().ok_or(StompError::NoAckHeader)?;
        let req = AckReq { message_id };
        self.c2s.send(ClientReq::Ack(req)).await?;
        Ok(())
    }
}

impl Stream for Subscription {
    type Item = Frame;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.s2c).poll_next(cx)
    }
}
