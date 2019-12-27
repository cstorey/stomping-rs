use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::channel::{
    mpsc::{channel, Receiver, Sender},
    oneshot,
};
use futures::{sink::SinkExt, stream::Stream};
use log::*;
use tokio::net::{TcpStream, ToSocketAddrs};

use crate::connection::{
    self, AckReq, ClientReq, ConnectReq, Connection, DisconnectReq, PublishReq, SubscribeReq,
};
use crate::errors::*;
use crate::protocol::{AckMode, Frame, Headers};

#[derive(Debug)]
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
) -> Result<(Connection, Client)> {
    let conn = TcpStream::connect(a).await?;

    let req = ConnectReq {
        credentials: credentials.map(|(u, p)| (u.to_string(), p.to_string())),
        keepalive,
        headers,
    };

    let (mux, c2s_tx) = connection::connect(conn, req).await?;

    let client = Client { c2s: c2s_tx };
    Ok((mux, client))
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
            id: id.as_bytes().to_vec(),
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
        let id = "42".as_bytes().to_vec();

        let req = DisconnectReq { done, id };
        self.c2s.send(ClientReq::Disconnect(req)).await?;

        rx.await?;

        Ok(())
    }

    pub async fn ack(&mut self, headers: &Headers) -> Result<()> {
        let message_id = headers
            .get("ack".as_bytes())
            .map(|v| v.to_vec())
            .ok_or(StompError::NoAckHeader)?;
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
