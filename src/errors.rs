use thiserror::Error;

use crate::parser::ParseError;
use crate::protocol::Frame;

pub type Result<T> = std::result::Result<T, StompError>;

#[derive(Debug, Error)]
pub enum StompError {
    #[error("stomp error: {0:?}")]
    StompError(Frame),
    #[error("Protocol error")]
    ProtocolError,
    #[error("Tried to ack a frame with no `ack` header")]
    NoAckHeader,
    #[error("peer seems to be unresponsive")]
    PeerFailed,
    #[error("system time: {0}")]
    SystemTime(#[from] std::time::SystemTimeError),
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("parse integer: {0}")]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("parse integer: {0}")]
    ProtocolParse(#[from] ParseError),
    #[error("Parse utf8: {0}")]
    Utf8(#[from] std::str::Utf8Error),
    #[error("Client dropped")]
    ClientDropped,
    #[error("Connection dropped: {0}")]
    ConnectionDropped(#[from] futures::channel::mpsc::SendError),
    #[error("Connection dropped: {0}")]
    ConnectionDropped2(#[from] futures::channel::oneshot::Canceled),
    #[error("Timeout: {0}")]
    TimedOut(#[from] tokio::time::Elapsed),
}
