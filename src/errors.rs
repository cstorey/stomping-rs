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
    #[error("system time")]
    SystemTime(#[from] std::time::SystemTimeError),
    #[error("I/O")]
    Io(#[from] std::io::Error),
    #[error("parse integer")]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("parse integer")]
    ProtocolParse(#[from] ParseError),
    #[error("Parse utf8")]
    Utf8(#[from] std::str::Utf8Error),
    #[error("Client dropped")]
    ClientDropped,
    #[error("Connection dropped")]
    ConnectionDropped(#[from] futures::channel::mpsc::SendError),
    #[error("Connection dropped")]
    ConnectionDropped2(#[from] futures::channel::oneshot::Canceled),
    #[error("Timeout")]
    TimedOut(#[from] tokio::time::Elapsed),
}

#[cfg(never)]
error_chain! (
    foreign_links {
        io::Error, Io;
        num::ParseIntError, ParseInt;
        time::SystemTimeError, SystemTime;
    }

    errors {
        StompError(command: String, headers:BTreeMap<String, String>, body: String) {
            description("stomp error")
            display("stomp error: {}: {:?}: {:?}", command, headers, body)
        }
        ProtocolError {
            description("protocol error")
        }
        NoAckHeader {
            description("Tried to ack a frame with no `ack` header")
        }
        PeerFailed {
            description("peer seems to be unresponsive")
        }
    }
);
