use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Clone, Eq, PartialEq, Debug, Error)]
pub enum StompError {
    #[error("stomp error: {}: {:?}: {:?}", _0, _1, _2)]
    StompError(String, BTreeMap<String, String>, String),
    #[error("Protocol error")]
    ProtocolError,
    #[error("Tried to ack a frame with no `ack` header")]
    NoAckHeader,
    #[error("peer seems to be unresponsive")]
    PeerFailed,
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
