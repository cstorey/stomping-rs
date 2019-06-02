use std::collections::BTreeMap;

#[derive(Clone, Eq, PartialEq, Debug, Fail)]
pub enum StompError {
    #[fail(display = "stomp error: {}: {:?}: {:?}", _0, _1, _2)]
    StompError(String, BTreeMap<String, String>, String),
    #[fail(display = "Protocol error")]
    ProtocolError,
    #[fail(display = "Tried to ack a frame with no `ack` header")]
    NoAckHeader,
    #[fail(display = "peer seems to be unresponsive")]
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
