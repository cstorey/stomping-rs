use std::{io, num};
use std::collections::BTreeMap;
use std::time;

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
    }
);
