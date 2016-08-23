use std::{io, num};

error_chain! (
    foreign_links {
        io::Error, Io;
        num::ParseIntError, ParseInt;
    }

    errors {
        ProtocolError {
            description("protocol error")
        }
    }
);
