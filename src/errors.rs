use std::io;

error_chain! (
    foreign_links {
        io::Error, Io;
    }

    errors {
        ProtocolError {
            description("protocol error")
        }
    }
);
