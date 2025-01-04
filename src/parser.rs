use std::fmt;

use bytes::{Buf, BytesMut};
use nom::{
    branch::alt,
    bytes::streaming::{tag, take, take_till},
    character::streaming::*,
    combinator::map,
    multi::{fold_many0, many0, many1},
};
use nom::{error::ErrorKind, Err, IResult};
use thiserror::Error;
use tracing::trace;

use crate::protocol::{Command, Frame, FrameOrKeepAlive, Headers};

#[derive(Debug, Error)]
pub struct ParseError {
    remaining: Vec<u8>,
    truncated: bool,
    kind: nom::error::ErrorKind,
}

// See grammar described at https://stomp.github.io/stomp-specification-1.2.html#Augmented_BNF
pub(crate) fn parse_frame(input: &mut BytesMut) -> Result<Option<FrameOrKeepAlive>, ParseError> {
    match run_parse(input) {
        Ok((remainder, frame)) => {
            let consumed = input.len() - remainder.len();

            assert!(
                consumed <= input.len(),
                "consumed:{}; len:{}",
                consumed,
                input.len()
            );

            trace!(?consumed, "Parsed frame");

            input.advance(consumed);

            Ok(Some(frame))
        }
        Err(Err::Incomplete(needed)) => {
            trace!(?needed, have=?input.len(), "Incomplete frame parse");
            Ok(None)
        }
        Err(Err::Error(e)) => Err(e.into()),
        Err(Err::Failure(e)) => Err(e.into()),
    }
}

fn run_parse(input: &[u8]) -> IResult<&[u8], FrameOrKeepAlive> {
    let mut p = alt((
        map(parse_inner, FrameOrKeepAlive::Frame),
        map(parse_keepalive, |()| FrameOrKeepAlive::KeepAlive),
    ));
    p(input)
}

fn parse_keepalive(input: &[u8]) -> IResult<&[u8], ()> {
    let (input, _) = newline(input)?;
    Ok((input, ()))
}

fn parse_inner(input: &[u8]) -> IResult<&[u8], Frame> {
    let (input, command) = parse_command(input)?;

    let (input, headers) = parse_headers(input)?;

    let (input, _) = newline(input)?;

    let content_length = headers.get("content-length").and_then(|ls| ls.parse().ok());

    let (input, body) = parse_body(content_length, input)?;
    let body = body.to_owned();

    let frame = Frame {
        command,
        headers,
        body,
    };
    Ok((input, frame))
}

fn parse_command(input: &[u8]) -> IResult<&[u8], Command> {
    let (input, cmd) = alt((
        map(tag("CONNECT\n"), |_| Command::Connect),
        map(tag("SEND\n"), |_| Command::Send),
        map(tag("SUBSCRIBE\n"), |_| Command::Subscribe),
        map(tag("UNSUBSCRIBE\n"), |_| Command::Unsubscribe),
        map(tag("DISCONNECT\n"), |_| Command::Disconnect),
        map(tag("ACK\n"), |_| Command::Ack),
        map(tag("CONNECTED\n"), |_| Command::Connected),
        map(tag("MESSAGE\n"), |_| Command::Message),
        map(tag("RECEIPT\n"), |_| Command::Receipt),
        map(tag("ERROR\n"), |_| Command::Error),
    ))(input)?;
    Ok((input, cmd))
}

fn parse_headers(input: &[u8]) -> IResult<&[u8], Headers> {
    let (input, headers) = fold_many0(parse_header, Headers::new, |mut headers, (k, v)| {
        headers.insert(k, v);
        headers
    })(input)?;

    Ok((input, headers))
}

fn parse_header(input: &[u8]) -> IResult<&[u8], (String, String)> {
    let (input, name) = many1(parse_header_char)(input)?;
    let (input, _) = char(':')(input)?;

    // ActiveMQ includes literal colons in the value for some headers, such
    // as session id.
    let (input, value) = many0(alt((parse_header_char, map(tag(b":"), |_| b':'))))(input)?;

    let (input, _) = newline(input)?;

    let name = String::from_utf8(name)
        .map_err(|_| nom::Err::Error(nom::error::Error::new(input, ErrorKind::Char)))?;
    let value = String::from_utf8(value)
        .map_err(|_| nom::Err::Error(nom::error::Error::new(input, ErrorKind::Char)))?;

    Ok((input, (name, value)))
}

fn parse_body(content_length: Option<usize>, input: &[u8]) -> IResult<&[u8], &[u8]> {
    let (input, body) = match content_length {
        Some(len) => take(len)(input)?,
        None => take_till(|b| b == b'\0')(input)?,
    };
    let (input, _) = tag(b"\0")(input)?;

    Ok((input, body))
}

fn parse_header_char(input: &[u8]) -> IResult<&[u8], u8> {
    let needed = nom::Needed::Size(std::num::NonZeroUsize::new(1).unwrap());
    match input.split_first().ok_or(nom::Err::Incomplete(needed))? {
        (b'\n', input) => Err(nom::Err::Error(nom::error::Error::new(
            input,
            ErrorKind::Char,
        ))),
        (b':', input) => Err(nom::Err::Error(nom::error::Error::new(
            input,
            ErrorKind::Char,
        ))),
        (b'\\', input) => match input.split_first().ok_or(nom::Err::Incomplete(needed))? {
            (b'c', input) => Ok((input, b':')),
            (b'\\', input) => Ok((input, b'\\')),
            (b'r', input) => Ok((input, b'\r')),
            (b'n', input) => Ok((input, b'\n')),
            _ => Err(nom::Err::Error(nom::error::Error::new(
                input,
                ErrorKind::Char,
            ))),
        },
        (ch, input) => Ok((input, *ch)),
    }
}

impl From<nom::error::Error<&[u8]>> for ParseError {
    fn from(e: nom::error::Error<&[u8]>) -> Self {
        // let (remaining, kind): (&'a [u8], nom::error::ErrorKind) = ();
        const MAX_SNIPPET: usize = 80;
        if e.input.len() > MAX_SNIPPET {
            let remaining = e.input[..MAX_SNIPPET].to_vec();
            let truncated = true;
            ParseError {
                remaining,
                truncated,
                kind: e.code,
            }
        } else {
            let remaining = e.input.to_vec();
            let truncated = false;
            ParseError {
                remaining,
                truncated,
                kind: e.code,
            }
        }
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        if self.truncated {
            write!(fmt, "{:?}: {:?}…", self.kind, &self.remaining)
        } else {
            write!(fmt, "{:?}: {:?}", self.kind, self.remaining)
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;

    use super::*;

    #[test]
    fn parse_connect_frame_no_headers() {
        let mut data = BytesMut::from(b"CONNECT\n\n\0" as &[u8]);

        let result = parse_frame(&mut data).expect("parse");
        let frame = result.expect("some frame").unwrap_frame();
        assert_eq!(b"" as &[u8], &data);
        assert_eq!(frame.command, Command::Connect);
    }

    #[test]
    fn parse_connect_frame() {
        let mut data = BytesMut::from(
            b"CONNECT\naccept-version:1.2\nlogin:guest\npasscode:guest\n\n\0" as &[u8],
        );

        let result = parse_frame(&mut data).expect("parse");
        let frame = result.expect("some frame").unwrap_frame();
        assert_eq!(b"" as &[u8], &data);
        assert_eq!(frame.command, Command::Connect);
        assert_eq!(frame.headers.get("login").cloned(), Some("guest".into()));
    }

    #[test]
    fn parse_escapes_in_headers() {
        let mut data = BytesMut::from(b"CONNECT\n" as &[u8]);
        data.put_slice(b"colon\\c:cr\r\n" as &[u8]);
        data.put_slice(b"slash\\\\:nl\\n\n" as &[u8]);
        data.put_slice(b"\n\0" as &[u8]);

        let result = parse_frame(&mut data).expect("parse");
        let frame = result.expect("some frame").unwrap_frame();
        assert_eq!(b"" as &[u8], &data);
        assert_eq!(frame.headers.get("colon:").cloned(), Some("cr\r".into()));
        assert_eq!(frame.headers.get("slash\\").cloned(), Some("nl\n".into()));
    }

    #[test]
    fn parse_send_with_body() {
        let mut data = BytesMut::from(b"SEND\n\nwibble\0" as &[u8]);

        let result = parse_frame(&mut data).expect("parse");
        let frame = result.expect("some frame").unwrap_frame();
        assert_eq!(b"" as &[u8], &data);
        assert_eq!(frame.command, Command::Send);
        assert_eq!(&*frame.body, b"wibble");
    }

    #[test]
    fn parse_send_with_body_and_content_length() {
        let mut data = BytesMut::from(b"SEND\ncontent-length:7\n\nfoo\0bar\0" as &[u8]);

        let result = parse_frame(&mut data).expect("parse");
        let frame = result.expect("some frame").unwrap_frame();
        assert_eq!(b"" as &[u8], &data);
        assert_eq!(frame.command, Command::Send);
        assert_eq!(&*frame.body, b"foo\0bar");
    }

    #[test]
    fn parse_keepalive() {
        let mut data = BytesMut::from(b"\nstuff" as &[u8]);

        let result = parse_frame(&mut data).expect("parse");
        let frame = result.expect("some frame");
        assert_eq!(b"stuff" as &[u8], &data);
        assert_eq!(FrameOrKeepAlive::KeepAlive, frame);
    }

    // ActiveMQ includes literal colons in their header values.
    #[test]
    fn activemq_example() {
        tracing_subscriber::fmt::try_init().unwrap_or_default();

        let mut data = BytesMut::from(
            b"CONNECTED\n\
            server:ActiveMQ/5.15.10\n\
            heart-beat:0,0\n\
            session:ID:nrdp-prod-01.dsg.caci.co.uk-37320-1573126237601-1:148528\n\
            version:1.2\n\
            \n\
            \0\n" as &[u8],
        );

        let result = parse_frame(&mut data).expect("parse");
        let frame = result.expect("some frame").unwrap_frame();
        assert_eq!(b"\n" as &[u8], &data);
        assert_eq!(frame.command, Command::Connected);
        assert_eq!(&*frame.body, b"");
    }
    #[test]
    fn rabbitmq_example() {
        tracing_subscriber::fmt::try_init().unwrap_or_default();

        let mut data = BytesMut::from(
            b"CONNECTED\n\
            server:RabbitMQ/3.7.8\n\
            session:session-c6cLWDadx0jYlG9s25R5Ag\n\
            heart-beat:0,0\n\
            version:1.2\n\
            \n\
            \0\n" as &[u8],
        );

        let result = parse_frame(&mut data).expect("parse");
        let frame = result.expect("some frame").unwrap_frame();
        assert_eq!(b"\n" as &[u8], &data);
        assert_eq!(frame.command, Command::Connected);
        assert_eq!(&*frame.body, b"");
    }
}
