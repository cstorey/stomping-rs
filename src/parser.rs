#![cfg(test)]

use std::fmt;

use bytes::{Buf, BytesMut};
use nom::{
    branch::alt,
    bytes::streaming::{tag, take, take_till},
    character::streaming::*,
    combinator::map,
    multi::{fold_many0, fold_many1},
};
use nom::{Err, IResult};
use thiserror::Error;

use crate::{Command, Frame, FrameOrKeepAlive, Headers};

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

            input.advance(consumed);

            Ok(Some(frame))
        }
        Err(Err::Incomplete(_)) => return Ok(None),
        Err(Err::Error(e)) => return Err(e.into()),
        Err(Err::Failure(e)) => return Err(e.into()),
    }
}

fn run_parse(input: &[u8]) -> IResult<&[u8], FrameOrKeepAlive> {
    let p = alt((
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
    // headers should go here.
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
    let (input, headers) = fold_many0(parse_header, Headers::new(), |mut headers, (k, v)| {
        headers.insert(k.to_owned(), v.to_owned());
        headers
    })(input)?;

    Ok((input, headers))
}

fn parse_header(input: &[u8]) -> IResult<&[u8], (String, String)> {
    let (input, name) = fold_many1(parse_header_char, String::new(), |mut s, c| {
        s.push(c);
        s
    })(input)?;
    let (input, _) = char(':')(input)?;
    let (input, value) = fold_many0(parse_header_char, String::new(), |mut s, c| {
        s.push(c);
        s
    })(input)?;
    let (input, _) = newline(input)?;

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

fn parse_header_char(input: &[u8]) -> IResult<&[u8], char> {
    let (input, ch) = alt((none_of(":\\\r\n"), map(tag("\\c"), |_| ':')))(input)?;

    Ok((input, ch))
}

impl<'a> From<(&'a [u8], nom::error::ErrorKind)> for ParseError {
    fn from((remaining, kind): (&'a [u8], nom::error::ErrorKind)) -> Self {
        const MAX_SNIPPET: usize = 80;
        if remaining.len() > MAX_SNIPPET {
            let remaining = remaining[..MAX_SNIPPET].to_vec();
            let truncated = true;
            ParseError {
                remaining,
                truncated,
                kind,
            }
        } else {
            let remaining = remaining.to_vec();
            let truncated = false;
            ParseError {
                remaining,
                truncated,
                kind,
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
        assert_eq!(frame.headers.get("login"), Some(&"guest".to_string()));
    }

    #[test]
    fn parse_colon_in_header_name() {
        let mut data = BytesMut::from(b"CONNECT\nfoo\\cbar:yes\n\n\0" as &[u8]);

        let result = parse_frame(&mut data).expect("parse");
        let frame = result.expect("some frame").unwrap_frame();
        assert_eq!(b"" as &[u8], &data);
        assert_eq!(frame.headers.get("foo:bar"), Some(&"yes".to_string()));
    }

    #[test]
    fn parse_colon_in_header_value() {
        let mut data = BytesMut::from(b"CONNECT\nfoo:one\\ctwo\n\n\0" as &[u8]);

        let result = parse_frame(&mut data).expect("parse");
        let frame = result.expect("some frame").unwrap_frame();
        assert_eq!(b"" as &[u8], &data);
        assert_eq!(frame.headers.get("foo"), Some(&"one:two".to_string()));
    }

    #[test]
    fn parse_send_with_body() {
        let mut data = BytesMut::from(b"SEND\n\nwibble\0" as &[u8]);

        let result = parse_frame(&mut data).expect("parse");
        let frame = result.expect("some frame").unwrap_frame();
        assert_eq!(b"" as &[u8], &data);
        assert_eq!(frame.command, Command::Send);
        assert_eq!(&*frame.body, &*b"wibble");
    }

    #[test]
    fn parse_send_with_body_and_content_length() {
        let mut data = BytesMut::from(b"SEND\ncontent-length:7\n\nfoo\0bar\0" as &[u8]);

        let result = parse_frame(&mut data).expect("parse");
        let frame = result.expect("some frame").unwrap_frame();
        assert_eq!(b"" as &[u8], &data);
        assert_eq!(frame.command, Command::Send);
        assert_eq!(&*frame.body, &*b"foo\0bar");
    }

    #[test]
    fn parse_keepalive() {
        let mut data = BytesMut::from(b"\nstuff" as &[u8]);

        let result = parse_frame(&mut data).expect("parse");
        let frame = result.expect("some frame");
        assert_eq!(b"stuff" as &[u8], &data);
        assert_eq!(FrameOrKeepAlive::KeepAlive, frame);
    }
}
