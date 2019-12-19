#![cfg(test)]

use std::fmt;

use nom::{
    branch::alt,
    bytes::streaming::{tag, take, take_till},
    character::streaming::*,
    combinator::map,
    multi::{fold_many0, fold_many1},
};
use nom::{Err, IResult};
use thiserror::Error;

use crate::{Command, Frame, Headers};

#[derive(Debug, Error)]
pub struct ParseError<'a> {
    remaining: &'a [u8],
    kind: nom::error::ErrorKind,
}

// See grammar described at https://stomp.github.io/stomp-specification-1.2.html#Augmented_BNF
pub(crate) fn parse_frame(input: &[u8]) -> Result<Option<(&[u8], Frame)>, ParseError> {
    match parse_inner(input) {
        Ok((remainder, frame)) => Ok(Some((remainder, frame))),
        Err(Err::Incomplete(_)) => Ok(None),
        Err(Err::Error(e)) => Err(e.into()),
        Err(Err::Failure(e)) => Err(e.into()),
    }
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

impl<'a> From<(&'a [u8], nom::error::ErrorKind)> for ParseError<'a> {
    fn from((remaining, kind): (&'a [u8], nom::error::ErrorKind)) -> Self {
        ParseError { remaining, kind }
    }
}

impl<'a> fmt::Display for ParseError<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        const MAX_SNIPPET: usize = 80;
        if self.remaining.len() > MAX_SNIPPET {
            write!(
                fmt,
                "{:?}: {:?}…",
                self.kind,
                &self.remaining[..MAX_SNIPPET]
            )
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
        let data = b"CONNECT\n\n\0";

        let result = parse_frame(&*data).expect("parse");
        let (remainder, frame) = result.expect("some frame");
        assert_eq!(b"", remainder);
        assert_eq!(frame.command, Command::Connect);
    }

    #[test]
    fn parse_connect_frame() {
        let data = b"CONNECT\naccept-version:1.2\nlogin:guest\npasscode:guest\n\n\0";

        let result = parse_frame(&*data).expect("parse");
        let (remainder, frame) = result.expect("some frame");
        assert_eq!(b"", remainder);
        assert_eq!(frame.command, Command::Connect);
        assert_eq!(frame.headers.get("login"), Some(&"guest".to_string()));
    }

    #[test]
    fn parse_colon_in_header_name() {
        let data = b"CONNECT\nfoo\\cbar:yes\n\n\0";

        let result = parse_frame(&*data).expect("parse");
        let (remainder, frame) = result.expect("some frame");
        assert_eq!(b"", remainder);
        assert_eq!(frame.headers.get("foo:bar"), Some(&"yes".to_string()));
    }
    #[test]
    fn parse_colon_in_header_value() {
        let data = b"CONNECT\nfoo:one\\ctwo\n\n\0";

        let result = parse_frame(&*data).expect("parse");
        let (remainder, frame) = result.expect("some frame");
        assert_eq!(b"", remainder);
        assert_eq!(frame.headers.get("foo"), Some(&"one:two".to_string()));
    }

    #[test]
    fn parse_send_with_body() {
        let data = b"SEND\n\nwibble\0";

        let result = parse_frame(&*data).expect("parse");
        let (remainder, frame) = result.expect("some frame");
        assert_eq!(b"", remainder);
        assert_eq!(frame.command, Command::Send);
        assert_eq!(&*frame.body, &*b"wibble");
    }

    #[test]
    fn parse_send_with_body_and_content_length() {
        let data = b"SEND\ncontent-length:7\n\nfoo\0bar\0";

        let result = parse_frame(&*data).expect("parse");
        let (remainder, frame) = result.expect("some frame");
        assert_eq!(b"", remainder);
        assert_eq!(frame.command, Command::Send);
        assert_eq!(&*frame.body, &*b"foo\0bar");
    }
}
