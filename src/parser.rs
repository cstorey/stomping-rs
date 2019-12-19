#![cfg(test)]

use std::fmt;

use nom::{
    branch::alt,
    bytes::streaming::{tag, take_while, take_while1},
    character::streaming::*,
    combinator::map,
    multi::fold_many0,
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
fn parse_frame(input: &[u8]) -> Result<Option<(&[u8], Frame)>, ParseError> {
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
    // Body should be parsed here.
    let body = Vec::new();

    let (input, _) = tag(b"\0")(input)?;

    let frame = Frame {
        command,
        headers,
        body,
    };
    Ok((input, frame))
}

fn parse_command(input: &[u8]) -> IResult<&[u8], Command> {
    let (input, cmd) = alt((
        map(tag("CONNECT"), |_| Command::Connect),
        map(tag("SEND"), |_| Command::Send),
        map(tag("SUBSCRIBE"), |_| Command::Subscribe),
        map(tag("UNSUBSCRIBE"), |_| Command::Unsubscribe),
        map(tag("DISCONNECT"), |_| Command::Disconnect),
        map(tag("ACK"), |_| Command::Ack),
        map(tag("CONNECTED"), |_| Command::Connected),
        map(tag("MESSAGE"), |_| Command::Message),
        map(tag("RECEIPT"), |_| Command::Receipt),
        map(tag("ERROR"), |_| Command::Error),
    ))(input)?;
    let (input, _) = newline(input)?;
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
    let (input, name) = take_while1(is_header_char)(input)?;
    let (input, _) = char(':')(input)?;
    let (input, value) = take_while(is_header_char)(input)?;
    let (input, _) = newline(input)?;

    // This is a horrendous hack. Replace this once we have a good way to
    // parse a &str out of a &[u8].
    let name = name
        .into_iter()
        .map(|&c| std::char::from_u32(c.into()).expect("from char"))
        .collect();
    let value = value
        .into_iter()
        .map(|&c| std::char::from_u32(c.into()).expect("from char"))
        .collect();
    Ok((input, (name, value)))
}

fn is_header_char(ch: u8) -> bool {
    match ch {
        b'\r' | b'\n' | b':' => false,
        _ => true,
    }
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
}
