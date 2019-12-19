#![cfg(test)]

use bytes::{BufMut, BytesMut};
use maplit::*;

use crate::errors::*;
use crate::{Command, Frame, Headers};

fn encode_frame(buf: &mut BytesMut, frame: &Frame) -> Result<()> {
    buf.put_slice(frame.command.as_str().as_bytes());
    buf.put_u8(b'\n');

    for (k, v) in frame.headers.iter() {
        for c in k.as_bytes() {
            match *c {
                b':' => buf.put_slice(b"\\c"),
                _ => buf.put_u8(*c),
            }
        }
        buf.put_u8(b':');
        for c in v.as_bytes() {
            match *c {
                b':' => buf.put_slice(b"\\c"),
                _ => buf.put_u8(*c),
            }
        }
        buf.put_u8(b'\n');
    }

    buf.put_u8(b'\n');

    buf.put_u8(b'\0');

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_trivial_example() {
        let frame = Frame {
            command: Command::Send,
            headers: Headers::new(),
            body: Vec::new(),
        };
        let mut buf = BytesMut::new();

        encode_frame(&mut buf, &frame).expect("encode frame");

        assert_eq!(
            &*"SEND\n\n\0",
            std::str::from_utf8(&buf).expect("from utf8")
        );
    }

    #[test]
    fn should_encode_a_header() {
        let frame = Frame {
            command: Command::Send,
            headers: btreemap! {"hello".into() => "world".into()},
            body: Vec::new(),
        };
        let mut buf = BytesMut::new();

        encode_frame(&mut buf, &frame).expect("encode frame");

        assert_eq!(
            &*"SEND\nhello:world\n\n\0",
            std::str::from_utf8(&buf).expect("from utf8")
        );
    }

    #[test]
    fn should_encode_a_header_colon_in_name() {
        let frame = Frame {
            command: Command::Send,
            headers: btreemap! {"foo:bar".into() => "y".into()},
            body: Vec::new(),
        };
        let mut buf = BytesMut::new();

        encode_frame(&mut buf, &frame).expect("encode frame");

        assert_eq!(
            &*"SEND\nfoo\\cbar:y\n\n\0",
            std::str::from_utf8(&buf).expect("from utf8")
        );
    }

    #[test]
    fn should_encode_a_header_colon_in_value() {
        let frame = Frame {
            command: Command::Send,
            headers: btreemap! {"y".into() => "foo:bar".into()},
            body: Vec::new(),
        };
        let mut buf = BytesMut::new();

        encode_frame(&mut buf, &frame).expect("encode frame");

        assert_eq!(
            &*"SEND\ny:foo\\cbar\n\n\0",
            std::str::from_utf8(&buf).expect("from utf8")
        );
    }
}
