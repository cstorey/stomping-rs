#![cfg(test)]

use bytes::{BufMut, BytesMut};

use crate::errors::*;
use crate::{Command, Frame, Headers};

fn encode_frame(buf: &mut BytesMut, frame: &Frame) -> Result<()> {
    buf.put_slice(frame.command.as_str().as_bytes());
    buf.put_u8(b'\n');

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

        assert_eq!(&*b"SEND\n\n\0", &*buf);
    }
}
