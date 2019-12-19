#![cfg(test)]

use bytes::{BufMut, BytesMut};
use maplit::*;

use crate::errors::*;
use crate::{Command, Frame, Headers};

fn encode_frame(buf: &mut BytesMut, frame: &Frame) -> Result<()> {
    buf.put_slice(frame.command.as_str().as_bytes());
    buf.put_u8(b'\n');

    for (k, v) in frame.headers.iter() {
        if k.len() == 0 {
            return Err(StompError::ProtocolError);
        }
        encode_header_label(buf, k);
        buf.put_u8(b':');
        encode_header_label(buf, v);
        buf.put_u8(b'\n');
    }

    buf.put_u8(b'\n');

    buf.put_slice(&frame.body);

    buf.put_u8(b'\0');

    Ok(())
}

fn encode_header_label(buf: &mut BytesMut, label: &str) {
    for c in label.as_bytes() {
        match *c {
            b':' => buf.put_slice(b"\\c"),
            b'\r' => buf.put_slice(b"\\r"),
            b'\n' => buf.put_slice(b"\\n"),
            _ => buf.put_u8(*c),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use suppositions::{generators::Generator, property};

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

    #[test]
    fn should_encode_newline_in_name() {
        let frame = Frame {
            command: Command::Send,
            headers: btreemap! {"\n".into() => "y".into()},
            body: Vec::new(),
        };
        let mut buf = BytesMut::new();

        encode_frame(&mut buf, &frame).expect("encode frame");

        assert_eq!(
            &*"SEND\n\\n:y\n\n\0",
            std::str::from_utf8(&buf).expect("from utf8")
        );
    }
    #[test]
    fn should_encode_return_in_value() {
        let frame = Frame {
            command: Command::Send,
            headers: btreemap! {"x".into() => "\r".into()},
            body: Vec::new(),
        };

        let mut buf = BytesMut::new();

        encode_frame(&mut buf, &frame).expect("encode frame");

        assert_eq!(
            &*"SEND\nx:\\r\n\n\0",
            std::str::from_utf8(&buf).expect("from utf8")
        );
    }

    #[test]
    fn should_fail_on_empty_header_name() {
        let frame = Frame {
            command: Command::Send,
            headers: btreemap! {"".into() => "y".into()},
            body: Vec::new(),
        };

        let mut buf = BytesMut::new();

        let res = encode_frame(&mut buf, &frame);

        assert!(res.is_err(), "Encoding should fail; got: {:?}", res);
    }

    #[test]
    fn should_encode_body() {
        let frame = Frame {
            command: Command::Send,
            headers: Headers::new(),
            body: b"x".to_vec(),
        };
        let mut buf = BytesMut::new();

        encode_frame(&mut buf, &frame).expect("encode frame");

        assert_eq!(
            &*"SEND\n\nx\0",
            std::str::from_utf8(&buf).expect("from utf8")
        );
    }

    #[ignore]
    #[test]
    fn should_round_trip_frames_without_content_length() {
        use crate::parser::parse_frame;

        env_logger::try_init().unwrap_or(());
        property(frames().filter(|frame| !frame.body.contains(&b'\0'))).check(|frame| {
            let mut buf = BytesMut::new();

            encode_frame(&mut buf, &frame).expect("encode frame");
            let (remaining, parsed) = parse_frame(&buf).expect("parse").expect("some frame");

            assert_eq!(frame, parsed);
            assert!(
                remaining.len() == 0,
                "Remaining should be empty: {}",
                String::from_utf8_lossy(remaining)
            )
        })
    }

    #[test]
    fn should_round_trip_trivial_frame() {
        use crate::parser::parse_frame;
        let frame = Frame {
            command: Command::Connected,
            headers: Default::default(),
            body: Default::default(),
        };
        println!("Frame: {:?}", frame);

        let mut buf = BytesMut::new();
        encode_frame(&mut buf, &frame).expect("encode frame");
        println!("Encoded: {:?}", String::from_utf8_lossy(&buf));

        let (remaining, parsed) = parse_frame(&buf).expect("parse").expect("some frame");

        assert_eq!(frame, parsed);
        assert!(
            remaining.len() == 0,
            "Remaining should be empty: {}",
            String::from_utf8_lossy(remaining)
        )
    }

    #[test]
    fn should_round_trip_trivial_frame_2() {
        use crate::parser::parse_frame;
        let frame = Frame {
            command: Command::Send,
            headers: Default::default(),
            body: vec![1],
        };
        println!("Frame: {:?}", frame);

        let mut buf = BytesMut::new();
        encode_frame(&mut buf, &frame).expect("encode frame");
        println!("Encoded: {:?}", String::from_utf8_lossy(&buf));

        let (remaining, parsed) = parse_frame(&buf).expect("parse").expect("some frame");

        assert_eq!(frame, parsed);
        assert!(
            remaining.len() == 0,
            "Remaining should be empty: {}",
            String::from_utf8_lossy(remaining)
        )
    }

    fn asciis() -> impl Generator<Item = String> {
        use suppositions::generators::*;
        collections(u8s().map(|c| std::char::from_u32(c as u32).expect("char")))
    }

    fn frames() -> impl Generator<Item = Frame> {
        use suppositions::generators::*;
        let commands = one_of(consts(Command::Send))
            .or(consts(Command::Connect))
            .or(consts(Command::Subscribe))
            .or(consts(Command::Unsubscribe))
            .or(consts(Command::Disconnect))
            .or(consts(Command::Ack))
            .or(consts(Command::Connected))
            .or(consts(Command::Message))
            .or(consts(Command::Receipt))
            .or(consts(Command::Error));

        let headers = collections((asciis(), asciis()).filter(|&(ref k, _)| k.len() != 0));

        let bodies = vecs(u8s());
        (commands, headers, bodies).map(|(command, headers, body)| Frame {
            command,
            headers,
            body,
        })
    }
}
