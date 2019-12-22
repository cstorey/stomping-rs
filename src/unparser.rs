use bytes::{BufMut, BytesMut};

use crate::errors::*;
use crate::{Frame, FrameOrKeepAlive};

pub(crate) fn encode_frame(buf: &mut BytesMut, item: &FrameOrKeepAlive) -> Result<()> {
    match item {
        FrameOrKeepAlive::Frame(ref frame) => encode_inner(buf, frame)?,
        FrameOrKeepAlive::KeepAlive => encode_keepalive(buf)?,
    }

    Ok(())
}

fn encode_inner(buf: &mut BytesMut, frame: &Frame) -> Result<()> {
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

fn encode_keepalive(buf: &mut BytesMut) -> Result<()> {
    buf.put_u8(b'\n');
    Ok(())
}

fn encode_header_label(buf: &mut BytesMut, label: &[u8]) {
    for c in label {
        match *c {
            b':' => buf.put_slice(b"\\c"),
            b'\r' => buf.put_slice(b"\\r"),
            b'\n' => buf.put_slice(b"\\n"),
            b'\\' => buf.put_slice(b"\\\\"),
            _ => buf.put_u8(*c),
        }
    }
}

#[cfg(test)]
mod tests {
    use maplit::*;
    use suppositions::{generators::Generator, property};

    use super::*;
    use crate::{Command, Frame, Headers};

    #[test]
    fn should_encode_trivial_example() {
        let frame = Frame {
            command: Command::Send,
            headers: Headers::new(),
            body: Vec::new(),
        };
        let mut buf = BytesMut::new();

        encode_frame(&mut buf, &FrameOrKeepAlive::Frame(frame)).expect("encode frame");

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

        encode_frame(&mut buf, &FrameOrKeepAlive::Frame(frame)).expect("encode frame");

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

        encode_frame(&mut buf, &FrameOrKeepAlive::Frame(frame)).expect("encode frame");

        assert_eq!(
            &*"SEND\nfoo\\cbar:y\n\n\0",
            std::str::from_utf8(&buf).expect("from utf8")
        );
    }

    #[test]
    fn should_encode_a_header_colon_in_value() {
        let frame = Frame {
            command: Command::Send,
            headers: btreemap! {"destination".into() => "/queue/hello:world".into()},
            body: Vec::new(),
        };
        let mut buf = BytesMut::new();

        encode_frame(&mut buf, &FrameOrKeepAlive::Frame(frame)).expect("encode frame");

        assert_eq!(
            &*"SEND\ndestination:/queue/hello\\cworld\n\n\0",
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

        encode_frame(&mut buf, &FrameOrKeepAlive::Frame(frame)).expect("encode frame");

        assert_eq!(
            &*"SEND\n\\n:y\n\n\0",
            std::str::from_utf8(&buf).expect("from utf8")
        );
    }

    #[test]
    fn should_encode_slash_in_header() {
        let frame = Frame {
            command: Command::Send,
            headers: btreemap! {"header".into() => "\\".into()},
            body: Vec::new(),
        };
        let mut buf = BytesMut::new();

        encode_frame(&mut buf, &FrameOrKeepAlive::Frame(frame)).expect("encode frame");

        assert_eq!(
            &*"SEND\nheader:\\\\\n\n\0",
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

        encode_frame(&mut buf, &FrameOrKeepAlive::Frame(frame)).expect("encode frame");

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

        let res = encode_frame(&mut buf, &FrameOrKeepAlive::Frame(frame));

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

        encode_frame(&mut buf, &FrameOrKeepAlive::Frame(frame)).expect("encode frame");

        assert_eq!(
            &*"SEND\n\nx\0",
            std::str::from_utf8(&buf).expect("from utf8")
        );
    }

    #[test]
    fn should_encode_keepalive() {
        let mut buf = BytesMut::new();

        encode_frame(&mut buf, &FrameOrKeepAlive::KeepAlive).expect("encode frame");

        assert_eq!(&*"\n", std::str::from_utf8(&buf).expect("from utf8"));
    }

    #[test]
    fn should_round_trip_frames_without_content_length() {
        use crate::parser::parse_frame;

        env_logger::try_init().unwrap_or(());
        property(frames().filter(|frame| !frame.body.contains(&b'\0'))).check(|frame| {
            let mut buf = BytesMut::new();

            encode_frame(&mut buf, &FrameOrKeepAlive::Frame(frame.clone())).expect("encode frame");
            let parsed = parse_frame(&mut buf)
                .expect("parse")
                .expect("some frame")
                .unwrap_frame();

            assert_eq!(frame, parsed);
            assert!(
                buf.len() == 0,
                "Remaining should be empty: {}",
                String::from_utf8_lossy(&buf)
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
        encode_frame(&mut buf, &FrameOrKeepAlive::Frame(frame.clone())).expect("encode frame");
        println!("Encoded: {:?}", String::from_utf8_lossy(&buf));

        let parsed = parse_frame(&mut buf)
            .expect("parse")
            .expect("some frame")
            .unwrap_frame();

        assert_eq!(frame, parsed);
        assert!(
            buf.len() == 0,
            "Remaining should be empty: {}",
            String::from_utf8_lossy(&buf)
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
        encode_frame(&mut buf, &FrameOrKeepAlive::Frame(frame.clone())).expect("encode frame");
        println!("Encoded: {:?}", String::from_utf8_lossy(&buf));

        let parsed = parse_frame(&mut buf)
            .expect("parse")
            .expect("some frame")
            .unwrap_frame();

        assert_eq!(frame, parsed);
        assert!(
            buf.len() == 0,
            "Remaining should be empty: {}",
            String::from_utf8_lossy(&buf)
        )
    }

    fn octet_vecs() -> impl Generator<Item = Vec<u8>> {
        use suppositions::generators::*;
        collections(u8s())
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

        let headers = collections((octet_vecs(), octet_vecs()).filter(|&(ref k, _)| k.len() != 0));

        let bodies = vecs(u8s());
        (commands, headers, bodies).map(|(command, headers, body)| Frame {
            command,
            headers,
            body,
        })
    }
}
