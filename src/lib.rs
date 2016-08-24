#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate log;
use std::collections::BTreeMap;
use std::net::{TcpStream, ToSocketAddrs};
use std::io::{BufWriter, BufReader, BufRead, Write, Read};

mod errors;
use errors::*;

pub struct Client {
    wr: BufWriter<TcpStream>,
    rdr: BufReader<TcpStream>,
}

pub enum AckMode {
    Auto,
    ClientIndividual,
}

impl AckMode {
    fn as_str(&self) -> &'static str {
        match self {
            &AckMode::Auto => "auto",
            &AckMode::ClientIndividual => "client-individual",
        }
    }
}

pub type Headers = BTreeMap<String, String>;

impl Client {
    pub fn connect<A: ToSocketAddrs>(a: A, credentials: Option<(&str, &str)>) -> Result<Self> {
        let wr = try!(TcpStream::connect(a));
        debug!("connected to: {:?}", try!(wr.peer_addr()));
        let rdr = try!(wr.try_clone());
        let mut conn_headers = BTreeMap::new();
        let mut client = Client {
            wr: BufWriter::new(wr),
            rdr: BufReader::new(rdr),
        };
        conn_headers.insert("accept-version".to_string(), "1.2".to_string());
        if let Some((user, pass)) = credentials {
            conn_headers.insert("login".to_string(), user.to_string());
            conn_headers.insert("passcode".to_string(), pass.to_string());
        }
        try!(client.send("CONNECT", conn_headers, &[]));

        let (cmd, hdrs, body) = try!(client.read_frame());
        if &cmd == "ERROR" {
            let body = String::from_utf8_lossy(&body).into_owned();
            warn!("Error response from server: {:?}: {:?}", cmd, hdrs);
            return Err(ErrorKind::StompError(cmd, hdrs, body).into());
        } else if &cmd != "CONNECTED" {
            warn!("Bad response from server: {:?}: {:?}", cmd, hdrs);
            return Err(ErrorKind::ProtocolError.into());
        }

        Ok(client)
    }

    pub fn subscribe(&mut self, destination: &str, id: &str, mode: AckMode) -> Result<()> {
        let mut h = BTreeMap::new();
        h.insert("destination".to_string(), destination.to_string());
        h.insert("id".to_string(), id.to_string());
        h.insert("ack".to_string(), mode.as_str().to_string());
        try!(self.send("SUBSCRIBE", h, b""));
        Ok(())
    }
    pub fn publish(&mut self, destination: &str, body: &[u8]) -> Result<()> {
        let mut h = BTreeMap::new();
        h.insert("destination".to_string(), destination.to_string());
        h.insert("content-length".to_string(), format!("{}", body.len()));
        try!(self.send("SEND", h, body));
        Ok(())
    }
    pub fn ack(&mut self, headers: &Headers) -> Result<()> {
        let mut h = BTreeMap::new();
        let mid = try!(headers.get("ack").ok_or(ErrorKind::NoAckHeader));
        // h.insert("content-length".to_string(), format!("{}", body.len()));
        try!(self.send("ACK", h, &[]));
        Ok(())
    }


    pub fn consume_next(&mut self) -> Result<(Headers, Vec<u8>)> {
        let (cmd, hdrs, body) = try!(self.read_frame());
        if &cmd != "MESSAGE" {
            warn!("Bad message from server: {:?}: {:?}", cmd, hdrs);
            return Err(ErrorKind::ProtocolError.into());
        }

        Ok((hdrs, body))
    }

    fn send(&mut self,
            command: &str,
            headers: BTreeMap<String, String>,
            body: &[u8])
            -> Result<()> {

        try!(writeln!(self.wr, "{}", command));
        for (k, v) in headers {
            try!(writeln!(self.wr, "{}:{}", k, v));
        }
        try!(writeln!(self.wr, ""));

        try!(self.wr.write_all(body));
        try!(self.wr.write(b"\0"));
        try!(self.wr.flush());
        Ok(())
    }
    fn read_frame(&mut self) -> Result<(String, Headers, Vec<u8>)> {
        let mut buf = String::new();
        while buf.trim().is_empty() {
            buf.clear();
            try!(self.rdr.read_line(&mut buf));
            trace!("Read command line: {:?}", buf);
        }
        let command = buf.trim().to_string();

        let mut headers = BTreeMap::new();
        let mut line = String::new();
        loop {
            buf.clear();
            try!(self.rdr.read_line(&mut buf));
            trace!("Read header line: {:?}", buf);
            if buf == "\n" {
                break;
            }
            let mut it = buf.trim().splitn(2, ':');
            let name = try!(it.next().ok_or(ErrorKind::ProtocolError));
            let value = try!(it.next().ok_or(ErrorKind::ProtocolError));
            headers.insert(name.to_string(), value.to_string());
        }
        trace!("Reading body");
        let mut buf = Vec::new();
        if let Some(lenstr) = headers.get("content-length") {
            let nbytes: u64 = try!(lenstr.parse());
            trace!("Read bytes: {}", nbytes);
            try!(self.rdr.by_ref().take(nbytes + 1).read_to_end(&mut buf));
        } else {
            trace!("Read until nul");
            try!(self.rdr.read_until(b'\0', &mut buf));
        }
        trace!("Read body: {:?}", buf);
        if buf.pop() != Some(b'\0') {
            warn!("No null at end of body");
            return Err(ErrorKind::ProtocolError.into());
        }

        let frame = (command, headers, buf);
        trace!("read frame: {:?}", frame);
        Ok(frame)
    }
}
