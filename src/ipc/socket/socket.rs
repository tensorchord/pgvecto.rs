use byteorder::{ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize};
use std::{
    error::Error,
    io::{Read, Write},
    os::unix::net::UnixStream,
};
use thiserror::Error;

macro_rules! resolve_closed {
    ($t: expr) => {
        match $t {
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Err(SocketError::Closed.into())
            }
            Err(e) => panic!("{}", e),
            Ok(e) => e,
        }
    };
}

pub struct Socket {
    pub(super) stream: Option<UnixStream>,
}

impl Socket {
    pub fn new() -> Self {
        let path = "./pg_vectors/_socket";
        let stream = UnixStream::connect(path).expect("Failed to bind.");
        Socket {
            stream: Some(stream),
        }
    }
}

impl crate::ipc::ChannelTrait for Socket {
    fn write(&mut self, buf: &Vec<u8>) -> Result<(), Box<dyn Error>> {
        let stream = self.stream.as_mut().ok_or(SocketError::Closed)?;
        resolve_closed!(stream.write_u32::<byteorder::NativeEndian>(buf.len() as u32));
        resolve_closed!(stream.write_all(buf));
        Ok(())
    }

    fn read(&mut self) -> Result<Vec<u8>, Box<dyn Error>> {
        let stream = self.stream.as_mut().ok_or(SocketError::Closed)?;
        let len = resolve_closed!(stream.read_u32::<byteorder::NativeEndian>());
        let mut buffer = vec![0u8; len as usize];
        resolve_closed!(stream.read_exact(&mut buffer));
        Ok(buffer)
    }
}

#[derive(Debug, Clone, Error, Serialize, Deserialize)]
pub enum SocketError {
    #[error("The connection is closed.")]
    Closed,
    #[error("Server encounters an error.")]
    Server,
}
