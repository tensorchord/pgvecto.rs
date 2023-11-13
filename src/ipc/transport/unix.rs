use crate::ipc::IpcError;
use crate::utils::file_socket::FileSocket;
use byteorder::{ReadBytesExt, WriteBytesExt};
use rustix::fd::AsFd;
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::sync::OnceLock;

static CHANNEL: OnceLock<FileSocket> = OnceLock::new();

pub fn init() {
    CHANNEL.set(FileSocket::new().unwrap()).ok().unwrap();
}

pub fn accept() -> Socket {
    let fd = CHANNEL.get().unwrap().recv().unwrap();
    let stream = UnixStream::from(fd);
    Socket { stream }
}

pub fn connect() -> Socket {
    let (other, stream) = UnixStream::pair().unwrap();
    CHANNEL.get().unwrap().send(other.as_fd()).unwrap();
    Socket { stream }
}

pub struct Socket {
    stream: UnixStream,
}

macro_rules! resolve_closed {
    ($t: expr) => {
        match $t {
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Err(IpcError::Closed),
            Err(e) => panic!("{}", e),
            Ok(e) => e,
        }
    };
}

impl Socket {
    pub fn send<T>(&mut self, packet: T) -> Result<(), IpcError>
    where
        T: Serialize,
    {
        use byteorder::NativeEndian as N;
        let buffer = bincode::serialize(&packet).expect("Failed to serialize");
        let len = u32::try_from(buffer.len()).expect("Packet is too large.");
        resolve_closed!(self.stream.write_u32::<N>(len));
        resolve_closed!(self.stream.write_all(&buffer));
        Ok(())
    }
    pub fn recv<T>(&mut self) -> Result<T, IpcError>
    where
        T: for<'a> Deserialize<'a>,
    {
        use byteorder::NativeEndian as N;
        let len = resolve_closed!(self.stream.read_u32::<N>());
        let mut buffer = vec![0u8; len as usize];
        resolve_closed!(self.stream.read_exact(&mut buffer));
        let packet = bincode::deserialize(&buffer).expect("Failed to deserialize.");
        Ok(packet)
    }
}
