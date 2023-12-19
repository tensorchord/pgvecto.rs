use super::IpcError;
use crate::utils::file_socket::FileSocket;
use byteorder::{ReadBytesExt, WriteBytesExt};
use rustix::fd::AsFd;
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
    pub fn send(&mut self, packet: &[u8]) -> Result<(), IpcError> {
        use byteorder::NativeEndian as N;
        let len = u32::try_from(packet.len()).expect("Packet is too large.");
        resolve_closed!(self.stream.write_u32::<N>(len));
        resolve_closed!(self.stream.write_all(packet));
        Ok(())
    }
    pub fn recv(&mut self) -> Result<Vec<u8>, IpcError> {
        use byteorder::NativeEndian as N;
        let len = resolve_closed!(self.stream.read_u32::<N>());
        let mut packet = vec![0u8; len as usize];
        resolve_closed!(self.stream.read_exact(&mut packet));
        Ok(packet)
    }
}
