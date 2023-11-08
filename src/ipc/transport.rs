use crate::ipc::IpcError;
use byteorder::{ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize};
use std::io::ErrorKind;
use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;

macro_rules! resolve_server_closed {
    ($t: expr) => {
        match $t {
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Err(IpcError::Closed),
            Err(e) => panic!("{}", e),
            Ok(e) => e,
        }
    };
}

macro_rules! resolve_client_closed {
    ($t: expr) => {
        match $t {
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Err(IpcError::Closed),
            Err(e) => panic!("{}", e),
            Ok(e) => e,
        }
    };
}

pub(super) struct Listener {
    listener: UnixListener,
}

impl Listener {
    pub fn new() -> Self {
        let path = ".s.VECTORS";
        remove_file_if_exists(&path).expect("Failed to bind.");
        let listener = UnixListener::bind(&path).expect("Failed to bind.");
        Self { listener }
    }
    pub fn accept(&mut self) -> Socket {
        let (stream, _) = self.listener.accept().expect("Failed to listen.");
        Socket {
            stream: Some(stream),
        }
    }
}

pub(super) struct Socket {
    stream: Option<UnixStream>,
}

impl Socket {
    pub fn new() -> Self {
        let path = ".s.VECTORS";
        let stream = UnixStream::connect(path).expect("Failed to bind.");
        Socket {
            stream: Some(stream),
        }
    }
    pub fn server_send<T>(&mut self, packet: T) -> Result<(), IpcError>
    where
        T: Serialize,
    {
        use byteorder::NativeEndian as N;
        let stream = self.stream.as_mut().ok_or(IpcError::Closed)?;
        let buffer = bincode::serialize(&packet).expect("Failed to serialize");
        let len = u32::try_from(buffer.len()).expect("Packet is too large.");
        resolve_server_closed!(stream.write_u32::<N>(len));
        resolve_server_closed!(stream.write_all(&buffer));
        Ok(())
    }
    pub fn client_recv<T>(&mut self) -> Result<T, IpcError>
    where
        T: for<'a> Deserialize<'a>,
    {
        use byteorder::NativeEndian as N;
        let stream = self.stream.as_mut().ok_or(IpcError::Closed)?;
        let len = resolve_client_closed!(stream.read_u32::<N>());
        let mut buffer = vec![0u8; len as usize];
        resolve_client_closed!(stream.read_exact(&mut buffer));
        let packet = bincode::deserialize(&buffer).expect("Failed to deserialize.");
        Ok(packet)
    }
    pub fn client_send<T>(&mut self, packet: T) -> Result<(), IpcError>
    where
        T: Serialize,
    {
        use byteorder::NativeEndian as N;
        let stream = self.stream.as_mut().ok_or(IpcError::Closed)?;
        let buffer = bincode::serialize(&packet).expect("Failed to serialize");
        let len = u32::try_from(buffer.len()).expect("Packet is too large.");
        resolve_client_closed!(stream.write_u32::<N>(len));
        resolve_client_closed!(stream.write_all(&buffer));
        Ok(())
    }
    pub fn server_recv<T>(&mut self) -> Result<T, IpcError>
    where
        T: for<'a> Deserialize<'a>,
    {
        use byteorder::NativeEndian as N;
        let stream = self.stream.as_mut().ok_or(IpcError::Closed)?;
        let len = resolve_server_closed!(stream.read_u32::<N>());
        let mut buffer = vec![0u8; len as usize];
        resolve_server_closed!(stream.read_exact(&mut buffer));
        let packet = bincode::deserialize(&buffer).expect("Failed to deserialize.");
        Ok(packet)
    }
}

fn remove_file_if_exists(path: impl AsRef<Path>) -> std::io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}
