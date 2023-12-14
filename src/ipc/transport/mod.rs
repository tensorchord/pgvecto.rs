pub mod mmap;
pub mod unix;

use super::IpcError;
use serde::{Deserialize, Serialize};

pub enum Socket {
    Unix(unix::Socket),
    Mmap(mmap::Socket),
}

impl Socket {
    pub fn client_send<T: Serialize>(&mut self, packet: T) -> Result<(), IpcError> {
        match self {
            Socket::Unix(x) => x.send(packet),
            Socket::Mmap(x) => x.send(packet),
        }
    }
    pub fn client_recv<T: for<'a> Deserialize<'a>>(&mut self) -> Result<T, IpcError> {
        match self {
            Socket::Unix(x) => x.recv(),
            Socket::Mmap(x) => x.recv(),
        }
    }
    pub fn server_send<T: Serialize>(&mut self, packet: T) -> Result<(), IpcError> {
        match self {
            Socket::Unix(x) => x.send(packet),
            Socket::Mmap(x) => x.send(packet),
        }
    }
    pub fn server_recv<T: for<'a> Deserialize<'a>>(&mut self) -> Result<T, IpcError> {
        match self {
            Socket::Unix(x) => x.recv(),
            Socket::Mmap(x) => x.recv(),
        }
    }
}
