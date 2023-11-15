#[cfg(target_os = "linux")]
pub mod mmap;
pub mod unix;

use super::IpcError;
use serde::{Deserialize, Serialize};

pub enum Socket {
    Unix(unix::Socket),
    #[cfg(target_os = "linux")]
    Mmap(mmap::Socket),
}

impl Socket {
    pub fn send<T: Serialize>(&mut self, packet: T) -> Result<(), IpcError> {
        match self {
            Socket::Unix(x) => x.send(packet),
            #[cfg(target_os = "linux")]
            Socket::Mmap(x) => x.send(packet),
        }
    }
    pub fn recv<T: for<'a> Deserialize<'a>>(&mut self) -> Result<T, IpcError> {
        match self {
            Socket::Unix(x) => x.recv(),
            #[cfg(target_os = "linux")]
            Socket::Mmap(x) => x.recv(),
        }
    }
}
