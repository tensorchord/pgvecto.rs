pub mod mmap;
pub mod unix;

use super::IpcError;
use serde::{Deserialize, Serialize};
use service::prelude::FriendlyError;

pub enum ServerSocket {
    Unix(unix::Socket),
    Mmap(mmap::Socket),
}

pub enum ClientSocket {
    Unix(unix::Socket),
    Mmap(mmap::Socket),
}

impl ServerSocket {
    pub fn ok<T: Serialize>(&mut self, packet: T) -> Result<(), IpcError> {
        let mut buffer = Vec::new();
        buffer.push(0u8);
        buffer.extend(bincode::serialize(&packet).expect("Failed to serialize"));
        match self {
            Self::Unix(x) => x.send(&buffer),
            Self::Mmap(x) => x.send(&buffer),
        }
    }
    pub fn err(&mut self, err: FriendlyError) -> Result<!, IpcError> {
        let mut buffer = Vec::new();
        buffer.push(1u8);
        buffer.extend(bincode::serialize(&err).expect("Failed to serialize"));
        match self {
            Self::Unix(x) => x.send(&buffer)?,
            Self::Mmap(x) => x.send(&buffer)?,
        }
        Err(IpcError::Closed)
    }
    pub fn recv<T: for<'a> Deserialize<'a>>(&mut self) -> Result<T, IpcError> {
        let buffer = match self {
            Self::Unix(x) => x.recv()?,
            Self::Mmap(x) => x.recv()?,
        };
        Ok(bincode::deserialize(&buffer).expect("Failed to deserialize."))
    }
}

impl ClientSocket {
    pub fn send<T: Serialize>(&mut self, packet: T) -> Result<(), IpcError> {
        let buffer = bincode::serialize(&packet).expect("Failed to serialize");
        match self {
            Self::Unix(x) => x.send(&buffer),
            Self::Mmap(x) => x.send(&buffer),
        }
    }
    pub fn recv<T: for<'a> Deserialize<'a>>(&mut self) -> Result<T, FriendlyError> {
        let buffer = match self {
            Self::Unix(x) => x.recv().map_err(|_| FriendlyError::Ipc)?,
            Self::Mmap(x) => x.recv().map_err(|_| FriendlyError::Ipc)?,
        };
        match buffer[0] {
            0u8 => Ok(bincode::deserialize(&buffer[1..]).expect("Failed to deserialize.")),
            1u8 => Err(bincode::deserialize(&buffer[1..]).expect("Failed to deserialize.")),
            _ => unreachable!(),
        }
    }
}
