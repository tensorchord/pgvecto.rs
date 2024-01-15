pub mod mmap;
pub mod unix;

use super::IpcError;
use crate::prelude::*;
use serde::{Deserialize, Serialize};
use service::prelude::ServiceError;

pub enum ServerSocket {
    Unix(unix::Socket),
    Mmap(mmap::Socket),
}

pub enum ClientSocket {
    Unix { socket: unix::Socket },
    Mmap { socket: mmap::Socket },
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
    pub fn err(&mut self, err: ServiceError) -> Result<!, IpcError> {
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
            Self::Unix { socket } => socket.send(&buffer),
            Self::Mmap { socket } => socket.send(&buffer),
        }
    }
    pub fn recv<T: for<'a> Deserialize<'a>>(&mut self) -> Result<T, Box<dyn FriendlyError>> {
        let buffer = match self {
            Self::Unix { socket } => socket
                .recv()
                .map_err(|e| Box::new(e) as Box<dyn FriendlyError>)?,
            Self::Mmap { socket } => socket
                .recv()
                .map_err(|e| Box::new(e) as Box<dyn FriendlyError>)?,
        };
        match buffer[0] {
            0u8 => Ok(bincode::deserialize::<T>(&buffer[1..]).expect("Failed to deserialize.")),
            1u8 => Err(Box::new(
                bincode::deserialize::<ServiceError>(&buffer[1..]).expect("Failed to deserialize."),
            )),
            _ => unreachable!(),
        }
    }
}
