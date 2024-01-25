pub mod mmap;
pub mod unix;

use super::{ConnectionError, GraceError};
use serde::{Deserialize, Serialize};
use service::prelude::ServiceError;
use std::fmt::Debug;

pub trait Bincode: Debug {
    fn serialize(&self) -> Vec<u8>;
    fn deserialize(_: &[u8]) -> Self;
}

impl<T: Debug + Serialize + for<'a> Deserialize<'a>> Bincode for T {
    fn serialize(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }

    fn deserialize(bytes: &[u8]) -> Self {
        bincode::deserialize(bytes).unwrap()
    }
}

pub enum ServerSocket {
    Unix(unix::Socket),
    Mmap(mmap::Socket),
}

pub enum ClientSocket {
    Unix(unix::Socket),
    Mmap(mmap::Socket),
}

impl ServerSocket {
    pub fn ok<T: Bincode>(&mut self, packet: T) -> Result<(), ConnectionError> {
        let mut buffer = vec![0u8];
        buffer.extend(packet.serialize());
        match self {
            Self::Unix(x) => x.send(&buffer),
            Self::Mmap(x) => x.send(&buffer),
        }
    }
    pub fn err(&mut self, packet: ServiceError) -> Result<!, ConnectionError> {
        let mut buffer = vec![1u8];
        buffer.extend(Bincode::serialize(&packet));
        match self {
            Self::Unix(x) => x.send(&buffer)?,
            Self::Mmap(x) => x.send(&buffer)?,
        }
        Err(ConnectionError::Service(packet))
    }
    pub fn recv<T: Bincode>(&mut self) -> Result<T, ConnectionError> {
        let buffer = match self {
            Self::Unix(x) => x.recv()?,
            Self::Mmap(x) => x.recv()?,
        };
        let c = &buffer[1..];
        match buffer[0] {
            0u8 => Ok(T::deserialize(c)),
            1u8 => Err(ConnectionError::Grace(bincode::deserialize(c).unwrap())),
            _ => unreachable!(),
        }
    }
}

impl ClientSocket {
    pub fn ok<T: Bincode>(&mut self, packet: T) -> Result<(), ConnectionError> {
        let mut buffer = vec![0u8];
        buffer.extend(packet.serialize());
        match self {
            Self::Unix(x) => x.send(&buffer),
            Self::Mmap(x) => x.send(&buffer),
        }
    }
    #[allow(unused)]
    pub fn err(&mut self, packet: GraceError) -> Result<!, ConnectionError> {
        let mut buffer = vec![1u8];
        buffer.extend(Bincode::serialize(&packet));
        match self {
            Self::Unix(x) => x.send(&buffer)?,
            Self::Mmap(x) => x.send(&buffer)?,
        }
        Err(ConnectionError::Grace(packet))
    }
    pub fn recv<T: Bincode>(&mut self) -> Result<T, ConnectionError> {
        let buffer = match self {
            Self::Unix(x) => x.recv()?,
            Self::Mmap(x) => x.recv()?,
        };
        let c = &buffer[1..];
        match buffer[0] {
            0u8 => Ok(T::deserialize(c)),
            1u8 => Err(ConnectionError::Service(bincode::deserialize(c).unwrap())),
            _ => unreachable!(),
        }
    }
}
