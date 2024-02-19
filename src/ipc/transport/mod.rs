pub mod mmap;
pub mod unix;

use super::ConnectionError;
use serde::{Deserialize, Serialize};

pub trait Packet: Sized {
    fn serialize(&self) -> Option<Vec<u8>>;
    fn deserialize(_: &[u8]) -> Option<Self>;
}

impl<T: Serialize + for<'a> Deserialize<'a>> Packet for T {
    fn serialize(&self) -> Option<Vec<u8>> {
        bincode::serialize(self).ok()
    }

    fn deserialize(bytes: &[u8]) -> Option<Self> {
        bincode::deserialize(bytes).ok()
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
    pub fn ok<T: Packet>(&mut self, packet: T) -> Result<(), ConnectionError> {
        let buffer = packet
            .serialize()
            .ok_or(ConnectionError::BadSerialization)?;
        match self {
            Self::Unix(x) => x.send(&buffer),
            Self::Mmap(x) => x.send(&buffer),
        }
    }
    pub fn recv<T: Packet>(&mut self) -> Result<T, ConnectionError> {
        let buffer = match self {
            Self::Unix(x) => x.recv()?,
            Self::Mmap(x) => x.recv()?,
        };
        T::deserialize(&buffer).ok_or(ConnectionError::BadDeserialization)
    }
}

impl ClientSocket {
    pub fn ok<T: Packet>(&mut self, packet: T) -> Result<(), ConnectionError> {
        let buffer = packet
            .serialize()
            .ok_or(ConnectionError::BadSerialization)?;
        match self {
            Self::Unix(x) => x.send(&buffer),
            Self::Mmap(x) => x.send(&buffer),
        }
    }
    pub fn recv<T: Packet>(&mut self) -> Result<T, ConnectionError> {
        let buffer = match self {
            Self::Unix(x) => x.recv()?,
            Self::Mmap(x) => x.recv()?,
        };
        T::deserialize(&buffer).ok_or(ConnectionError::BadDeserialization)
    }
}
