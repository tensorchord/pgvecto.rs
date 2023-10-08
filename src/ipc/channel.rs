use serde::{Deserialize, Serialize};
use std::error::Error;

pub type Channel = Box<dyn ChannelTrait>;

pub trait ChannelTrait: Send + Sync {
    fn write(&mut self, buf: &Vec<u8>) -> Result<(), Box<dyn Error>>;
    fn read(&mut self) -> Result<Vec<u8>, Box<dyn Error>>;
}

pub trait ChannelWithSerialize {
    fn send<T>(&mut self, packet: T) -> Result<(), Box<dyn Error>>
    where
        T: Serialize;
    fn recv<T>(&mut self) -> Result<T, Box<dyn Error>>
    where
        T: for<'a> Deserialize<'a>;
}

impl ChannelWithSerialize for dyn ChannelTrait {
    fn send<T>(&mut self, packet: T) -> Result<(), Box<dyn Error>>
    where
        T: Serialize,
    {
        let buffer = bincode::serialize(&packet)?;
        self.write(&buffer)?;
        Ok(())
    }

    fn recv<T>(&mut self) -> Result<T, Box<dyn Error>>
    where
        T: for<'a> Deserialize<'a>,
    {
        let buffer = self.read()?;
        let packet = bincode::deserialize(&buffer)?;
        Ok(packet)
    }
}
