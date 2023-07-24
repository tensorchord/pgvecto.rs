use serde::{Deserialize, Serialize};

pub trait BincodeDeserialize {
    fn deserialize<'a, T: Deserialize<'a>>(&'a self) -> anyhow::Result<T>;
}

impl BincodeDeserialize for [u8] {
    fn deserialize<'a, T: Deserialize<'a>>(&'a self) -> anyhow::Result<T> {
        let t = bincode::deserialize::<T>(self)?;
        Ok(t)
    }
}

pub trait Bincode: Sized {
    fn bincode(&self) -> anyhow::Result<Vec<u8>>;
}

impl<T: Serialize> Bincode for T {
    fn bincode(&self) -> anyhow::Result<Vec<u8>> {
        let bytes = bincode::serialize(self)?;
        Ok(bytes)
    }
}
