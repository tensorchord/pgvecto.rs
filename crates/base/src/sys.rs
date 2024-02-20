use serde::{Deserialize, Serialize};
use std::{fmt::Display, num::ParseIntError, str::FromStr};

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Handle {
    pub newtype: u32,
}

impl Handle {
    pub fn as_u32(self) -> u32 {
        self.newtype
    }
}

impl Display for Handle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_u32())
    }
}

impl FromStr for Handle {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Handle {
            newtype: u32::from_str(s)?,
        })
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pointer {
    pub newtype: u64,
}

impl Pointer {
    pub fn from_u48(value: u64) -> Self {
        assert!(value < (1u64 << 48));
        Self { newtype: value }
    }
    pub fn as_u48(self) -> u64 {
        self.newtype
    }
}
