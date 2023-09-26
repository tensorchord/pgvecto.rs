use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Id {
    newtype: u32,
}

impl Id {
    pub fn from_sys(sys: pgrx::pg_sys::Oid) -> Self {
        Self {
            newtype: sys.as_u32(),
        }
    }
    pub fn as_u32(self) -> u32 {
        self.newtype
    }
}

impl Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_u32())
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pointer {
    newtype: u64,
}

impl Pointer {
    pub fn from_sys(sys: pgrx::pg_sys::ItemPointerData) -> Self {
        let mut newtype = 0;
        newtype |= (sys.ip_blkid.bi_hi as u64) << 32;
        newtype |= (sys.ip_blkid.bi_lo as u64) << 16;
        newtype |= (sys.ip_posid as u64) << 0;
        Self { newtype }
    }
    pub fn into_sys(self) -> pgrx::pg_sys::ItemPointerData {
        pgrx::pg_sys::ItemPointerData {
            ip_blkid: pgrx::pg_sys::BlockIdData {
                bi_hi: ((self.newtype >> 32) & 0xffff) as u16,
                bi_lo: ((self.newtype >> 16) & 0xffff) as u16,
            },
            ip_posid: ((self.newtype >> 0) & 0xffff) as u16,
        }
    }
    pub fn from_u48(value: u64) -> Self {
        assert!(value < (1u64 << 48));
        Self { newtype: value }
    }
    pub fn as_u48(self) -> u64 {
        self.newtype
    }
}
