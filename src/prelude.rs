use crate::algorithms::Flat;
use crate::algorithms::Hnsw;
use crate::algorithms::Ivf;
use crate::algorithms::Vectors;
use crate::memory::Address;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use validator::Validate;

pub use crate::utils::scalar::Float;
pub use crate::utils::scalar::Scalar;

pub use crate::utils::bincode::Bincode;
pub use crate::utils::bincode::BincodeDeserialize;

pub use crate::utils::distance::Distance;

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

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum Storage {
    Ram = 0,
    Disk = 1,
}

pub trait Algorithm: Sized {
    type Options: Clone + Serialize + for<'a> Deserialize<'a>;
    fn build(options: Options, vectors: Arc<Vectors>, n: usize) -> anyhow::Result<Self>;
    fn address(&self) -> Address;
    fn load(options: Options, vectors: Arc<Vectors>, address: Address) -> anyhow::Result<Self>;
    fn insert(&self, i: usize) -> anyhow::Result<()>;
    fn search(&self, search: (Box<[Scalar]>, usize)) -> anyhow::Result<Vec<(Scalar, u64)>>;
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct Options {
    pub dims: u16,
    pub distance: Distance,
    pub capacity: usize,
    #[validate(range(min = 16384))]
    pub size_ram: usize,
    #[validate(range(min = 16384))]
    pub size_disk: usize,
    pub storage_vectors: Storage,
    pub algorithm: AlgorithmOptions,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlgorithmOptions {
    Hnsw(<Hnsw as Algorithm>::Options),
    Flat(<Flat as Algorithm>::Options),
    Ivf(<Ivf as Algorithm>::Options),
}

impl AlgorithmOptions {
    pub fn unwrap_hnsw(self) -> <Hnsw as Algorithm>::Options {
        use AlgorithmOptions::*;
        match self {
            Hnsw(x) => x,
            _ => unreachable!(),
        }
    }
    #[allow(dead_code)]
    pub fn unwrap_flat(self) -> <Flat as Algorithm>::Options {
        use AlgorithmOptions::*;
        match self {
            Flat(x) => x,
            _ => unreachable!(),
        }
    }
    pub fn unwrap_ivf(self) -> <Ivf as Algorithm>::Options {
        use AlgorithmOptions::*;
        match self {
            Ivf(x) => x,
            _ => unreachable!(),
        }
    }
}
