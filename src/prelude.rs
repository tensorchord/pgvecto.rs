use crate::hnsw::Hnsw;
use serde::{Deserialize, Serialize};
use std::{alloc::Global, path::Path};

pub type Scalar = f32;

#[derive(Debug, serde::Serialize, serde::Deserialize, thiserror::Error)]
pub enum Error {
    #[error("The index is broken.")]
    IndexIsBroken,
    #[error("The index is not loaded.")]
    IndexIsUnloaded,
    #[error("Build an index with an invaild option.")]
    BuildOptionIsInvaild,
}

#[derive(
    Debug, Clone, Copy, Hash, PartialEq, Eq, serde::Serialize, serde::Deserialize, PartialOrd, Ord,
)]
pub struct Id {
    pub newtype: u32,
}

impl Id {
    pub fn from_sys(sys: pgrx::pg_sys::Oid) -> Self {
        Self {
            newtype: sys.as_u32(),
        }
    }
    #[allow(dead_code)]
    pub fn into_sys(self) -> pgrx::pg_sys::Oid {
        unsafe { pgrx::pg_sys::Oid::from_u32_unchecked(self.newtype) }
    }
    #[allow(dead_code)]
    pub fn from_u32(value: u32) -> Self {
        Self { newtype: value }
    }
    pub fn as_u32(self) -> u32 {
        self.newtype
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Pointer {
    pub newtype: u64,
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

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct Options {
    pub dims: u16,
    pub algorithm: String,
    pub options_algorithm: String,
    pub distance: Distance,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Copy)]
pub enum Distance {
    L2,
    Cosine,
    Dot,
}

impl Distance {
    pub fn distance(self, lhs: &[Scalar], rhs: &[Scalar]) -> Scalar {
        if lhs.len() != rhs.len() {
            return Scalar::NAN;
        }
        let n = lhs.len();
        match self {
            Distance::L2 => {
                let mut result = 0.0 as Scalar;
                for i in 0..n {
                    result += (lhs[i] - rhs[i]) * (lhs[i] - rhs[i]);
                }
                result
            }
            Distance::Cosine => {
                let mut dot = 0.0 as Scalar;
                let mut x2 = 0.0 as Scalar;
                let mut y2 = 0.0 as Scalar;
                for i in 0..n {
                    dot += lhs[i] * rhs[i];
                    x2 += lhs[i] * lhs[i];
                    y2 += rhs[i] * rhs[i];
                }
                1.0 - dot * dot / (x2 * y2)
            }
            Distance::Dot => {
                let mut dot = 0.0 as Scalar;
                for i in 0..n {
                    dot += lhs[i] * rhs[i];
                }
                1.0 - dot
            }
        }
    }
}

pub trait Algorithm0: Sized {
    type Allocator;
    fn build(
        options: Options,
        data: async_channel::Receiver<(Vec<Scalar>, u64)>,
        allocator: Self::Allocator,
    ) -> anyhow::Result<Self>;
    fn load(
        options: Options,
        path: impl AsRef<Path>,
        allocator: Self::Allocator,
    ) -> anyhow::Result<Self>;
}

pub trait Algorithm1 {
    fn save(&mut self, path: impl AsRef<Path>) -> anyhow::Result<()>;
    fn insert(&self, insert: (Vec<Scalar>, u64)) -> anyhow::Result<()>;
    fn search(&self, search: (Vec<Scalar>, usize)) -> anyhow::Result<Vec<(Scalar, u64)>>;
}

pub trait Algorithm: Algorithm0 + Algorithm1 {}

impl<T: Algorithm0 + Algorithm1> Algorithm for T {}

#[derive(Debug, Clone, Copy)]
pub enum Algo0 {
    Hnsw,
}

impl Algo0 {
    pub fn new(name: &str) -> anyhow::Result<Self> {
        match name {
            "HNSW" => Ok(Self::Hnsw),
            _ => anyhow::bail!(Error::BuildOptionIsInvaild),
        }
    }
    pub async fn build(
        self,
        options: Options,
        data: async_channel::Receiver<(Vec<Scalar>, u64)>,
    ) -> anyhow::Result<Algo1> {
        tokio::task::block_in_place(|| {
            Ok(match self {
                Self::Hnsw => Hnsw::build(options, data, Global)?.into(),
            })
        })
    }
    pub async fn load(self, options: Options, path: impl AsRef<Path>) -> anyhow::Result<Algo1> {
        tokio::task::block_in_place(|| {
            Ok(match self {
                Self::Hnsw => Hnsw::load(options, path, Global)?.into(),
            })
        })
    }
}

pub enum Algo1 {
    Hnsw(Hnsw<Global>),
}

impl From<Hnsw<Global>> for Algo1 {
    fn from(value: Hnsw<Global>) -> Self {
        Self::Hnsw(value)
    }
}

impl Algo1 {
    pub async fn save(&mut self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        use Algo1::*;
        tokio::task::block_in_place(|| match self {
            Hnsw(x) => x.save(path),
        })
    }
    pub async fn insert(&self, insert: (Vec<Scalar>, u64)) -> anyhow::Result<()> {
        use Algo1::*;
        tokio::task::block_in_place(|| match self {
            Hnsw(x) => x.insert(insert),
        })
    }
    pub async fn search(&self, search: (Vec<Scalar>, usize)) -> anyhow::Result<Vec<(Scalar, u64)>> {
        use Algo1::*;
        tokio::task::block_in_place(|| match self {
            Hnsw(x) => x.search(search),
        })
    }
}

pub trait BincodeDeserialize {
    fn deserialize<'a, T: Deserialize<'a>>(&'a self) -> anyhow::Result<T>;
}

impl BincodeDeserialize for [u8] {
    fn deserialize<'a, T: Deserialize<'a>>(&'a self) -> anyhow::Result<T> {
        let t = bincode::deserialize::<T>(self)?;
        Ok(t)
    }
}

pub trait BincodeSerialize {
    fn serialize(&self) -> anyhow::Result<Vec<u8>>;
}

impl<T: Serialize> BincodeSerialize for T {
    fn serialize(&self) -> anyhow::Result<Vec<u8>> {
        let bytes = bincode::serialize(self)?;
        Ok(bytes)
    }
}
