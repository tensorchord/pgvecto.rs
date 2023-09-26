mod flat;
mod hnsw;
mod impls;
mod ivf;
mod quantization;
mod utils;
mod vamana;

pub use flat::{Flat, FlatOptions};
pub use hnsw::{Hnsw, HnswOptions};
pub use ivf::{Ivf, IvfOptions};
pub use vamana::{Vamana, VamanaOptions};

use self::flat::FlatError;
use self::hnsw::HnswError;
use self::ivf::IvfError;
use self::vamana::VamanaError;
use crate::bgworker::index::IndexOptions;
use crate::bgworker::storage::{Storage, StoragePreallocator};
use crate::bgworker::vectors::Vectors;
use crate::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize, Error)]
pub enum AlgorithmError {
    #[error("Flat {0}")]
    Flat(#[from] FlatError),
    #[error("HNSW {0}")]
    Hnsw(#[from] HnswError),
    #[error("Ivf {0}")]
    Ivf(#[from] IvfError),
    #[error("Vamana {0}")]
    Vamana(#[from] VamanaError),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlgorithmOptions {
    Flat(FlatOptions),
    Hnsw(HnswOptions),
    Ivf(IvfOptions),
    Vamana(VamanaOptions),
}

impl Default for AlgorithmOptions {
    fn default() -> Self {
        Self::Hnsw(Default::default())
    }
}

impl AlgorithmOptions {
    pub fn unwrap_flat(self) -> FlatOptions {
        use AlgorithmOptions::*;
        match self {
            Flat(x) => x,
            _ => unreachable!(),
        }
    }
    pub fn unwrap_hnsw(self) -> HnswOptions {
        use AlgorithmOptions::*;
        match self {
            Hnsw(x) => x,
            _ => unreachable!(),
        }
    }
    pub fn unwrap_ivf(self) -> IvfOptions {
        use AlgorithmOptions::*;
        match self {
            Ivf(x) => x,
            _ => unreachable!(),
        }
    }
    pub fn unwrap_vamana(self) -> VamanaOptions {
        use AlgorithmOptions::*;
        match self {
            Vamana(x) => x,
            _ => unreachable!(),
        }
    }
}

pub enum Algorithm {
    Flat(Flat),
    Hnsw(Hnsw),
    Ivf(Ivf),
    Vamana(Vamana),
}

impl Algorithm {
    pub fn prebuild(
        storage: &mut StoragePreallocator,
        options: IndexOptions,
    ) -> Result<(), AlgorithmError> {
        use AlgorithmOptions as O;
        match &options.algorithm {
            O::Flat(_) => Ok(Flat::prebuild(storage, options)?),
            O::Hnsw(_) => Ok(Hnsw::prebuild(storage, options)?),
            O::Ivf(_) => Ok(Ivf::prebuild(storage, options)?),
            O::Vamana(_) => Ok(Vamana::prebuild(storage, options)?),
        }
    }
    pub fn build(
        storage: &mut Storage,
        options: IndexOptions,
        vectors: Arc<Vectors>,
        n: usize,
    ) -> Result<Self, AlgorithmError> {
        use AlgorithmOptions as O;
        match &options.algorithm {
            O::Flat(_) => Ok(Flat::build(storage, options, vectors, n).map(Self::Flat)?),
            O::Hnsw(_) => Ok(Hnsw::build(storage, options, vectors, n).map(Self::Hnsw)?),
            O::Ivf(_) => Ok(Ivf::build(storage, options, vectors, n).map(Self::Ivf)?),
            O::Vamana(_) => Ok(Vamana::build(storage, options, vectors, n).map(Self::Vamana)?),
        }
    }
    pub fn load(
        storage: &mut Storage,
        options: IndexOptions,
        vectors: Arc<Vectors>,
    ) -> Result<Self, AlgorithmError> {
        use AlgorithmOptions as O;
        match &options.algorithm {
            O::Flat(_) => Ok(Flat::load(storage, options, vectors).map(Self::Flat)?),
            O::Hnsw(_) => Ok(Hnsw::load(storage, options, vectors).map(Self::Hnsw)?),
            O::Ivf(_) => Ok(Ivf::load(storage, options, vectors).map(Self::Ivf)?),
            O::Vamana(_) => Ok(Vamana::load(storage, options, vectors).map(Self::Vamana)?),
        }
    }
    pub fn insert(&self, insert: usize) -> Result<(), AlgorithmError> {
        use Algorithm::*;
        match self {
            Flat(sel) => Ok(sel.insert(insert)?),
            Hnsw(sel) => Ok(sel.insert(insert)?),
            Ivf(sel) => Ok(sel.insert(insert)?),
            Vamana(sel) => Ok(sel.insert(insert)?),
        }
    }
    pub fn search<F>(
        &self,
        target: Box<[Scalar]>,
        k: usize,
        filter: F,
    ) -> Result<Vec<(Scalar, u64)>, AlgorithmError>
    where
        F: FnMut(u64) -> bool,
    {
        use Algorithm::*;
        match self {
            Flat(sel) => Ok(sel.search(target, k, filter)?),
            Hnsw(sel) => Ok(sel.search(target, k, filter)?),
            Ivf(sel) => Ok(sel.search(target, k, filter)?),
            Vamana(sel) => Ok(sel.search(target, k, filter)?),
        }
    }
}

pub trait Algo: Sized {
    type Error: std::error::Error + serde::Serialize + for<'a> serde::Deserialize<'a>;
    fn prebuild(
        storage: &mut StoragePreallocator,
        options: IndexOptions,
    ) -> Result<(), Self::Error>;
    fn build(
        storage: &mut Storage,
        options: IndexOptions,
        vectors: Arc<Vectors>,
        n: usize,
    ) -> Result<Self, Self::Error>;
    fn load(
        storage: &mut Storage,
        options: IndexOptions,
        vectors: Arc<Vectors>,
    ) -> Result<Self, Self::Error>;
    fn insert(&self, i: usize) -> Result<(), Self::Error>;
    fn search<F>(
        &self,
        target: Box<[Scalar]>,
        k: usize,
        filter: F,
    ) -> Result<Vec<(Scalar, u64)>, Self::Error>
    where
        F: FnMut(u64) -> bool;
}
