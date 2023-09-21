mod flat;
mod hnsw;
mod impls;
mod ivf;
mod utils;

pub use flat::{Flat, FlatOptions};
pub use hnsw::{Hnsw, HnswOptions};
pub use ivf::{Ivf, IvfOptions};

use self::flat::FlatError;
use self::hnsw::HnswError;
use self::ivf::IvfError;
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlgorithmOptions {
    Flat(FlatOptions),
    Hnsw(HnswOptions),
    Ivf(IvfOptions),
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
}

pub enum Algorithm {
    FlatL2(Flat<L2>),
    FlatCosine(Flat<Cosine>),
    FlatDot(Flat<Dot>),
    HnswL2(Hnsw<L2>),
    HnswCosine(Hnsw<Cosine>),
    HnswDot(Hnsw<Dot>),
    IvfL2(Ivf<L2>),
    IvfCosine(Ivf<Cosine>),
    IvfDot(Ivf<Dot>),
}

impl Algorithm {
    pub fn prebuild(
        storage: &mut StoragePreallocator,
        options: IndexOptions,
    ) -> Result<(), AlgorithmError> {
        use AlgorithmOptions as O;
        match (options.algorithm.clone(), options.distance) {
            (O::Flat(_), Distance::L2) => Ok(Flat::<L2>::prebuild(storage, options)?),
            (O::Flat(_), Distance::Cosine) => Ok(Flat::<Cosine>::prebuild(storage, options)?),
            (O::Flat(_), Distance::Dot) => Ok(Flat::<Dot>::prebuild(storage, options)?),
            (O::Hnsw(_), Distance::L2) => Ok(Hnsw::<L2>::prebuild(storage, options)?),
            (O::Hnsw(_), Distance::Cosine) => Ok(Hnsw::<Cosine>::prebuild(storage, options)?),
            (O::Hnsw(_), Distance::Dot) => Ok(Hnsw::<Dot>::prebuild(storage, options)?),
            (O::Ivf(_), Distance::L2) => Ok(Ivf::<L2>::prebuild(storage, options)?),
            (O::Ivf(_), Distance::Cosine) => Ok(Ivf::<Cosine>::prebuild(storage, options)?),
            (O::Ivf(_), Distance::Dot) => Ok(Ivf::<Dot>::prebuild(storage, options)?),
        }
    }
    pub fn build(
        storage: &mut Storage,
        options: IndexOptions,
        vectors: Arc<Vectors>,
        n: usize,
    ) -> Result<Self, AlgorithmError> {
        use AlgorithmOptions as O;
        match (options.algorithm.clone(), options.distance) {
            (O::Flat(_), Distance::L2) => {
                Ok(Flat::build(storage, options, vectors, n).map(Self::FlatL2)?)
            }
            (O::Flat(_), Distance::Cosine) => {
                Ok(Flat::build(storage, options, vectors, n).map(Self::FlatCosine)?)
            }
            (O::Flat(_), Distance::Dot) => {
                Ok(Flat::build(storage, options, vectors, n).map(Self::FlatDot)?)
            }
            (O::Hnsw(_), Distance::L2) => {
                Ok(Hnsw::build(storage, options, vectors, n).map(Self::HnswL2)?)
            }
            (O::Hnsw(_), Distance::Cosine) => {
                Ok(Hnsw::build(storage, options, vectors, n).map(Self::HnswCosine)?)
            }
            (O::Hnsw(_), Distance::Dot) => {
                Ok(Hnsw::build(storage, options, vectors, n).map(Self::HnswDot)?)
            }
            (O::Ivf(_), Distance::L2) => {
                Ok(Ivf::build(storage, options, vectors, n).map(Self::IvfL2)?)
            }
            (O::Ivf(_), Distance::Cosine) => {
                Ok(Ivf::build(storage, options, vectors, n).map(Self::IvfCosine)?)
            }
            (O::Ivf(_), Distance::Dot) => {
                Ok(Ivf::build(storage, options, vectors, n).map(Self::IvfDot)?)
            }
        }
    }
    pub fn load(
        storage: &mut Storage,
        options: IndexOptions,
        vectors: Arc<Vectors>,
    ) -> Result<Self, AlgorithmError> {
        use AlgorithmOptions as O;
        match (options.algorithm.clone(), options.distance) {
            (O::Flat(_), Distance::L2) => {
                Ok(Flat::load(storage, options, vectors).map(Self::FlatL2)?)
            }
            (O::Flat(_), Distance::Cosine) => {
                Ok(Flat::load(storage, options, vectors).map(Self::FlatCosine)?)
            }
            (O::Flat(_), Distance::Dot) => {
                Ok(Flat::load(storage, options, vectors).map(Self::FlatDot)?)
            }
            (O::Hnsw(_), Distance::L2) => {
                Ok(Hnsw::load(storage, options, vectors).map(Self::HnswL2)?)
            }
            (O::Hnsw(_), Distance::Cosine) => {
                Ok(Hnsw::load(storage, options, vectors).map(Self::HnswCosine)?)
            }
            (O::Hnsw(_), Distance::Dot) => {
                Ok(Hnsw::load(storage, options, vectors).map(Self::HnswDot)?)
            }
            (O::Ivf(_), Distance::L2) => Ok(Ivf::load(storage, options, vectors).map(Self::IvfL2)?),
            (O::Ivf(_), Distance::Cosine) => {
                Ok(Ivf::load(storage, options, vectors).map(Self::IvfCosine)?)
            }
            (O::Ivf(_), Distance::Dot) => {
                Ok(Ivf::load(storage, options, vectors).map(Self::IvfDot)?)
            }
        }
    }
    pub fn insert(&self, insert: usize) -> Result<(), AlgorithmError> {
        use Algorithm::*;
        match self {
            FlatL2(sel) => Ok(sel.insert(insert)?),
            FlatCosine(sel) => Ok(sel.insert(insert)?),
            FlatDot(sel) => Ok(sel.insert(insert)?),
            HnswL2(sel) => Ok(sel.insert(insert)?),
            HnswCosine(sel) => Ok(sel.insert(insert)?),
            HnswDot(sel) => Ok(sel.insert(insert)?),
            IvfL2(sel) => Ok(sel.insert(insert)?),
            IvfCosine(sel) => Ok(sel.insert(insert)?),
            IvfDot(sel) => Ok(sel.insert(insert)?),
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
            FlatL2(sel) => Ok(sel.search(target, k, filter)?),
            FlatCosine(sel) => Ok(sel.search(target, k, filter)?),
            FlatDot(sel) => Ok(sel.search(target, k, filter)?),
            HnswL2(sel) => Ok(sel.search(target, k, filter)?),
            HnswCosine(sel) => Ok(sel.search(target, k, filter)?),
            HnswDot(sel) => Ok(sel.search(target, k, filter)?),
            IvfL2(sel) => Ok(sel.search(target, k, filter)?),
            IvfCosine(sel) => Ok(sel.search(target, k, filter)?),
            IvfDot(sel) => Ok(sel.search(target, k, filter)?),
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
