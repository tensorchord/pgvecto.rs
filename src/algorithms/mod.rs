mod flat;
mod flat_q;
mod hnsw;
mod impls;
mod ivf;
mod utils;

pub use flat::{Flat, FlatOptions};
pub use flat_q::{FlatQ, FlatQOptions};
pub use hnsw::{Hnsw, HnswOptions};
pub use ivf::{Ivf, IvfOptions};

use self::flat::FlatError;
use self::flat_q::FlatQError;
use self::hnsw::HnswError;
use self::impls::quantization::ProductQuantization;
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
    #[error("HNSW {0}")]
    Hnsw(#[from] HnswError),
    #[error("Flat {0}")]
    Flat(#[from] FlatError),
    #[error("FlatQ {0}")]
    FlatQ(#[from] FlatQError),
    #[error("Ivf {0}")]
    Ivf(#[from] IvfError),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlgorithmOptions {
    Hnsw(HnswOptions),
    Ivf(IvfOptions),
    Flat(FlatOptions),
    FlatQ(FlatQOptions),
}

impl AlgorithmOptions {
    pub fn unwrap_hnsw(self) -> HnswOptions {
        use AlgorithmOptions::*;
        match self {
            Hnsw(x) => x,
            _ => unreachable!(),
        }
    }
    #[allow(dead_code)]
    pub fn unwrap_flat(self) -> FlatOptions {
        use AlgorithmOptions::*;
        match self {
            Flat(x) => x,
            _ => unreachable!(),
        }
    }
    pub fn unwrap_flat_q(self) -> FlatQOptions {
        use AlgorithmOptions::*;
        match self {
            FlatQ(x) => x,
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
    HnswL2(Hnsw<L2>),
    HnswCosine(Hnsw<Cosine>),
    HnswDot(Hnsw<Dot>),
    FlatL2(Flat<L2>),
    FlatCosine(Flat<Cosine>),
    FlatDot(Flat<Dot>),
    FlatPqL2(FlatQ<L2, ProductQuantization<L2>>),
    FlatPqCosine(FlatQ<Cosine, ProductQuantization<Cosine>>),
    FlatPqDot(FlatQ<Dot, ProductQuantization<Dot>>),
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
            (O::Hnsw(_), Distance::L2) => Ok(Hnsw::<L2>::prebuild(storage, options)?),
            (O::Hnsw(_), Distance::Cosine) => Ok(Hnsw::<Cosine>::prebuild(storage, options)?),
            (O::Hnsw(_), Distance::Dot) => Ok(Hnsw::<Dot>::prebuild(storage, options)?),
            (O::Flat(_), Distance::L2) => Ok(Flat::<L2>::prebuild(storage, options)?),
            (O::Flat(_), Distance::Cosine) => Ok(Flat::<Cosine>::prebuild(storage, options)?),
            (O::Flat(_), Distance::Dot) => Ok(Flat::<Dot>::prebuild(storage, options)?),
            (O::FlatQ(_), Distance::L2) => Ok(FlatQ::<L2, ProductQuantization<L2>>::prebuild(
                storage, options,
            )?),
            (O::FlatQ(_), Distance::Cosine) => Ok(
                FlatQ::<Cosine, ProductQuantization<Cosine>>::prebuild(storage, options)?,
            ),
            (O::FlatQ(_), Distance::Dot) => Ok(FlatQ::<Dot, ProductQuantization<Dot>>::prebuild(
                storage, options,
            )?),
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
            (O::Hnsw(_), Distance::L2) => {
                Ok(Hnsw::build(storage, options, vectors, n).map(Self::HnswL2)?)
            }
            (O::Hnsw(_), Distance::Cosine) => {
                Ok(Hnsw::build(storage, options, vectors, n).map(Self::HnswCosine)?)
            }
            (O::Hnsw(_), Distance::Dot) => {
                Ok(Hnsw::build(storage, options, vectors, n).map(Self::HnswDot)?)
            }
            (O::Flat(_), Distance::L2) => {
                Ok(Flat::build(storage, options, vectors, n).map(Self::FlatL2)?)
            }
            (O::Flat(_), Distance::Cosine) => {
                Ok(Flat::build(storage, options, vectors, n).map(Self::FlatCosine)?)
            }
            (O::Flat(_), Distance::Dot) => {
                Ok(FlatQ::build(storage, options, vectors, n).map(Self::FlatPqDot)?)
            }
            (O::FlatQ(_), Distance::L2) => {
                Ok(FlatQ::build(storage, options, vectors, n).map(Self::FlatPqL2)?)
            }
            (O::FlatQ(_), Distance::Cosine) => {
                Ok(FlatQ::build(storage, options, vectors, n).map(Self::FlatPqCosine)?)
            }
            (O::FlatQ(_), Distance::Dot) => {
                Ok(FlatQ::build(storage, options, vectors, n).map(Self::FlatPqDot)?)
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
    pub fn save(&self) -> Vec<u8> {
        use Algorithm::*;
        match self {
            HnswL2(sel) => bincode::serialize(&sel.save()).expect("Failed to serialize."),
            HnswCosine(sel) => bincode::serialize(&sel.save()).expect("Failed to serialize."),
            HnswDot(sel) => bincode::serialize(&sel.save()).expect("Failed to serialize."),
            FlatL2(sel) => bincode::serialize(&sel.save()).expect("Failed to serialize."),
            FlatCosine(sel) => bincode::serialize(&sel.save()).expect("Failed to serialize."),
            FlatDot(sel) => bincode::serialize(&sel.save()).expect("Failed to serialize."),
            FlatPqL2(sel) => bincode::serialize(&sel.save()).expect("Failed to serialize."),
            FlatPqCosine(sel) => bincode::serialize(&sel.save()).expect("Failed to serialize."),
            FlatPqDot(sel) => bincode::serialize(&sel.save()).expect("Failed to serialize."),
            IvfL2(sel) => bincode::serialize(&sel.save()).expect("Failed to serialize."),
            IvfCosine(sel) => bincode::serialize(&sel.save()).expect("Failed to serialize."),
            IvfDot(sel) => bincode::serialize(&sel.save()).expect("Failed to serialize."),
        }
    }
    pub fn load(
        storage: &mut Storage,
        options: IndexOptions,
        vectors: Arc<Vectors>,
        save: Vec<u8>,
    ) -> Result<Self, AlgorithmError> {
        use AlgorithmOptions as O;
        match (options.algorithm.clone(), options.distance) {
            (O::Hnsw(_), Distance::L2) => {
                let save = bincode::deserialize(&save).expect("Failed to deserialize.");
                Ok(Hnsw::load(storage, options, vectors, save).map(Self::HnswL2)?)
            }
            (O::Hnsw(_), Distance::Cosine) => {
                let save = bincode::deserialize(&save).expect("Failed to deserialize.");
                Ok(Hnsw::load(storage, options, vectors, save).map(Self::HnswCosine)?)
            }
            (O::Hnsw(_), Distance::Dot) => {
                let save = bincode::deserialize(&save).expect("Failed to deserialize.");
                Ok(Hnsw::load(storage, options, vectors, save).map(Self::HnswDot)?)
            }
            (O::Flat(_), Distance::L2) => {
                let save = bincode::deserialize(&save).expect("Failed to deserialize.");
                Ok(Flat::load(storage, options, vectors, save).map(Self::FlatL2)?)
            }
            (O::Flat(_), Distance::Cosine) => {
                let save = bincode::deserialize(&save).expect("Failed to deserialize.");
                Ok(Flat::load(storage, options, vectors, save).map(Self::FlatCosine)?)
            }
            (O::Flat(_), Distance::Dot) => {
                let save = bincode::deserialize(&save).expect("Failed to deserialize.");
                Ok(Flat::load(storage, options, vectors, save).map(Self::FlatDot)?)
            }
            (O::FlatQ(_), Distance::L2) => {
                let save = bincode::deserialize(&save).expect("Failed to deserialize.");
                Ok(FlatQ::load(storage, options, vectors, save).map(Self::FlatPqL2)?)
            }
            (O::FlatQ(_), Distance::Cosine) => {
                let save = bincode::deserialize(&save).expect("Failed to deserialize.");
                Ok(FlatQ::load(storage, options, vectors, save).map(Self::FlatPqCosine)?)
            }
            (O::FlatQ(_), Distance::Dot) => {
                let save = bincode::deserialize(&save).expect("Failed to deserialize.");
                Ok(FlatQ::load(storage, options, vectors, save).map(Self::FlatPqDot)?)
            }
            (O::Ivf(_), Distance::L2) => {
                let save = bincode::deserialize(&save).expect("Failed to deserialize.");
                Ok(Ivf::load(storage, options, vectors, save).map(Self::IvfL2)?)
            }
            (O::Ivf(_), Distance::Cosine) => {
                let save = bincode::deserialize(&save).expect("Failed to deserialize.");
                Ok(Ivf::load(storage, options, vectors, save).map(Self::IvfCosine)?)
            }
            (O::Ivf(_), Distance::Dot) => {
                let save = bincode::deserialize(&save).expect("Failed to deserialize.");
                Ok(Ivf::load(storage, options, vectors, save).map(Self::IvfDot)?)
            }
        }
    }
    pub fn insert(&self, insert: usize) -> Result<(), AlgorithmError> {
        use Algorithm::*;
        match self {
            HnswL2(sel) => Ok(sel.insert(insert)?),
            HnswCosine(sel) => Ok(sel.insert(insert)?),
            HnswDot(sel) => Ok(sel.insert(insert)?),
            FlatL2(sel) => Ok(sel.insert(insert)?),
            FlatCosine(sel) => Ok(sel.insert(insert)?),
            FlatDot(sel) => Ok(sel.insert(insert)?),
            FlatPqL2(sel) => Ok(sel.insert(insert)?),
            FlatPqCosine(sel) => Ok(sel.insert(insert)?),
            FlatPqDot(sel) => Ok(sel.insert(insert)?),
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
            HnswL2(sel) => Ok(sel.search(target, k, filter)?),
            HnswCosine(sel) => Ok(sel.search(target, k, filter)?),
            HnswDot(sel) => Ok(sel.search(target, k, filter)?),
            FlatL2(sel) => Ok(sel.search(target, k, filter)?),
            FlatCosine(sel) => Ok(sel.search(target, k, filter)?),
            FlatDot(sel) => Ok(sel.search(target, k, filter)?),
            FlatPqL2(sel) => Ok(sel.search(target, k, filter)?),
            FlatPqCosine(sel) => Ok(sel.search(target, k, filter)?),
            FlatPqDot(sel) => Ok(sel.search(target, k, filter)?),
            IvfL2(sel) => Ok(sel.search(target, k, filter)?),
            IvfCosine(sel) => Ok(sel.search(target, k, filter)?),
            IvfDot(sel) => Ok(sel.search(target, k, filter)?),
        }
    }
}

pub trait Algo: Sized {
    type Error: std::error::Error + serde::Serialize + for<'a> serde::Deserialize<'a>;
    type Save: serde::Serialize + for<'a> serde::Deserialize<'a>;
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
    fn save(&self) -> Self::Save;
    fn load(
        storage: &mut Storage,
        options: IndexOptions,
        vectors: Arc<Vectors>,
        save: Self::Save,
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
