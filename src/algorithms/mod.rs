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
use self::impls::quantization::{NopQuantization, ProductQuantization};
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
    FlatNopL2(Flat<L2, NopQuantization<L2>>),
    FlatNopCosine(Flat<Cosine, NopQuantization<Cosine>>),
    FlatNopDot(Flat<Dot, NopQuantization<Dot>>),
    FlatPqL2(Flat<L2, ProductQuantization<L2>>),
    FlatPqCosine(Flat<Cosine, ProductQuantization<Cosine>>),
    FlatPqDot(Flat<Dot, ProductQuantization<Dot>>),
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
            (O::Flat(flat_options), Distance::L2) => match flat_options.quantization {
                None => Ok(Flat::<L2, NopQuantization<L2>>::prebuild(storage, options)?),
                Some(_) => Ok(Flat::<L2, ProductQuantization<L2>>::prebuild(
                    storage, options,
                )?),
            },
            (O::Flat(flat_options), Distance::Cosine) => match flat_options.quantization {
                None => Ok(Flat::<Cosine, NopQuantization<Cosine>>::prebuild(
                    storage, options,
                )?),
                Some(_) => Ok(Flat::<Cosine, ProductQuantization<Cosine>>::prebuild(
                    storage, options,
                )?),
            },
            (O::Flat(flat_options), Distance::Dot) => match flat_options.quantization {
                None => Ok(Flat::<Dot, NopQuantization<Dot>>::prebuild(
                    storage, options,
                )?),
                Some(_) => Ok(Flat::<Dot, ProductQuantization<Dot>>::prebuild(
                    storage, options,
                )?),
            },
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
            (O::Flat(flat_options), Distance::L2) => match flat_options.quantization {
                None => Ok(Flat::build(storage, options, vectors, n).map(Self::FlatNopL2)?),
                Some(_) => Ok(Flat::build(storage, options, vectors, n).map(Self::FlatPqL2)?),
            },
            (O::Flat(flat_options), Distance::Cosine) => match flat_options.quantization {
                None => Ok(Flat::build(storage, options, vectors, n).map(Self::FlatNopCosine)?),
                Some(_) => Ok(Flat::build(storage, options, vectors, n).map(Self::FlatPqCosine)?),
            },
            (O::Flat(flat_options), Distance::Dot) => match flat_options.quantization {
                None => Ok(Flat::build(storage, options, vectors, n).map(Self::FlatNopDot)?),
                Some(_) => Ok(Flat::build(storage, options, vectors, n).map(Self::FlatPqDot)?),
            },
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
    pub fn save(&self) -> Vec<u8> {
        use Algorithm::*;
        match self {
            FlatNopL2(sel) => bincode::serialize(&sel.save()).expect("Failed to serialize."),
            FlatNopCosine(sel) => bincode::serialize(&sel.save()).expect("Failed to serialize."),
            FlatNopDot(sel) => bincode::serialize(&sel.save()).expect("Failed to serialize."),
            FlatPqL2(sel) => bincode::serialize(&sel.save()).expect("Failed to serialize."),
            FlatPqCosine(sel) => bincode::serialize(&sel.save()).expect("Failed to serialize."),
            FlatPqDot(sel) => bincode::serialize(&sel.save()).expect("Failed to serialize."),
            HnswL2(sel) => bincode::serialize(&sel.save()).expect("Failed to serialize."),
            HnswCosine(sel) => bincode::serialize(&sel.save()).expect("Failed to serialize."),
            HnswDot(sel) => bincode::serialize(&sel.save()).expect("Failed to serialize."),
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
            (O::Flat(flat_options), Distance::L2) => match flat_options.quantization {
                None => {
                    let save = bincode::deserialize(&save).expect("Failed to deserialize.");
                    Ok(Flat::load(storage, options, vectors, save).map(Self::FlatNopL2)?)
                }
                Some(_) => {
                    let save = bincode::deserialize(&save).expect("Failed to deserialize.");
                    Ok(Flat::load(storage, options, vectors, save).map(Self::FlatPqL2)?)
                }
            },
            (O::Flat(flat_options), Distance::Cosine) => match flat_options.quantization {
                None => {
                    let save = bincode::deserialize(&save).expect("Failed to deserialize.");
                    Ok(Flat::load(storage, options, vectors, save).map(Self::FlatNopCosine)?)
                }
                Some(_) => {
                    let save = bincode::deserialize(&save).expect("Failed to deserialize.");
                    Ok(Flat::load(storage, options, vectors, save).map(Self::FlatPqCosine)?)
                }
            },
            (O::Flat(flat_options), Distance::Dot) => match flat_options.quantization {
                None => {
                    let save = bincode::deserialize(&save).expect("Failed to deserialize.");
                    Ok(Flat::load(storage, options, vectors, save).map(Self::FlatNopDot)?)
                }
                Some(_) => {
                    let save = bincode::deserialize(&save).expect("Failed to deserialize.");
                    Ok(Flat::load(storage, options, vectors, save).map(Self::FlatPqDot)?)
                }
            },
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
            FlatNopL2(sel) => Ok(sel.insert(insert)?),
            FlatNopCosine(sel) => Ok(sel.insert(insert)?),
            FlatNopDot(sel) => Ok(sel.insert(insert)?),
            FlatPqL2(sel) => Ok(sel.insert(insert)?),
            FlatPqCosine(sel) => Ok(sel.insert(insert)?),
            FlatPqDot(sel) => Ok(sel.insert(insert)?),
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
            FlatNopL2(sel) => Ok(sel.search(target, k, filter)?),
            FlatNopCosine(sel) => Ok(sel.search(target, k, filter)?),
            FlatNopDot(sel) => Ok(sel.search(target, k, filter)?),
            FlatPqL2(sel) => Ok(sel.search(target, k, filter)?),
            FlatPqCosine(sel) => Ok(sel.search(target, k, filter)?),
            FlatPqDot(sel) => Ok(sel.search(target, k, filter)?),
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
