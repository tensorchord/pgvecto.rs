use super::impls::ivf_native::IvfNative;
use super::impls::ivf_pq::IvfPq;
use super::Algo;
use crate::algorithms::quantization::QuantizationError;
use crate::algorithms::quantization::QuantizationOptions;
use crate::bgworker::index::IndexOptions;
use crate::bgworker::storage::Storage;
use crate::bgworker::storage::StoragePreallocator;
use crate::bgworker::vectors::Vectors;
use crate::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Clone, Error, Serialize, Deserialize)]
pub enum IvfError {
    #[error("Quantization {0}")]
    Quantization(#[from] QuantizationError),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IvfOptions {
    #[serde(default)]
    pub memmap: Memmap,
    #[serde(default = "IvfOptions::default_build_threads")]
    pub build_threads: usize,
    #[serde(default = "IvfOptions::default_least_iterations")]
    pub least_iterations: usize,
    #[serde(default = "IvfOptions::default_iterations")]
    pub iterations: usize,
    #[serde(default = "IvfOptions::default_nlist")]
    pub nlist: usize,
    #[serde(default = "IvfOptions::default_nprobe")]
    pub nprobe: usize,
    #[serde(default)]
    pub quantization: QuantizationOptions,
}

impl IvfOptions {
    fn default_build_threads() -> usize {
        std::thread::available_parallelism().unwrap().get()
    }
    fn default_least_iterations() -> usize {
        16
    }
    fn default_iterations() -> usize {
        500
    }
    fn default_nlist() -> usize {
        1000
    }
    fn default_nprobe() -> usize {
        10
    }
}

impl Default for IvfOptions {
    fn default() -> Self {
        Self {
            memmap: Default::default(),
            build_threads: Self::default_build_threads(),
            least_iterations: Self::default_least_iterations(),
            iterations: Self::default_iterations(),
            nlist: Self::default_nlist(),
            nprobe: Self::default_nprobe(),
            quantization: Default::default(),
        }
    }
}

pub enum Ivf {
    Native(IvfNative),
    Pq(IvfPq),
}

impl Algo for Ivf {
    type Error = IvfError;

    fn prebuild(
        storage: &mut StoragePreallocator,
        options: IndexOptions,
    ) -> Result<(), Self::Error> {
        let ivf_options = options.algorithm.clone().unwrap_ivf();
        if ivf_options.quantization.is_product_quantization() {
            IvfPq::prebuild(
                storage,
                options.dims,
                ivf_options.nlist,
                options.capacity,
                ivf_options.memmap,
                options,
                ivf_options.quantization,
            )?;
        } else {
            IvfNative::prebuild(
                storage,
                options.dims,
                ivf_options.nlist,
                options.capacity,
                ivf_options.memmap,
                options,
                ivf_options.quantization,
            )?;
        }
        Ok(())
    }

    fn build(
        storage: &mut Storage,
        options: IndexOptions,
        vectors: Arc<Vectors>,
        n: usize,
    ) -> Result<Self, IvfError> {
        let ivf_options = options.algorithm.clone().unwrap_ivf();
        if ivf_options.quantization.is_product_quantization() {
            let x = IvfPq::new(
                storage,
                vectors.clone(),
                n,
                ivf_options.nlist,
                ivf_options.nlist * 50,
                ivf_options.nprobe,
                ivf_options.least_iterations,
                ivf_options.iterations,
                options.capacity,
                ivf_options.memmap,
                options,
                ivf_options.quantization,
            )?;
            let i = AtomicUsize::new(0);
            std::thread::scope(|scope| -> Result<(), IvfError> {
                let mut handles = Vec::new();
                for _ in 0..ivf_options.build_threads {
                    handles.push(scope.spawn(|| -> Result<(), IvfError> {
                        loop {
                            let i = i.fetch_add(1, Ordering::Relaxed);
                            if i >= n {
                                break;
                            }
                            x.insert(i)?;
                        }
                        Result::Ok(())
                    }));
                }
                for handle in handles.into_iter() {
                    handle.join().unwrap()?;
                }
                Result::Ok(())
            })?;
            Ok(Self::Pq(x))
        } else {
            let x = IvfNative::new(
                storage,
                vectors.clone(),
                n,
                ivf_options.nlist,
                ivf_options.nlist * 50,
                ivf_options.nprobe,
                ivf_options.least_iterations,
                ivf_options.iterations,
                options.capacity,
                ivf_options.memmap,
                options,
                ivf_options.quantization,
            )?;
            let i = AtomicUsize::new(0);
            std::thread::scope(|scope| -> Result<(), IvfError> {
                let mut handles = Vec::new();
                for _ in 0..ivf_options.build_threads {
                    handles.push(scope.spawn(|| -> Result<(), IvfError> {
                        loop {
                            let i = i.fetch_add(1, Ordering::Relaxed);
                            if i >= n {
                                break;
                            }
                            x.insert(i)?;
                        }
                        Result::Ok(())
                    }));
                }
                for handle in handles.into_iter() {
                    handle.join().unwrap()?;
                }
                Result::Ok(())
            })?;
            Ok(Self::Native(x))
        }
    }
    fn load(
        storage: &mut Storage,
        options: IndexOptions,
        vectors: Arc<Vectors>,
    ) -> Result<Self, IvfError> {
        let ivf_options = options.algorithm.clone().unwrap_ivf();
        if ivf_options.quantization.is_product_quantization() {
            let x = IvfPq::load(
                storage,
                vectors,
                ivf_options.nlist,
                ivf_options.nprobe,
                options.capacity,
                ivf_options.memmap,
                options,
                ivf_options.quantization,
            )?;
            Ok(Self::Pq(x))
        } else {
            let x = IvfNative::load(
                storage,
                vectors,
                ivf_options.nlist,
                ivf_options.nprobe,
                options.capacity,
                ivf_options.memmap,
                options,
                ivf_options.quantization,
            )?;
            Ok(Self::Native(x))
        }
    }
    fn insert(&self, insert: usize) -> Result<(), IvfError> {
        match self {
            Ivf::Native(x) => x.insert(insert),
            Ivf::Pq(x) => x.insert(insert),
        }
    }
    fn search<F>(
        &self,
        target: Box<[Scalar]>,
        k: usize,
        filter: F,
    ) -> Result<Vec<(Scalar, u64)>, IvfError>
    where
        F: FnMut(u64) -> bool,
    {
        match self {
            Ivf::Native(x) => x.search(target, k, filter),
            Ivf::Pq(x) => x.search(target, k, filter),
        }
    }
}
