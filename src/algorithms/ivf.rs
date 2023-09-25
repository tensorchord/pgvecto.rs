use super::impls::ivf::IvfImpl;
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
    #[serde(default = "IvfOptions::default_memmap")]
    pub memmap: Memmap,
    #[serde(default = "IvfOptions::default_build_threads")]
    pub build_threads: usize,
    #[serde(default = "IvfOptions::default_least_iterations")]
    pub least_iterations: usize,
    #[serde(default = "IvfOptions::default_iterations")]
    pub iterations: usize,
    pub nlist: usize,
    pub nprobe: usize,
    #[serde(default)]
    pub quantization: QuantizationOptions,
}

impl IvfOptions {
    fn default_memmap() -> Memmap {
        Memmap::Ram
    }
    fn default_build_threads() -> usize {
        std::thread::available_parallelism().unwrap().get()
    }
    fn default_least_iterations() -> usize {
        16
    }
    fn default_iterations() -> usize {
        500
    }
}

pub struct Ivf {
    implementation: IvfImpl,
}

impl Algo for Ivf {
    type Error = IvfError;

    fn prebuild(
        storage: &mut StoragePreallocator,
        options: IndexOptions,
    ) -> Result<(), Self::Error> {
        let ivf_options = options.algorithm.clone().unwrap_ivf();
        IvfImpl::prebuild(
            storage,
            options.dims,
            ivf_options.nlist,
            options.capacity,
            ivf_options.memmap,
            options,
            ivf_options.quantization,
        )?;
        Ok(())
    }

    fn build(
        storage: &mut Storage,
        options: IndexOptions,
        vectors: Arc<Vectors>,
        n: usize,
    ) -> Result<Self, IvfError> {
        let ivf_options = options.algorithm.clone().unwrap_ivf();
        let implementation = IvfImpl::new(
            storage,
            vectors.clone(),
            options.dims,
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
                        implementation.insert(i)?;
                    }
                    Result::Ok(())
                }));
            }
            for handle in handles.into_iter() {
                handle.join().unwrap()?;
            }
            Result::Ok(())
        })?;
        Ok(Self { implementation })
    }
    fn load(
        storage: &mut Storage,
        options: IndexOptions,
        vectors: Arc<Vectors>,
    ) -> Result<Self, IvfError> {
        let ivf_options = options.algorithm.clone().unwrap_ivf();
        let implementation = IvfImpl::load(
            storage,
            options.dims,
            vectors,
            ivf_options.nlist,
            ivf_options.nprobe,
            options.capacity,
            ivf_options.memmap,
            options,
            ivf_options.quantization,
        )?;
        Ok(Self { implementation })
    }
    fn insert(&self, insert: usize) -> Result<(), IvfError> {
        self.implementation.insert(insert)
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
        self.implementation.search(target, k, filter)
    }
}
