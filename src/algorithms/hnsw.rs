use super::impls::hnsw::HnswImpl;
use super::Algo;
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
pub enum HnswError {
    //
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HnswOptions {
    #[serde(default = "HnswOptions::default_memmap")]
    pub memmap: Memmap,
    #[serde(default = "HnswOptions::default_build_threads")]
    pub build_threads: usize,
    #[serde(default = "HnswOptions::default_max_threads")]
    pub max_threads: usize,
    #[serde(default = "HnswOptions::default_m")]
    pub m: usize,
    #[serde(default = "HnswOptions::default_ef_construction")]
    pub ef_construction: usize,
}

impl HnswOptions {
    fn default_memmap() -> Memmap {
        Memmap::Ram
    }
    fn default_build_threads() -> usize {
        std::thread::available_parallelism().unwrap().get()
    }
    fn default_max_threads() -> usize {
        std::thread::available_parallelism().unwrap().get() * 2
    }
    fn default_m() -> usize {
        36
    }
    fn default_ef_construction() -> usize {
        500
    }
}

pub struct Hnsw<D: DistanceFamily> {
    implementation: HnswImpl<D>,
}

impl<D: DistanceFamily> Algo for Hnsw<D> {
    type Error = HnswError;

    fn prebuild(
        storage: &mut StoragePreallocator,
        options: IndexOptions,
    ) -> Result<(), Self::Error> {
        let hnsw_options = options.algorithm.clone().unwrap_hnsw();
        HnswImpl::<D>::prebuild(
            storage,
            options.capacity,
            hnsw_options.m,
            hnsw_options.memmap,
        )?;
        Ok(())
    }

    fn build(
        storage: &mut Storage,
        options: IndexOptions,
        vectors: Arc<Vectors>,
        n: usize,
    ) -> Result<Self, HnswError> {
        let hnsw_options = options.algorithm.clone().unwrap_hnsw();
        let implementation = HnswImpl::new(
            storage,
            vectors,
            options.dims,
            options.capacity,
            hnsw_options.max_threads,
            hnsw_options.m,
            hnsw_options.ef_construction,
            hnsw_options.memmap,
        )?;
        let i = AtomicUsize::new(0);
        std::thread::scope(|scope| -> Result<(), HnswError> {
            let mut handles = Vec::new();
            for _ in 0..hnsw_options.build_threads {
                handles.push(scope.spawn(|| -> Result<(), HnswError> {
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
    ) -> Result<Self, HnswError> {
        let hnsw_options = options.algorithm.unwrap_hnsw();
        let implementation = HnswImpl::load(
            storage,
            vectors,
            options.dims,
            options.capacity,
            hnsw_options.max_threads,
            hnsw_options.m,
            hnsw_options.ef_construction,
            hnsw_options.memmap,
        )?;
        Ok(Self { implementation })
    }
    fn insert(&self, insert: usize) -> Result<(), HnswError> {
        self.implementation.insert(insert)
    }
    fn search<F>(
        &self,
        target: Box<[Scalar]>,
        k: usize,
        filter: F,
    ) -> Result<Vec<(Scalar, u64)>, HnswError>
    where
        F: FnMut(u64) -> bool,
    {
        self.implementation.search(target, k, filter)
    }
}
