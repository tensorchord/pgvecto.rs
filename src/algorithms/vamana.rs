use super::impls::vamana::VamanaImpl;
use super::Algo;
use crate::bgworker::index::IndexOptions;
use crate::bgworker::storage::Storage;
use crate::bgworker::storage::StoragePreallocator;
use crate::bgworker::vectors::Vectors;
use crate::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Clone, Error, Serialize, Deserialize)]
pub enum VamanaError {
    //
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VamanaOptions {
    #[serde(default)]
    pub memmap: Memmap,
    /// out degree bound
    #[serde(default = "VamanaOptions::default_r")]
    pub r: usize,
    /// Distance threshold
    #[serde(default = "VamanaOptions::default_alpha")]
    pub alpha: f32,
    /// Search list size
    #[serde(default = "VamanaOptions::default_l")]
    pub l: usize,
    #[serde(default = "VamanaOptions::default_build_threads")]
    pub build_threads: usize,
}

impl VamanaOptions {
    fn default_r() -> usize {
        50
    }
    fn default_alpha() -> f32 {
        1.2
    }
    fn default_l() -> usize {
        70
    }
    fn default_build_threads() -> usize {
        std::thread::available_parallelism().unwrap().get()
    }
}

impl Default for VamanaOptions {
    fn default() -> Self {
        Self {
            memmap: Default::default(),
            r: Self::default_r(),
            alpha: Self::default_alpha(),
            l: Self::default_l(),
            build_threads: Self::default_build_threads(),
        }
    }
}

pub struct Vamana {
    implementation: VamanaImpl,
}

impl Algo for Vamana {
    type Error = VamanaError;

    fn prebuild(
        storage: &mut StoragePreallocator,
        options: IndexOptions,
    ) -> Result<(), Self::Error> {
        let vamana_options = options.algorithm.clone().unwrap_vamana();
        VamanaImpl::prebuild(
            storage,
            options.capacity,
            vamana_options.r,
            vamana_options.memmap,
        )?;
        Ok(())
    }

    fn build(
        storage: &mut Storage,
        options: IndexOptions,
        vectors: Arc<Vectors>,
        n: usize,
    ) -> Result<Self, VamanaError> {
        let vamana_options = options.algorithm.clone().unwrap_vamana();
        let implementation = VamanaImpl::new(
            storage,
            vectors,
            n,
            options.capacity,
            options.dims,
            vamana_options.r,
            vamana_options.alpha,
            vamana_options.l,
            vamana_options.build_threads,
            vamana_options.memmap,
            options.d,
        )?;
        Ok(Self { implementation })
    }

    fn load(
        storage: &mut Storage,
        options: IndexOptions,
        vectors: Arc<Vectors>,
    ) -> Result<Self, VamanaError> {
        let vamana_options = options.algorithm.unwrap_vamana();
        let implementation = VamanaImpl::load(
            storage,
            vectors,
            options.capacity,
            options.dims,
            vamana_options.r,
            vamana_options.alpha,
            vamana_options.l,
            vamana_options.build_threads,
            vamana_options.memmap,
            options.d,
        )?;
        Ok(Self { implementation })
    }
    #[allow(unused)]
    fn insert(&self, insert: usize) -> Result<(), VamanaError> {
        Ok(self.implementation.insert(insert)?)
    }
    fn search<F>(
        &self,
        target: Box<[Scalar]>,
        k: usize,
        filter: F,
    ) -> Result<Vec<(Scalar, u64)>, VamanaError>
    where
        F: FnMut(u64) -> bool,
    {
        Ok(self.implementation.search(target, k, filter)?)
    }
}
