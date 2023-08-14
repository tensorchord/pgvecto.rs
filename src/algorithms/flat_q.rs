use super::impls::quantization::*;
use super::utils::filtered_fixed_heap::FilteredFixedHeap;
use super::Algo;
use crate::bgworker::index::IndexOptions;
use crate::bgworker::storage::Storage;
use crate::bgworker::storage::StoragePreallocator;
use crate::bgworker::vectors::Vectors;
use crate::prelude::*;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Clone, Error, Serialize, Deserialize)]
pub enum FlatQError {
    #[error("Quantization {0}")]
    Quantization(#[from] QuantizationError),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlatQOptions {
    pub memmap: Memmap,
    pub sample_size: usize,
}

pub struct FlatQ<D: DistanceFamily, Q: Quantization> {
    vectors: Arc<Vectors>,
    implementation: QuantizationImpl<Q>,
    _maker: PhantomData<D>,
}

impl<D: DistanceFamily, Q: Quantization> Algo for FlatQ<D, Q> {
    type Error = FlatQError;

    type Save = Q;

    fn prebuild(
        storage: &mut StoragePreallocator,
        options: IndexOptions,
    ) -> Result<(), Self::Error> {
        let flat_q_options = options.algorithm.clone().unwrap_flat_q();
        QuantizationImpl::<Q>::prebuild(
            storage,
            options.dims,
            options.capacity,
            flat_q_options.memmap,
        )?;
        Ok(())
    }

    fn build(
        storage: &mut Storage,
        options: IndexOptions,
        vectors: Arc<Vectors>,
        n: usize,
    ) -> Result<Self, FlatQError> {
        let flat_q_options = options.algorithm.clone().unwrap_flat_q();
        let implementation = QuantizationImpl::new(
            storage,
            vectors.clone(),
            options.dims,
            n,
            flat_q_options.sample_size,
            options.capacity,
            flat_q_options.memmap,
        )?;
        Ok(Self {
            vectors,
            implementation,
            _maker: PhantomData,
        })
    }

    fn save(&self) -> Q {
        self.implementation.save()
    }

    fn load(
        storage: &mut Storage,
        options: IndexOptions,
        vectors: Arc<Vectors>,
        save: Q,
    ) -> Result<Self, FlatQError> {
        let flat_q_options = options.algorithm.clone().unwrap_flat_q();
        Ok(Self {
            vectors: vectors.clone(),
            implementation: QuantizationImpl::load(
                storage,
                vectors,
                save,
                options.capacity,
                flat_q_options.memmap,
            )?,
            _maker: PhantomData,
        })
    }

    fn insert(&self, x: usize) -> Result<(), FlatQError> {
        self.implementation.insert(x)?;
        Ok(())
    }

    fn search<F>(
        &self,
        target: Box<[Scalar]>,
        k: usize,
        filter: F,
    ) -> Result<Vec<(Scalar, u64)>, FlatQError>
    where
        F: FnMut(u64) -> bool,
    {
        let mut result = FilteredFixedHeap::new(k, filter);
        let vector = self.implementation.process(&target);
        for i in 0..self.vectors.len() {
            let this_vector = self.implementation.get_vector(i);
            let this_data = self.vectors.get_data(i);
            let dis = self.implementation.distance(&vector, this_vector);
            result.push((dis, this_data));
        }
        let mut output = Vec::new();
        for (i, j) in result.into_sorted_vec().into_iter() {
            output.push((i, j));
        }
        Ok(output)
    }
}
