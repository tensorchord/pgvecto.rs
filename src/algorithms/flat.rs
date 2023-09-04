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
pub enum FlatError {
    #[error("Quantization {0}")]
    Quantization(#[from] QuantizationError),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlatOptions {
    #[serde(default)]
    pub quantization: Option<QuantizationOptions>,
}

pub struct Flat<D: DistanceFamily, Q: Quantization> {
    vectors: Arc<Vectors>,
    implementation: QuantizationImpl<Q>,
    _maker: PhantomData<D>,
}

impl<D: DistanceFamily, Q: Quantization> Algo for Flat<D, Q> {
    type Error = FlatError;

    type Save = Q;

    fn prebuild(
        storage: &mut StoragePreallocator,
        options: IndexOptions,
    ) -> Result<(), Self::Error> {
        let flat_options = options.algorithm.clone().unwrap_flat();
        QuantizationImpl::<Q>::prebuild(
            storage,
            options.dims,
            options.capacity,
            flat_options.quantization.unwrap_or(QuantizationOptions {
                memmap: Memmap::Ram,
                sample: 0,
            }),
        )?;
        Ok(())
    }

    fn build(
        storage: &mut Storage,
        options: IndexOptions,
        vectors: Arc<Vectors>,
        n: usize,
    ) -> Result<Self, FlatError> {
        let flat_options = options.algorithm.clone().unwrap_flat();
        let implementation = QuantizationImpl::new(
            storage,
            vectors.clone(),
            options.dims,
            n,
            options.capacity,
            flat_options.quantization.unwrap_or(QuantizationOptions {
                memmap: Memmap::Ram,
                sample: 0,
            }),
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
    ) -> Result<Self, FlatError> {
        let flat_options = options.algorithm.clone().unwrap_flat();
        Ok(Self {
            vectors: vectors.clone(),
            implementation: QuantizationImpl::load(
                storage,
                vectors,
                save,
                options.capacity,
                flat_options.quantization.unwrap_or(QuantizationOptions {
                    memmap: Memmap::Ram,
                    sample: 0,
                }),
            )?,
            _maker: PhantomData,
        })
    }

    fn insert(&self, x: usize) -> Result<(), FlatError> {
        self.implementation.insert(x)?;
        Ok(())
    }

    fn search<F>(
        &self,
        target: Box<[Scalar]>,
        k: usize,
        filter: F,
    ) -> Result<Vec<(Scalar, u64)>, FlatError>
    where
        F: FnMut(u64) -> bool,
    {
        let mut result = FilteredFixedHeap::new(k, filter);
        for i in 0..self.vectors.len() {
            let this_vector = self.implementation.get_vector(i);
            let this_data = self.vectors.get_data(i);
            let dis = self
                .implementation
                .asymmetric_distance(&target, this_vector);
            result.push((dis, this_data));
        }
        let mut output = Vec::new();
        for (i, j) in result.into_sorted_vec().into_iter() {
            output.push((i, j));
        }
        Ok(output)
    }
}
