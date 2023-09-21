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
    pub quantization: QuantizationOptions,
}

pub struct Flat<D: DistanceFamily> {
    vectors: Arc<Vectors>,
    quantization: Box<dyn Quantization>,
    _maker: PhantomData<D>,
}

impl<D: DistanceFamily> Algo for Flat<D> {
    type Error = FlatError;

    fn prebuild(
        storage: &mut StoragePreallocator,
        options: IndexOptions,
    ) -> Result<(), Self::Error> {
        let flat_options = options.algorithm.clone().unwrap_flat();
        match flat_options.quantization {
            quantization_options @ QuantizationOptions::Trivial(_) => {
                TrivialQuantization::<D>::prebuild(storage, options, quantization_options);
            }
            quantization_options @ QuantizationOptions::Scalar(_) => {
                ScalarQuantization::<D>::prebuild(storage, options, quantization_options);
            }
            quantization_options @ QuantizationOptions::Product(_) => {
                ProductQuantization::<D>::prebuild(storage, options, quantization_options);
            }
        };
        Ok(())
    }

    fn build(
        storage: &mut Storage,
        options: IndexOptions,
        vectors: Arc<Vectors>,
        n: usize,
    ) -> Result<Self, FlatError> {
        let flat_options = options.algorithm.clone().unwrap_flat();
        let implementation: Box<dyn Quantization> = match flat_options.quantization {
            quantization_options @ QuantizationOptions::Trivial(_) => {
                Box::new(TrivialQuantization::<D>::build(
                    storage,
                    options,
                    quantization_options,
                    vectors.clone(),
                ))
            }
            quantization_options @ QuantizationOptions::Scalar(_) => {
                Box::new(ScalarQuantization::<D>::build(
                    storage,
                    options,
                    quantization_options,
                    vectors.clone(),
                ))
            }
            quantization_options @ QuantizationOptions::Product(_) => {
                Box::new(ProductQuantization::<D>::build(
                    storage,
                    options,
                    quantization_options,
                    vectors.clone(),
                ))
            }
        };
        for i in 0..n {
            implementation.insert(i, vectors.get_vector(i))?;
        }
        Ok(Self {
            vectors,
            quantization: implementation,
            _maker: PhantomData,
        })
    }

    fn load(
        storage: &mut Storage,
        options: IndexOptions,
        vectors: Arc<Vectors>,
    ) -> Result<Self, FlatError> {
        let flat_options = options.algorithm.clone().unwrap_flat();
        let implementation: Box<dyn Quantization> = match flat_options.quantization {
            quantization_options @ QuantizationOptions::Trivial(_) => {
                Box::new(TrivialQuantization::<D>::load(
                    storage,
                    options,
                    quantization_options,
                    vectors.clone(),
                ))
            }
            quantization_options @ QuantizationOptions::Scalar(_) => {
                Box::new(ScalarQuantization::<D>::load(
                    storage,
                    options,
                    quantization_options,
                    vectors.clone(),
                ))
            }
            quantization_options @ QuantizationOptions::Product(_) => {
                Box::new(ProductQuantization::<D>::load(
                    storage,
                    options,
                    quantization_options,
                    vectors.clone(),
                ))
            }
        };
        Ok(Self {
            vectors: vectors.clone(),
            quantization: implementation,
            _maker: PhantomData,
        })
    }

    fn insert(&self, x: usize) -> Result<(), FlatError> {
        self.quantization.insert(x, self.vectors.get_vector(x))?;
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
            let this_data = self.vectors.get_data(i);
            let dis = self.quantization.distance(&target, i);
            result.push((dis, this_data));
        }
        let mut output = Vec::new();
        for (i, j) in result.into_sorted_vec().into_iter() {
            output.push((i, j));
        }
        Ok(output)
    }
}
