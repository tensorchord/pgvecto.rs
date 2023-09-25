use super::quantization::Quantization;
use super::utils::filtered_fixed_heap::FilteredFixedHeap;
use super::Algo;
use crate::algorithms::quantization::Quan;
use crate::algorithms::quantization::QuantizationError;
use crate::algorithms::quantization::QuantizationOptions;
use crate::bgworker::index::IndexOptions;
use crate::bgworker::storage::Storage;
use crate::bgworker::storage::StoragePreallocator;
use crate::bgworker::vectors::Vectors;
use crate::prelude::*;
use serde::{Deserialize, Serialize};
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

pub struct Flat {
    vectors: Arc<Vectors>,
    quantization: Quantization,
    d: Distance,
}

impl Algo for Flat {
    type Error = FlatError;

    fn prebuild(
        storage: &mut StoragePreallocator,
        index_options: IndexOptions,
    ) -> Result<(), Self::Error> {
        let flat_options = index_options.algorithm.clone().unwrap_flat();
        Quantization::prebuild(storage, index_options, flat_options.quantization);
        Ok(())
    }

    fn build(
        storage: &mut Storage,
        options: IndexOptions,
        vectors: Arc<Vectors>,
        n: usize,
    ) -> Result<Self, FlatError> {
        let d = options.d;
        let flat_options = options.algorithm.clone().unwrap_flat();
        let quantization =
            Quantization::build(storage, options, flat_options.quantization, vectors.clone());
        for i in 0..n {
            quantization.insert(i, vectors.get_vector(i))?;
        }
        Ok(Self {
            vectors,
            quantization,
            d,
        })
    }

    fn load(
        storage: &mut Storage,
        options: IndexOptions,
        vectors: Arc<Vectors>,
    ) -> Result<Self, FlatError> {
        let d = options.d;
        let flat_options = options.algorithm.clone().unwrap_flat();
        let quantization =
            Quantization::load(storage, options, flat_options.quantization, vectors.clone());
        Ok(Self {
            vectors: vectors.clone(),
            quantization,
            d,
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
            let dis = self.quantization.distance(self.d, &target, i);
            result.push((dis, this_data));
        }
        let mut output = Vec::new();
        for (i, j) in result.into_sorted_vec().into_iter() {
            output.push((i, j));
        }
        Ok(output)
    }
}
