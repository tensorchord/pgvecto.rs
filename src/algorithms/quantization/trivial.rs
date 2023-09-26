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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrivialQuantizationOptions {}

impl Default for TrivialQuantizationOptions {
    fn default() -> Self {
        Self {}
    }
}

#[derive(Clone)]
pub struct TrivialQuantization {
    vectors: Arc<Vectors>,
}

impl Quan for TrivialQuantization {
    fn prebuild(_: &mut StoragePreallocator, _: IndexOptions, _: QuantizationOptions) {}

    fn build(
        _: &mut Storage,
        _: IndexOptions,
        quantization_options: QuantizationOptions,
        vectors: Arc<Vectors>,
    ) -> Self {
        let _quantization_options = quantization_options.unwrap_trivial_quantization();
        Self { vectors }
    }

    fn load(
        _: &mut Storage,
        _: IndexOptions,
        _: QuantizationOptions,
        vectors: Arc<Vectors>,
    ) -> Self {
        Self { vectors }
    }

    fn insert(&self, _: usize, _: &[Scalar]) -> Result<(), QuantizationError> {
        Ok(())
    }

    fn distance(&self, d: Distance, lhs: &[Scalar], rhs: usize) -> Scalar {
        d.distance(lhs, self.vectors.get_vector(rhs))
    }

    fn distance2(&self, d: Distance, lhs: usize, rhs: usize) -> Scalar {
        d.distance(self.vectors.get_vector(lhs), self.vectors.get_vector(rhs))
    }
}
