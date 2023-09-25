mod product;
mod scalar;
mod trivial;

use crate::bgworker::index::IndexOptions;
use crate::bgworker::storage::{Storage, StoragePreallocator};
use crate::bgworker::vectors::Vectors;
use crate::prelude::*;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::Arc;
use thiserror::Error;

pub use self::product::{ProductQuantization, ProductQuantizationOptions};
pub use self::scalar::{ScalarQuantization, ScalarQuantizationOptions};
pub use self::trivial::{TrivialQuantization, TrivialQuantizationOptions};

#[derive(Debug, Clone, Error, Serialize, Deserialize)]
pub enum QuantizationError {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuantizationOptions {
    Trivial(TrivialQuantizationOptions),
    Scalar(ScalarQuantizationOptions),
    Product(ProductQuantizationOptions),
}

impl Default for QuantizationOptions {
    fn default() -> Self {
        Self::Trivial(TrivialQuantizationOptions {})
    }
}

impl QuantizationOptions {
    fn unwrap_trivial_quantization(self) -> TrivialQuantizationOptions {
        match self {
            Self::Trivial(x) => x,
            _ => unreachable!(),
        }
    }
    fn unwrap_scalar_quantization(self) -> ScalarQuantizationOptions {
        match self {
            Self::Scalar(x) => x,
            _ => unreachable!(),
        }
    }
    fn unwrap_product_quantization(self) -> ProductQuantizationOptions {
        match self {
            Self::Product(x) => x,
            _ => unreachable!(),
        }
    }
    pub fn is_product_quantization(&self) -> bool {
        match self {
            Self::Product(_) => true,
            _ => false,
        }
    }
}

pub trait Quan {
    fn prebuild(
        storage: &mut StoragePreallocator,
        index_options: IndexOptions,
        quantization_options: QuantizationOptions,
    );
    fn build(
        storage: &mut Storage,
        index_options: IndexOptions,
        quantization_options: QuantizationOptions,
        vectors: Arc<Vectors>,
    ) -> Self;
    fn load(
        storage: &mut Storage,
        index_options: IndexOptions,
        quantization_options: QuantizationOptions,
        vectors: Arc<Vectors>,
    ) -> Self;
    fn insert(&self, i: usize, point: &[Scalar]) -> Result<(), QuantizationError>;
    fn distance(&self, d: Distance, lhs: &[Scalar], rhs: usize) -> Scalar;
    fn distance2(&self, d: Distance, lhs: usize, rhs: usize) -> Scalar;
}

pub enum Quantization {
    Trivial(TrivialQuantization),
    Scalar(ScalarQuantization),
    Product(ProductQuantization),
}

impl Quan for Quantization {
    fn prebuild(
        storage: &mut StoragePreallocator,
        index_options: IndexOptions,
        quantization_options: QuantizationOptions,
    ) {
        match quantization_options {
            quantization_options @ QuantizationOptions::Trivial(_) => {
                TrivialQuantization::prebuild(storage, index_options, quantization_options)
            }
            quantization_options @ QuantizationOptions::Scalar(_) => {
                ScalarQuantization::prebuild(storage, index_options, quantization_options)
            }
            quantization_options @ QuantizationOptions::Product(_) => {
                ProductQuantization::prebuild(storage, index_options, quantization_options)
            }
        }
    }

    fn build(
        storage: &mut Storage,
        index_options: IndexOptions,
        quantization_options: QuantizationOptions,
        vectors: Arc<Vectors>,
    ) -> Self {
        match quantization_options {
            quantization_options @ QuantizationOptions::Trivial(_) => Self::Trivial(
                TrivialQuantization::build(storage, index_options, quantization_options, vectors),
            ),
            quantization_options @ QuantizationOptions::Scalar(_) => Self::Scalar(
                ScalarQuantization::build(storage, index_options, quantization_options, vectors),
            ),
            quantization_options @ QuantizationOptions::Product(_) => Self::Product(
                ProductQuantization::build(storage, index_options, quantization_options, vectors),
            ),
        }
    }

    fn load(
        storage: &mut Storage,
        index_options: IndexOptions,
        quantization_options: QuantizationOptions,
        vectors: Arc<Vectors>,
    ) -> Self {
        match quantization_options {
            quantization_options @ QuantizationOptions::Trivial(_) => Self::Trivial(
                TrivialQuantization::load(storage, index_options, quantization_options, vectors),
            ),
            quantization_options @ QuantizationOptions::Scalar(_) => Self::Scalar(
                ScalarQuantization::load(storage, index_options, quantization_options, vectors),
            ),
            quantization_options @ QuantizationOptions::Product(_) => Self::Product(
                ProductQuantization::load(storage, index_options, quantization_options, vectors),
            ),
        }
    }

    fn insert(&self, i: usize, point: &[Scalar]) -> Result<(), QuantizationError> {
        use Quantization::*;
        match self {
            Trivial(x) => x.insert(i, point),
            Scalar(x) => x.insert(i, point),
            Product(x) => x.insert(i, point),
        }
    }

    fn distance(&self, d: Distance, lhs: &[Scalar], rhs: usize) -> Scalar {
        use Quantization::*;
        match self {
            Trivial(x) => x.distance(d, lhs, rhs),
            Scalar(x) => x.distance(d, lhs, rhs),
            Product(x) => x.distance(d, lhs, rhs),
        }
    }

    fn distance2(&self, d: Distance, lhs: usize, rhs: usize) -> Scalar {
        use Quantization::*;
        match self {
            Trivial(x) => x.distance2(d, lhs, rhs),
            Scalar(x) => x.distance2(d, lhs, rhs),
            Product(x) => x.distance2(d, lhs, rhs),
        }
    }
}
