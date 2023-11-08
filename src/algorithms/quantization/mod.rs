pub mod product;
pub mod scalar;
pub mod trivial;

use self::product::{ProductQuantization, ProductQuantizationOptions};
use self::scalar::{ScalarQuantization, ScalarQuantizationOptions};
use self::trivial::{TrivialQuantization, TrivialQuantizationOptions};
use super::raw::Raw;
use crate::index::IndexOptions;
use crate::prelude::*;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;
use validator::Validate;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuantizationOptions {
    Trivial(TrivialQuantizationOptions),
    Scalar(ScalarQuantizationOptions),
    Product(ProductQuantizationOptions),
}

impl Validate for QuantizationOptions {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            Self::Trivial(x) => x.validate(),
            Self::Scalar(x) => x.validate(),
            Self::Product(x) => x.validate(),
        }
    }
}

impl Default for QuantizationOptions {
    fn default() -> Self {
        Self::Trivial(Default::default())
    }
}

impl QuantizationOptions {
    fn _unwrap_scalar_quantization(self) -> ScalarQuantizationOptions {
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
    fn create(
        path: PathBuf,
        options: IndexOptions,
        quantization_options: QuantizationOptions,
        raw: &Arc<Raw>,
    ) -> Self;
    fn open(
        path: PathBuf,
        options: IndexOptions,
        quantization_options: QuantizationOptions,
        raw: &Arc<Raw>,
    ) -> Self;
    fn distance(&self, d: Distance, lhs: &[Scalar], rhs: u32) -> Scalar;
    fn distance2(&self, d: Distance, lhs: u32, rhs: u32) -> Scalar;
}

pub enum Quantization {
    Trivial(TrivialQuantization),
    Scalar(ScalarQuantization),
    Product(ProductQuantization),
}

impl Quantization {
    pub fn create(
        path: PathBuf,
        options: IndexOptions,
        quantization_options: QuantizationOptions,
        raw: &Arc<Raw>,
    ) -> Self {
        match quantization_options {
            QuantizationOptions::Trivial(_) => Self::Trivial(TrivialQuantization::create(
                path,
                options,
                quantization_options,
                raw,
            )),
            QuantizationOptions::Scalar(_) => Self::Scalar(ScalarQuantization::create(
                path,
                options,
                quantization_options,
                raw,
            )),
            QuantizationOptions::Product(_) => Self::Product(ProductQuantization::create(
                path,
                options,
                quantization_options,
                raw,
            )),
        }
    }

    pub fn open(
        path: PathBuf,
        options: IndexOptions,
        quantization_options: QuantizationOptions,
        raw: &Arc<Raw>,
    ) -> Self {
        match quantization_options {
            QuantizationOptions::Trivial(_) => Self::Trivial(TrivialQuantization::open(
                path,
                options,
                quantization_options,
                raw,
            )),
            QuantizationOptions::Scalar(_) => Self::Scalar(ScalarQuantization::open(
                path,
                options,
                quantization_options,
                raw,
            )),
            QuantizationOptions::Product(_) => Self::Product(ProductQuantization::open(
                path,
                options,
                quantization_options,
                raw,
            )),
        }
    }

    pub fn distance(&self, d: Distance, lhs: &[Scalar], rhs: u32) -> Scalar {
        use Quantization::*;
        match self {
            Trivial(x) => x.distance(d, lhs, rhs),
            Scalar(x) => x.distance(d, lhs, rhs),
            Product(x) => x.distance(d, lhs, rhs),
        }
    }

    pub fn distance2(&self, d: Distance, lhs: u32, rhs: u32) -> Scalar {
        use Quantization::*;
        match self {
            Trivial(x) => x.distance2(d, lhs, rhs),
            Scalar(x) => x.distance2(d, lhs, rhs),
            Product(x) => x.distance2(d, lhs, rhs),
        }
    }
}
