pub mod product;
pub mod scalar;
pub mod trivial;

use self::product::ProductQuantization;
use self::scalar::ScalarQuantization;
use self::trivial::TrivialQuantization;
use super::raw::Raw;
use crate::prelude::*;
use std::path::Path;
use std::sync::Arc;

pub trait Quan<S: G> {
    fn create(
        path: &Path,
        options: IndexOptions,
        quantization_options: QuantizationOptions,
        raw: &Arc<Raw<S>>,
        permutation: Vec<u32>,
    ) -> Self;
    fn open2(
        path: &Path,
        options: IndexOptions,
        quantization_options: QuantizationOptions,
        raw: &Arc<Raw<S>>,
    ) -> Self;
    fn distance(&self, lhs: Borrowed<'_, S>, rhs: u32) -> F32;
    fn distance2(&self, lhs: u32, rhs: u32) -> F32;
}

pub enum Quantization<S: G> {
    Trivial(TrivialQuantization<S>),
    Scalar(ScalarQuantization<S>),
    Product(ProductQuantization<S>),
}

impl<S: G> Quantization<S> {
    pub fn create(
        path: &Path,
        options: IndexOptions,
        quantization_options: QuantizationOptions,
        raw: &Arc<Raw<S>>,
        permutation: Vec<u32>, // permutation is the mapping from placements to original ids
    ) -> Self {
        match quantization_options {
            QuantizationOptions::Trivial(_) => Self::Trivial(TrivialQuantization::create(
                path,
                options,
                quantization_options,
                raw,
                permutation,
            )),
            QuantizationOptions::Scalar(_) => Self::Scalar(ScalarQuantization::create(
                path,
                options,
                quantization_options,
                raw,
                permutation,
            )),
            QuantizationOptions::Product(_) => Self::Product(ProductQuantization::create(
                path,
                options,
                quantization_options,
                raw,
                permutation,
            )),
        }
    }

    pub fn open(
        path: &Path,
        options: IndexOptions,
        quantization_options: QuantizationOptions,
        raw: &Arc<Raw<S>>,
    ) -> Self {
        match quantization_options {
            QuantizationOptions::Trivial(_) => Self::Trivial(TrivialQuantization::open2(
                path,
                options,
                quantization_options,
                raw,
            )),
            QuantizationOptions::Scalar(_) => Self::Scalar(ScalarQuantization::open2(
                path,
                options,
                quantization_options,
                raw,
            )),
            QuantizationOptions::Product(_) => Self::Product(ProductQuantization::open2(
                path,
                options,
                quantization_options,
                raw,
            )),
        }
    }

    pub fn distance(&self, lhs: Borrowed<'_, S>, rhs: u32) -> F32 {
        use Quantization::*;
        match self {
            Trivial(x) => x.distance(lhs, rhs),
            Scalar(x) => x.distance(lhs, rhs),
            Product(x) => x.distance(lhs, rhs),
        }
    }

    pub fn distance2(&self, lhs: u32, rhs: u32) -> F32 {
        use Quantization::*;
        match self {
            Trivial(x) => x.distance2(lhs, rhs),
            Scalar(x) => x.distance2(lhs, rhs),
            Product(x) => x.distance2(lhs, rhs),
        }
    }
}
