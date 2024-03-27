#![feature(doc_cfg)]
#![feature(avx512_target_feature)]

pub mod operator;
pub mod product;
pub mod scalar;
pub mod trivial;

use self::product::ProductQuantization;
use self::scalar::ScalarQuantization;
use self::trivial::TrivialQuantization;
use crate::operator::OperatorQuantization;
use base::index::*;
use base::operator::*;
use base::scalar::*;
use base::search::*;
use std::path::Path;
use std::sync::Arc;

pub enum Quantization<O: OperatorQuantization, C: Collection<O>> {
    Trivial(TrivialQuantization<O, C>),
    Scalar(ScalarQuantization<O, C>),
    Product(ProductQuantization<O, C>),
}

impl<O: OperatorQuantization, C: Collection<O>> Quantization<O, C> {
    pub fn create(
        path: &Path,
        options: IndexOptions,
        quantization_options: QuantizationOptions,
        collection: &Arc<C>,
        permutation: Vec<u32>, // permutation is the mapping from placements to original ids
    ) -> Self {
        match quantization_options {
            QuantizationOptions::Trivial(_) => Self::Trivial(TrivialQuantization::create(
                path,
                options,
                quantization_options,
                collection,
                permutation,
            )),
            QuantizationOptions::Scalar(_) => Self::Scalar(ScalarQuantization::create(
                path,
                options,
                quantization_options,
                collection,
                permutation,
            )),
            QuantizationOptions::Product(_) => Self::Product(ProductQuantization::create(
                path,
                options,
                quantization_options,
                collection,
                permutation,
            )),
        }
    }

    pub fn open(
        path: &Path,
        options: IndexOptions,
        quantization_options: QuantizationOptions,
        collection: &Arc<C>,
    ) -> Self {
        match quantization_options {
            QuantizationOptions::Trivial(_) => Self::Trivial(TrivialQuantization::open(
                path,
                options,
                quantization_options,
                collection,
            )),
            QuantizationOptions::Scalar(_) => Self::Scalar(ScalarQuantization::open(
                path,
                options,
                quantization_options,
                collection,
            )),
            QuantizationOptions::Product(_) => Self::Product(ProductQuantization::open(
                path,
                options,
                quantization_options,
                collection,
            )),
        }
    }

    pub fn distance(&self, lhs: Borrowed<'_, O>, rhs: u32) -> F32 {
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
