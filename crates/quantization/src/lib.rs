#![feature(avx512_target_feature)]

pub mod operator;
pub mod product;
pub mod scalar;

use self::product::ProductQuantizer;
use self::scalar::ScalarQuantizer;
use crate::operator::OperatorQuantization;
use base::index::*;
use base::operator::*;
use base::scalar::*;
use base::search::*;
use base::vector::*;
use common::dir_ops::sync_dir;
use common::json::Json;
use common::mmap_array::MmapArray;
use serde::Deserialize;
use serde::Serialize;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound = "")]
pub enum Quantizer<O: OperatorQuantization> {
    Trivial,
    Scalar(ScalarQuantizer<O>),
    Product(ProductQuantizer<O>),
}

impl<O: OperatorQuantization> Quantizer<O> {
    pub fn train(
        options: IndexOptions,
        quantization_options: QuantizationOptions,
        vectors: &impl Vectors<O>,
    ) -> Self {
        use QuantizationOptions::*;
        match quantization_options {
            Trivial(_) => Self::Trivial,
            Scalar(_) => Self::Scalar(ScalarQuantizer::train(options, vectors)),
            Product(product) => Self::Product(ProductQuantizer::train(options, product, vectors)),
        }
    }

    pub fn width(&self) -> usize {
        use Quantizer::*;
        match self {
            Trivial => 0,
            Scalar(x) => x.width(),
            Product(x) => x.width(),
        }
    }

    pub fn encode(&self, vector: &[Scalar<O>]) -> Vec<u8> {
        use Quantizer::*;
        match self {
            Trivial => Vec::new(),
            Scalar(x) => x.encode(vector),
            Product(x) => x.encode(vector),
        }
    }

    pub fn distance(&self, fallback: impl Fn() -> F32, lhs: Borrowed<'_, O>, rhs: &[u8]) -> F32 {
        use Quantizer::*;
        match self {
            Trivial => fallback(),
            Scalar(x) => x.distance(lhs, rhs),
            Product(x) => x.distance(lhs, rhs),
        }
    }
}

pub struct Quantization<O: OperatorQuantization> {
    train: Json<Quantizer<O>>,
    codes: MmapArray<u8>,
}

impl<O: OperatorQuantization> Quantization<O> {
    pub fn create(
        path: impl AsRef<Path>,
        options: IndexOptions,
        quantization_options: QuantizationOptions,
        vectors: &impl Vectors<O>,
    ) -> Self {
        std::fs::create_dir(path.as_ref()).unwrap();
        let train = Quantizer::train(options, quantization_options, vectors);
        let train = Json::create(path.as_ref().join("train"), train);
        let codes = MmapArray::create(
            path.as_ref().join("codes"),
            (0..vectors.len()).flat_map(|i| train.encode(&vectors.vector(i).to_vec()).into_iter()),
        );
        sync_dir(path);
        Self { train, codes }
    }

    pub fn open(path: impl AsRef<Path>) -> Self {
        let train = Json::open(path.as_ref().join("train"));
        let codes = MmapArray::open(path.as_ref().join("codes"));
        Self { train, codes }
    }

    pub fn distance(&self, vectors: &impl Vectors<O>, lhs: Borrowed<'_, O>, rhs: u32) -> F32 {
        let width = self.train.width();
        let start = rhs as usize * width;
        let end = start + width;
        self.train.distance(
            || O::distance(lhs, vectors.vector(rhs)),
            lhs,
            &self.codes[start..end],
        )
    }
}
