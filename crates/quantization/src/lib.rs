#![allow(clippy::needless_range_loop)]

pub mod operator;
pub mod product;
pub mod rabitq;
pub mod reranker;
pub mod scalar;
pub mod trivial;

use self::product::ProductQuantizer;
use self::rabitq::RaBitQuantizer;
use self::scalar::ScalarQuantizer;
use crate::operator::OperatorQuantization;
use base::index::*;
use base::operator::*;
use base::scalar::*;
use base::search::*;
use base::vector::*;
use common::json::Json;
use common::mmap_array::MmapArray;
use serde::Deserialize;
use serde::Serialize;
use std::path::Path;
use trivial::TrivialQuantizer;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound = "")]
pub enum Quantizer<O: OperatorQuantization> {
    Trivial(TrivialQuantizer<O>),
    Scalar(ScalarQuantizer<O>),
    Product(ProductQuantizer<O>),
    RaBitQ(RaBitQuantizer<O>),
}

impl<O: OperatorQuantization> Quantizer<O> {
    pub fn train(
        vector_options: VectorOptions,
        quantization_options: QuantizationOptions,
        vectors: &impl Vectors<O>,
        transform: impl Fn(Borrowed<'_, O>) -> Owned<O> + Copy + Send + Sync,
    ) -> Self {
        use QuantizationOptions::*;
        match quantization_options {
            Trivial(trivial_quantization_options) => Self::Trivial(TrivialQuantizer::train(
                vector_options,
                trivial_quantization_options,
                vectors,
                transform,
            )),
            Scalar(scalar_quantization_options) => Self::Scalar(ScalarQuantizer::train(
                vector_options,
                scalar_quantization_options,
                vectors,
                transform,
            )),
            Product(product_quantization_options) => Self::Product(ProductQuantizer::train(
                vector_options,
                product_quantization_options,
                vectors,
                transform,
            )),
            RaBitQ(rabitq_quantization_options) => Self::RaBitQ(RaBitQuantizer::train(
                vector_options,
                rabitq_quantization_options,
                vectors,
                transform,
            )),
        }
    }
}

pub enum QuantizationPreprocessed<O: OperatorQuantization> {
    Trivial(O::TrivialQuantizationPreprocessed),
    Scalar(O::ScalarQuantizationPreprocessed),
    Product(O::ProductQuantizationPreprocessed),
    Rabit(O::RabitQuantizationPreprocessed),
}

pub struct Quantization<O: OperatorQuantization> {
    train: Json<Quantizer<O>>,
    codes: MmapArray<u8>,
}

impl<O: OperatorQuantization> Quantization<O> {
    pub fn create(
        path: impl AsRef<Path>,
        vector_options: VectorOptions,
        quantization_options: QuantizationOptions,
        vectors: &impl Vectors<O>,
        transform: impl Fn(Borrowed<'_, O>) -> Owned<O> + Copy + Send + Sync,
    ) -> Self {
        use Quantizer::*;
        std::fs::create_dir(path.as_ref()).unwrap();
        let train = Quantizer::train(vector_options, quantization_options, vectors, transform);
        let train = Json::create(path.as_ref().join("train"), train);
        let codes = MmapArray::create(
            path.as_ref().join("codes"),
            match &*train {
                Trivial(_) => Box::new(std::iter::empty()) as Box<dyn Iterator<Item = u8>>,
                Scalar(x) => Box::new(
                    (0..vectors.len())
                        .flat_map(|i| x.encode(&vectors.vector(i).to_vec()).into_iter()),
                ),
                Product(x) => Box::new(
                    (0..vectors.len())
                        .flat_map(|i| x.encode(&vectors.vector(i).to_vec()).into_iter()),
                ),
                RaBitQ(_) => Box::new(std::iter::empty()) as Box<dyn Iterator<Item = u8>>,
            },
        );
        Self { train, codes }
    }

    pub fn open(path: impl AsRef<Path>) -> Self {
        let train = Json::open(path.as_ref().join("train"));
        let codes = MmapArray::open(path.as_ref().join("codes"));
        Self { train, codes }
    }

    pub fn preprocess(&self, lhs: Borrowed<'_, O>) -> QuantizationPreprocessed<O> {
        match &*self.train {
            Quantizer::Trivial(x) => QuantizationPreprocessed::Trivial(x.preprocess(lhs)),
            Quantizer::Scalar(x) => QuantizationPreprocessed::Scalar(x.preprocess(lhs)),
            Quantizer::Product(x) => QuantizationPreprocessed::Product(x.preprocess(lhs)),
            Quantizer::RaBitQ(x) => QuantizationPreprocessed::Rabit(x.preprocess(lhs)),
        }
    }

    pub fn process(
        &self,
        vectors: &impl Vectors<O>,
        preprocessed: &QuantizationPreprocessed<O>,
        rhs: u32,
    ) -> F32 {
        match (&*self.train, preprocessed) {
            (Quantizer::Trivial(x), QuantizationPreprocessed::Trivial(lhs)) => {
                let rhs = vectors.vector(rhs);
                x.process(lhs, rhs)
            }
            (Quantizer::Scalar(x), QuantizationPreprocessed::Scalar(lhs)) => {
                let bytes = x.bytes() as usize;
                let start = rhs as usize * bytes;
                let end = start + bytes;
                let rhs = &self.codes[start..end];
                x.process(lhs, rhs)
            }
            (Quantizer::Product(x), QuantizationPreprocessed::Product(lhs)) => {
                let bytes = x.bytes() as usize;
                let start = rhs as usize * bytes;
                let end = start + bytes;
                let rhs = &self.codes[start..end];
                x.process(lhs, rhs)
            }
            _ => unreachable!(),
        }
    }

    pub fn flat_rerank<'a, T: 'a>(
        &'a self,
        vector: Borrowed<'a, O>,
        opts: &'a SearchOptions,
        r: impl Fn(u32) -> (F32, T) + 'a,
    ) -> Box<dyn Reranker<T> + 'a> {
        use Quantizer::*;
        match &*self.train {
            Trivial(x) => x.flat_rerank(vector, opts, r),
            Scalar(x) => x.flat_rerank(
                vector,
                opts,
                |u| {
                    let bytes = x.bytes() as usize;
                    let start = u as usize * bytes;
                    let end = start + bytes;
                    &self.codes[start..end]
                },
                r,
            ),
            Product(x) => x.flat_rerank(
                vector,
                opts,
                |u| {
                    let bytes = x.bytes() as usize;
                    let start = u as usize * bytes;
                    let end = start + bytes;
                    &self.codes[start..end]
                },
                r,
            ),
            RaBitQ(x) => x.flat_rerank(vector, opts, r),
        }
    }

    pub fn ivf_naive_rerank<'a, T: 'a>(
        &'a self,
        vector: Borrowed<'a, O>,
        opts: &'a SearchOptions,
        r: impl Fn(u32) -> (F32, T) + 'a,
    ) -> Box<dyn Reranker<T> + 'a> {
        use Quantizer::*;
        match &*self.train {
            Trivial(x) => x.ivf_naive_rerank(vector, opts, r),
            Scalar(x) => x.ivf_naive_rerank(
                vector,
                opts,
                |u| {
                    let bytes = x.bytes() as usize;
                    let start = u as usize * bytes;
                    let end = start + bytes;
                    &self.codes[start..end]
                },
                r,
            ),
            Product(x) => x.ivf_naive_rerank(
                vector,
                opts,
                |u| {
                    let bytes = x.bytes() as usize;
                    let start = u as usize * bytes;
                    let end = start + bytes;
                    &self.codes[start..end]
                },
                r,
            ),
            RaBitQ(x) => x.ivf_naive_rerank(vector, opts, r),
        }
    }

    pub fn ivf_residual_rerank<'a, T: 'a>(
        &'a self,
        vectors: Vec<Owned<O>>,
        opts: &'a SearchOptions,
        r: impl Fn(u32) -> (F32, T) + 'a,
    ) -> Box<dyn Reranker<T, usize> + 'a> {
        use Quantizer::*;
        match &*self.train {
            Trivial(x) => x.ivf_residual_rerank(vectors, opts, r),
            Scalar(x) => x.ivf_residual_rerank(
                vectors,
                opts,
                |u| {
                    let bytes = x.bytes() as usize;
                    let start = u as usize * bytes;
                    let end = start + bytes;
                    &self.codes[start..end]
                },
                r,
            ),
            Product(x) => x.ivf_residual_rerank(
                vectors,
                opts,
                |u| {
                    let bytes = x.bytes() as usize;
                    let start = u as usize * bytes;
                    let end = start + bytes;
                    &self.codes[start..end]
                },
                r,
            ),
            RaBitQ(x) => x.ivf_residual_rerank(vectors, opts, r),
        }
    }

    pub fn graph_rerank<'a, T: 'a>(
        &'a self,
        vector: Borrowed<'a, O>,
        opts: &'a SearchOptions,
        r: impl Fn(u32) -> (F32, T) + 'a,
    ) -> Box<dyn Reranker<T> + 'a> {
        use Quantizer::*;
        match &*self.train {
            Trivial(x) => x.graph_rerank(vector, opts, r),
            Scalar(x) => x.graph_rerank(
                vector,
                opts,
                |u| {
                    let bytes = x.bytes() as usize;
                    let start = u as usize * bytes;
                    let end = start + bytes;
                    &self.codes[start..end]
                },
                r,
            ),
            Product(x) => x.graph_rerank(
                vector,
                opts,
                |u| {
                    let bytes = x.bytes() as usize;
                    let start = u as usize * bytes;
                    let end = start + bytes;
                    &self.codes[start..end]
                },
                r,
            ),
            RaBitQ(x) => x.graph_rerank(vector, opts, r),
        }
    }
}
