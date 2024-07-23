pub mod operator;

use self::operator::OperatorTrivialQuantization;
use crate::reranker::disabled::DisabledReranker;
use base::index::*;
use base::operator::*;
use base::scalar::*;
use base::search::*;
use serde::Deserialize;
use serde::Serialize;
use std::marker::PhantomData;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct TrivialQuantizer<O: OperatorTrivialQuantization> {
    dims: u32,
    _maker: PhantomData<O>,
}

impl<O: OperatorTrivialQuantization> TrivialQuantizer<O> {
    pub fn train(
        vector_options: VectorOptions,
        _: TrivialQuantizationOptions,
        _: &impl Vectors<O>,
        _: impl Fn(Borrowed<'_, O>) -> Owned<O> + Copy,
    ) -> Self {
        Self {
            dims: vector_options.dims,
            _maker: PhantomData,
        }
    }

    pub fn preprocess(&self, lhs: Borrowed<'_, O>) -> O::TrivialQuantizationPreprocessed {
        O::trivial_quantization_preprocess(lhs)
    }

    pub fn process(
        &self,
        preprocessed: &O::TrivialQuantizationPreprocessed,
        rhs: Borrowed<'_, O>,
    ) -> F32 {
        O::trivial_quantization_process(preprocessed, rhs)
    }

    pub fn flat_rerank<'a, T: 'a>(
        &'a self,
        _: Borrowed<'a, O>,
        _: &'a SearchOptions,
        r: impl Fn(u32) -> (F32, T) + 'a,
    ) -> Box<dyn Reranker<T> + 'a> {
        Box::new(DisabledReranker::new(r))
    }

    pub fn ivf_naive_rerank<'a, T: 'a>(
        &'a self,
        _: Borrowed<'a, O>,
        _: &'a SearchOptions,
        r: impl Fn(u32) -> (F32, T) + 'a,
    ) -> Box<dyn Reranker<T> + 'a> {
        Box::new(DisabledReranker::new(r))
    }

    pub fn ivf_residual_rerank<'a, T: 'a>(
        &'a self,
        _: Vec<Owned<O>>,
        _: &'a SearchOptions,
        r: impl Fn(u32) -> (F32, T) + 'a,
    ) -> Box<dyn Reranker<T, usize> + 'a> {
        Box::new(DisabledReranker::new(r))
    }

    pub fn graph_rerank<'a, T: 'a>(
        &'a self,
        _: Borrowed<'a, O>,
        _: &'a SearchOptions,
        r: impl Fn(u32) -> (F32, T) + 'a,
    ) -> Box<dyn Reranker<T> + 'a> {
        Box::new(DisabledReranker::new(r))
    }
}
