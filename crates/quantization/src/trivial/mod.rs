pub mod operator;

use self::operator::OperatorTrivialQuantization;
use crate::reranker::disabled::DisabledFlatReranker;
use crate::reranker::disabled::DisabledGraphReranker;
use base::index::*;
use base::operator::*;
use base::scalar::*;
use base::search::*;
use num_traits::Zero;
use serde::Deserialize;
use serde::Serialize;
use std::cmp::Reverse;
use std::marker::PhantomData;
use std::ops::Range;

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

    pub fn push_batch(
        &self,
        _preprocessed: &O::TrivialQuantizationPreprocessed,
        rhs: Range<u32>,
        heap: &mut Vec<(Reverse<F32>, u32)>,
    ) {
        heap.extend(rhs.map(|u| (Reverse(F32::zero()), u)));
    }

    pub fn flat_rerank<'a, T: 'a>(
        &'a self,
        heap: Vec<(Reverse<F32>, u32)>,
        r: impl Fn(u32) -> (F32, T) + 'a,
    ) -> impl RerankerPop<T> + 'a {
        DisabledFlatReranker::new(heap, r)
    }

    pub fn graph_rerank<'a, T: 'a, R: Fn(u32) -> (F32, T) + 'a>(
        &'a self,
        _: Borrowed<'a, O>,
        r: R,
    ) -> impl RerankerPop<T> + RerankerPush + 'a {
        DisabledGraphReranker::new(r)
    }
}
