pub mod operator;

use self::operator::OperatorTrivialQuantization;
use crate::reranker::flat::DisabledFlatReranker;
use crate::reranker::graph::GraphReranker;
use base::always_equal::AlwaysEqual;
use base::distance::Distance;
use base::index::*;
use base::operator::*;
use base::search::*;
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
        _: &impl Vectors<Owned<O>>,
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
    ) -> Distance {
        O::trivial_quantization_process(preprocessed, rhs)
    }

    pub fn push_batch(
        &self,
        _preprocessed: &O::TrivialQuantizationPreprocessed,
        rhs: Range<u32>,
        heap: &mut Vec<(Reverse<Distance>, AlwaysEqual<u32>)>,
    ) {
        heap.extend(rhs.map(|u| (Reverse(Distance::ZERO), AlwaysEqual(u))));
    }

    pub fn flat_rerank<'a, T: 'a>(
        &'a self,
        heap: Vec<(Reverse<Distance>, AlwaysEqual<u32>)>,
        r: impl Fn(u32) -> (Distance, T) + 'a,
    ) -> impl RerankerPop<T> + 'a {
        DisabledFlatReranker::new(heap, r)
    }

    pub fn graph_rerank<'a, T: 'a, R: Fn(u32) -> (Distance, T) + 'a>(
        &'a self,
        _: Borrowed<'a, O>,
        r: R,
    ) -> GraphReranker<T, R> {
        GraphReranker::new(None, r)
    }
}
