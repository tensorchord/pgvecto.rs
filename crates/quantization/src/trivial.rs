use crate::quantizer::Quantizer;
use crate::reranker::graph::GraphReranker;
use base::always_equal::AlwaysEqual;
use base::distance::Distance;
use base::index::*;
use base::operator::*;
use base::search::*;
use base::vector::VectorBorrowed;
use base::vector::VectorOwned;
use serde::Deserialize;
use serde::Serialize;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::marker::PhantomData;
use std::ops::Range;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct TrivialQuantizer<O: Operator> {
    _maker: PhantomData<O>,
}

impl<O: Operator> Quantizer<O> for TrivialQuantizer<O> {
    fn train(
        _: VectorOptions,
        _: Option<QuantizationOptions>,
        _: &impl Vectors<Owned<O>>,
        _: impl Fn(Borrowed<'_, O>) -> Owned<O> + Copy,
    ) -> Self {
        Self {
            _maker: PhantomData,
        }
    }

    fn encode(&self, _: Borrowed<'_, O>) -> Vec<u8> {
        Vec::new()
    }

    fn fscan_encode(&self, _: [Owned<O>; 32]) -> Vec<u8> {
        Vec::new()
    }

    fn code_size(&self) -> u32 {
        0
    }

    fn fcode_size(&self) -> u32 {
        0
    }

    type Lut = Owned<O>;

    fn preprocess(&self, vector: Borrowed<'_, O>) -> Self::Lut {
        vector.own()
    }

    fn process(&self, lut: &Self::Lut, _: &[u8], vector: Borrowed<'_, O>) -> Distance {
        O::distance(lut.as_borrowed(), vector)
    }

    type FLut = std::convert::Infallible;

    fn fscan_preprocess(&self, _: Borrowed<'_, O>) -> Self::FLut {
        unimplemented!()
    }

    fn fscan_process(_: &Self::FLut, _: &[u8]) -> [Distance; 32] {
        unimplemented!()
    }

    type FlatRerankVec = Vec<u32>;

    fn flat_rerank_start() -> Self::FlatRerankVec {
        Vec::new()
    }

    fn flat_rerank_preprocess(
        &self,
        vector: Borrowed<'_, O>,
        _: &SearchOptions,
    ) -> Result<Self::FLut, Self::Lut> {
        Err(self.preprocess(vector))
    }

    fn flat_rerank_continue<C>(
        &self,
        _: impl Fn(u32) -> C,
        _: impl Fn(u32) -> C,
        _: &Result<Self::FLut, Self::Lut>,
        range: Range<u32>,
        heap: &mut Vec<u32>,
    ) where
        C: AsRef<[u8]>,
    {
        heap.extend(range);
    }

    fn flat_rerank_break<'a, T: 'a, R>(
        &'a self,
        heap: Vec<u32>,
        rerank: R,
        _: &SearchOptions,
    ) -> impl RerankerPop<T> + 'a
    where
        R: Fn(u32) -> (Distance, T) + 'a,
    {
        heap.into_iter()
            .map(|u| {
                let (dis_u, pay_u) = rerank(u);
                (Reverse(dis_u), AlwaysEqual(u), AlwaysEqual(pay_u))
            })
            .collect::<BinaryHeap<(Reverse<Distance>, AlwaysEqual<u32>, AlwaysEqual<T>)>>()
    }

    fn graph_rerank<'a, T, R, C>(
        &'a self,
        _: impl Fn(u32) -> C + 'a,
        _: Borrowed<'a, O>,
        rerank: R,
    ) -> impl RerankerPush + RerankerPop<T> + 'a
    where
        T: 'a,
        R: Fn(u32) -> (Distance, T) + 'a,
        C: AsRef<[u8]>,
    {
        GraphReranker::new(rerank)
    }
}
