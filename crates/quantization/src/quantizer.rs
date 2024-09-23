use base::distance::Distance;
use base::index::{QuantizationOptions, SearchOptions, VectorOptions};
use base::operator::Borrowed;
use base::operator::Operator;
use base::search::{RerankerPop, RerankerPush, Vectors};
use serde::{Deserialize, Serialize};
use std::ops::Range;

pub trait Quantizer<O: Operator>:
    Serialize + for<'a> Deserialize<'a> + Send + Sync + 'static
{
    fn train(
        vector_options: VectorOptions,
        options: Option<QuantizationOptions>,
        vectors: &(impl Vectors<O::Vector> + Sync),
        transform: impl Fn(Borrowed<'_, O>) -> O::Vector + Copy + Sync,
    ) -> Self;

    fn encode(&self, vector: Borrowed<'_, O>) -> Vec<u8>;
    fn fscan_encode(&self, vectors: [O::Vector; 32]) -> Vec<u8>;
    fn code_size(&self) -> u32;
    fn fcode_size(&self) -> u32;

    fn project(&self, vector: Borrowed<'_, O>) -> O::Vector;

    type Lut;
    fn preprocess(&self, vector: Borrowed<'_, O>) -> Self::Lut;
    fn process(&self, lut: &Self::Lut, code: &[u8], vector: Borrowed<'_, O>) -> Distance;

    type FLut;
    fn fscan_preprocess(&self, vector: Borrowed<'_, O>) -> Self::FLut;
    fn fscan_process(&self, flut: &Self::FLut, code: &[u8]) -> [Distance; 32];

    type FlatRerankVec;

    fn flat_rerank_start() -> Self::FlatRerankVec;

    fn flat_rerank_preprocess(
        &self,
        vector: Borrowed<'_, O>,
        opts: &SearchOptions,
    ) -> Result<Self::FLut, Self::Lut>;

    fn flat_rerank_continue<C>(
        &self,
        locate_0: impl Fn(u32) -> C,
        locate_1: impl Fn(u32) -> C,
        frlut: &Result<Self::FLut, Self::Lut>,
        range: Range<u32>,
        heap: &mut Self::FlatRerankVec,
    ) where
        C: AsRef<[u8]>;

    fn flat_rerank_break<'a, T: 'a, R>(
        &'a self,
        heap: Self::FlatRerankVec,
        rerank: R,
        opts: &SearchOptions,
    ) -> impl RerankerPop<T> + 'a
    where
        R: Fn(u32) -> (Distance, T) + 'a;

    fn graph_rerank<'a, T, R, C>(
        &'a self,
        lut: Self::Lut,
        locate: impl Fn(u32) -> C + 'a,
        rerank: R,
    ) -> impl RerankerPush + RerankerPop<T> + 'a
    where
        T: 'a,
        R: Fn(u32) -> (Distance, T) + 'a,
        C: AsRef<[u8]>;
}
