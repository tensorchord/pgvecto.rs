pub mod operator;

use self::operator::OperatorProductQuantization;
use crate::reranker::flat::WindowFlatReranker;
use crate::reranker::graph::GraphReranker;
use base::always_equal::AlwaysEqual;
use base::index::*;
use base::operator::*;
use base::scalar::*;
use base::search::*;
use base::vector::VectorBorrowed;
use common::sample::sample_subvector_transform;
use common::vec2::Vec2;
use k_means::k_means;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use serde::Deserialize;
use serde::Serialize;
use std::cmp::Reverse;
use std::ops::Range;
use stoppable_rayon as rayon;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct ProductQuantizer<O: OperatorProductQuantization> {
    dims: u32,
    ratio: u32,
    bits: u32,
    originals: Vec<Vec2<Scalar<O>>>,
    centroids: Vec2<Scalar<O>>,
}

impl<O: OperatorProductQuantization> ProductQuantizer<O> {
    pub fn train(
        vector_options: VectorOptions,
        product_quantization_options: ProductQuantizationOptions,
        vectors: &impl Vectors<O>,
        transform: impl Fn(Borrowed<'_, O>) -> Owned<O> + Copy + Send + Sync,
    ) -> Self {
        let dims = vector_options.dims;
        let ratio = product_quantization_options.ratio;
        let bits = product_quantization_options.bits;
        let width = dims.div_ceil(ratio);
        let originals = (0..width)
            .into_par_iter()
            .map(|p| {
                let subdims = std::cmp::min(ratio, dims - ratio * p);
                let start = (p * ratio) as usize;
                let end = start + subdims as usize;
                let subsamples = sample_subvector_transform(vectors, start, end, transform);
                k_means(1 << bits, subsamples, false)
            })
            .collect::<Vec<_>>();
        let mut centroids = Vec2::zeros((1 << bits, dims as usize));
        for p in 0..width {
            let subdims = std::cmp::min(ratio, dims - ratio * p);
            for j in 0_usize..(1 << bits) {
                centroids[(j,)][(p * ratio) as usize..][..subdims as usize]
                    .copy_from_slice(&originals[p as usize][(j,)]);
            }
        }
        Self {
            dims,
            ratio,
            bits,
            originals,
            centroids,
        }
    }

    pub fn bits(&self) -> u32 {
        self.bits
    }

    pub fn bytes(&self) -> u32 {
        (self.dims.div_ceil(self.ratio) * self.bits).div_ceil(8)
    }

    pub fn width(&self) -> u32 {
        self.dims.div_ceil(self.ratio)
    }

    pub fn encode(&self, vector: Borrowed<'_, O>) -> Vec<u8> {
        let vector = vector.to_vec();
        let dims = self.dims;
        let ratio = self.ratio;
        let width = dims.div_ceil(ratio);
        let mut codes = Vec::with_capacity(width.div_ceil(self.bits) as usize);
        for p in 0..width {
            let subdims = std::cmp::min(ratio, dims - ratio * p);
            let left = &vector[(p * ratio) as usize..][..subdims as usize];
            let target = k_means::k_means_lookup(left, &self.originals[p as usize]);
            codes.push(target as u8);
        }
        codes
    }

    pub fn preprocess(&self, lhs: Borrowed<'_, O>) -> O::QuantizationPreprocessed {
        O::product_quantization_preprocess(
            self.dims,
            self.ratio,
            self.bits,
            self.centroids.as_slice(),
            lhs,
        )
    }

    pub fn process(&self, preprocessed: &O::QuantizationPreprocessed, rhs: &[u8]) -> F32 {
        let dims = self.dims;
        let ratio = self.ratio;
        match self.bits {
            1 => O::quantization_process(dims, ratio, 1, preprocessed, |i| {
                ((rhs[i >> 3] >> ((i & 7) << 0)) & 1) as usize
            }),
            2 => O::quantization_process(dims, ratio, 2, preprocessed, |i| {
                ((rhs[i >> 2] >> ((i & 3) << 1)) & 3) as usize
            }),
            4 => O::quantization_process(dims, ratio, 4, preprocessed, |i| {
                ((rhs[i >> 1] >> ((i & 1) << 2)) & 15) as usize
            }),
            8 => O::quantization_process(dims, ratio, 8, preprocessed, |i| rhs[i] as usize),
            _ => unreachable!(),
        }
    }

    pub fn push_batch(
        &self,
        preprocessed: &O::QuantizationPreprocessed,
        rhs: Range<u32>,
        heap: &mut Vec<(Reverse<F32>, AlwaysEqual<u32>)>,
        codes: &[u8],
        packed_codes: &[u8],
        fast_scan: bool,
    ) {
        todo!()
    }

    pub fn flat_rerank<'a, T: 'a, R: Fn(u32) -> (F32, T) + 'a>(
        &'a self,
        heap: Vec<(Reverse<F32>, AlwaysEqual<u32>)>,
        r: R,
        rerank_size: u32,
    ) -> impl RerankerPop<T> + 'a {
        WindowFlatReranker::new(heap, r, rerank_size)
    }

    pub fn graph_rerank<'a, T: 'a, C: Fn(u32) -> &'a [u8] + 'a, R: Fn(u32) -> (F32, T) + 'a>(
        &'a self,
        vector: Borrowed<'a, O>,
        c: C,
        r: R,
    ) -> GraphReranker<'a, T, R> {
        let p = O::product_quantization_preprocess(
            self.dims,
            self.ratio,
            self.bits,
            self.centroids.as_slice(),
            vector,
        );
        GraphReranker::new(Some(Box::new(move |u| self.process(&p, c(u)))), r)
    }
}
