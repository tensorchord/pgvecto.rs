pub mod operator;

use self::operator::OperatorProductQuantization;
use crate::reranker::window::WindowReranker;
use crate::reranker::window_0::Window0Reranker;
use crate::utils::InfiniteByteChunks;
use base::index::*;
use base::operator::*;
use base::scalar::*;
use base::search::*;
use base::vector::VectorOwned;
use common::sample::sample_subvector_transform;
use common::vec2::Vec2;
use k_means::k_means;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use serde::Deserialize;
use serde::Serialize;
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
                k_means(1 << bits, subsamples, |_| ())
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

    pub fn bytes(&self) -> u32 {
        (self.dims.div_ceil(self.ratio) * self.bits).div_ceil(8)
    }

    #[inline(always)]
    pub fn centroids(&self) -> &Vec2<Scalar<O>> {
        &self.centroids
    }

    pub fn encode(&self, vector: &[Scalar<O>]) -> Vec<u8> {
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
        let bytes = (self.dims.div_ceil(self.ratio) * self.bits).div_ceil(8);
        let codes = codes.into_iter().chain(std::iter::repeat(0));
        fn merge_8([b0, b1, b2, b3, b4, b5, b6, b7]: [u8; 8]) -> u8 {
            b0 | (b1 << 1) | (b2 << 2) | (b3 << 3) | (b4 << 4) | (b5 << 5) | (b6 << 6) | (b7 << 7)
        }
        fn merge_4([b0, b1, b2, b3]: [u8; 4]) -> u8 {
            b0 | (b1 << 2) | (b2 << 4) | (b3 << 6)
        }
        fn merge_2([b0, b1]: [u8; 2]) -> u8 {
            b0 | (b1 << 4)
        }
        match self.bits {
            1 => InfiniteByteChunks::new(codes)
                .map(merge_8)
                .take(bytes as usize)
                .collect(),
            2 => InfiniteByteChunks::new(codes)
                .map(merge_4)
                .take(bytes as usize)
                .collect(),
            4 => InfiniteByteChunks::new(codes)
                .map(merge_2)
                .take(bytes as usize)
                .collect(),
            8 => codes.take(bytes as usize).collect(),
            _ => unreachable!(),
        }
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

    pub fn flat_rerank<'a, T: 'a>(
        &'a self,
        vector: Borrowed<'a, O>,
        opts: &'a SearchOptions,
        c: impl Fn(u32) -> &'a [u8] + 'a,
        r: impl Fn(u32) -> (F32, T) + 'a,
    ) -> Box<dyn Reranker<T> + 'a> {
        let p = O::product_quantization_preprocess(
            self.dims,
            self.ratio,
            self.bits,
            self.centroids.as_slice(),
            vector,
        );
        if opts.flat_pq_rerank_size == 0 {
            Box::new(Window0Reranker::new(move |u, ()| self.process(&p, c(u)), r))
        } else {
            Box::new(WindowReranker::new(
                opts.flat_pq_rerank_size,
                move |u, ()| self.process(&p, c(u)),
                r,
            ))
        }
    }

    pub fn ivf_naive_rerank<'a, T: 'a>(
        &'a self,
        vector: Borrowed<'a, O>,
        opts: &'a SearchOptions,
        c: impl Fn(u32) -> &'a [u8] + 'a,
        r: impl Fn(u32) -> (F32, T) + 'a,
    ) -> Box<dyn Reranker<T> + 'a> {
        let p = O::product_quantization_preprocess(
            self.dims,
            self.ratio,
            self.bits,
            self.centroids.as_slice(),
            vector,
        );
        if opts.ivf_pq_rerank_size == 0 {
            Box::new(Window0Reranker::new(move |u, ()| self.process(&p, c(u)), r))
        } else {
            Box::new(WindowReranker::new(
                opts.ivf_pq_rerank_size,
                move |u, ()| self.process(&p, c(u)),
                r,
            ))
        }
    }

    pub fn ivf_residual_rerank<'a, T: 'a>(
        &'a self,
        vectors: Vec<Owned<O>>,
        opts: &'a SearchOptions,
        c: impl Fn(u32) -> &'a [u8] + 'a,
        r: impl Fn(u32) -> (F32, T) + 'a,
    ) -> Box<dyn Reranker<T, usize> + 'a> {
        let p = vectors
            .into_iter()
            .map(|vector| {
                O::product_quantization_preprocess(
                    self.dims,
                    self.ratio,
                    self.bits,
                    self.centroids.as_slice(),
                    vector.as_borrowed(),
                )
            })
            .collect::<Vec<_>>();
        if opts.ivf_pq_rerank_size == 0 {
            Box::new(Window0Reranker::new(
                move |u, i| self.process(&p[i], c(u)),
                r,
            ))
        } else {
            Box::new(WindowReranker::new(
                opts.ivf_pq_rerank_size,
                move |u, i| self.process(&p[i], c(u)),
                r,
            ))
        }
    }

    pub fn graph_rerank<'a, T: 'a>(
        &'a self,
        vector: Borrowed<'a, O>,
        _: &'a SearchOptions,
        c: impl Fn(u32) -> &'a [u8] + 'a,
        r: impl Fn(u32) -> (F32, T) + 'a,
    ) -> Box<dyn Reranker<T> + 'a> {
        let p = O::product_quantization_preprocess(
            self.dims,
            self.ratio,
            self.bits,
            self.centroids.as_slice(),
            vector,
        );
        Box::new(Window0Reranker::new(move |u, ()| self.process(&p, c(u)), r))
    }
}
