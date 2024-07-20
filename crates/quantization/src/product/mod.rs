pub mod operator;

use self::operator::OperatorProductQuantization;
use crate::reranker::window::WindowReranker;
use crate::reranker::window_0::Window0Reranker;
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
                k_means(1 << bits, subsamples)
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
        codes.extend(std::iter::repeat(0).take((8 / self.bits) as usize - 1));
        let result = codes.chunks_exact((8 / self.bits) as usize);
        match self.bits {
            8 => result
                .map(|x| <[u8; 1]>::try_from(x).unwrap())
                .map(|x| x[0] << 0)
                .collect(),
            4 => result
                .map(|x| <[u8; 2]>::try_from(x).unwrap())
                .map(|x| x[1] << 4 | x[0] << 0)
                .collect(),
            2 => result
                .map(|x| <[u8; 4]>::try_from(x).unwrap())
                .map(|x| x[3] << 6 | x[2] << 4 | x[1] << 2 | x[0] << 0)
                .collect(),
            1 => result
                .map(|x| <[u8; 8]>::try_from(x).unwrap())
                .map(|x| {
                    x[7] << 7
                        | x[6] << 6
                        | x[5] << 5
                        | x[4] << 4
                        | x[3] << 3
                        | x[2] << 2
                        | x[1] << 1
                        | x[0] << 0
                })
                .collect(),
            _ => unreachable!(),
        }
    }

    pub fn preprocess(&self, lhs: Borrowed<'_, O>) -> O::ProductQuantizationPreprocessed {
        O::product_quantization_preprocess(
            self.dims,
            self.ratio,
            self.bits,
            self.centroids.as_slice(),
            lhs,
        )
    }

    pub fn process(&self, preprocessed: &O::ProductQuantizationPreprocessed, rhs: &[u8]) -> F32 {
        match self.bits {
            1 => O::product_quantization_process(
                self.dims,
                self.ratio,
                self.bits,
                preprocessed,
                |i| find(1, rhs, i),
            ),
            2 => O::product_quantization_process(
                self.dims,
                self.ratio,
                self.bits,
                preprocessed,
                |i| find(2, rhs, i),
            ),
            4 => O::product_quantization_process(
                self.dims,
                self.ratio,
                self.bits,
                preprocessed,
                |i| find(4, rhs, i),
            ),
            8 => O::product_quantization_process(
                self.dims,
                self.ratio,
                self.bits,
                preprocessed,
                |i| find(8, rhs, i),
            ),
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

#[inline(always)]
fn find(bits: u32, rhs: &[u8], i: usize) -> usize {
    (match bits {
        1 => (rhs[i >> 3] >> ((i & 7) << 1)) & 1,
        2 => (rhs[i >> 2] >> ((i & 3) << 2)) & 3,
        4 => (rhs[i >> 1] >> ((i & 1) << 4)) & 15,
        8 => rhs[i],
        _ => unreachable!(),
    }) as usize
}
