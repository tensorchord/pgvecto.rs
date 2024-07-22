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
        let w = dims.div_ceil(ratio);
        let originals = (0..w)
            .into_par_iter()
            .map(|i| {
                let subdims = std::cmp::min(ratio, dims - ratio * i);
                let start = (i * ratio) as usize;
                let end = start + subdims as usize;
                let subsamples = sample_subvector_transform(vectors, start, end, transform);
                k_means(256, subsamples)
            })
            .collect::<Vec<_>>();
        let mut centroids = Vec2::zeros((256, dims as usize));
        for i in 0..w {
            let subdims = std::cmp::min(ratio, dims - ratio * i);
            for j in 0u8..=255 {
                centroids[(j as usize,)][(i * ratio) as usize..][..subdims as usize]
                    .copy_from_slice(&originals[i as usize][(j as usize,)]);
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
        let w = dims.div_ceil(ratio);
        let mut result = Vec::with_capacity(w as usize);
        for i in 0..w {
            let subdims = std::cmp::min(ratio, dims - ratio * i);
            let left = &vector[(i * ratio) as usize..][..subdims as usize];
            let target = k_means::k_means_lookup(left, &self.originals[i as usize]);
            result.push(target as u8);
        }
        result
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
        O::product_quantization_process(self.dims, self.ratio, self.bits, preprocessed, rhs)
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
            Box::new(Window0Reranker::new(
                move |u, ()| {
                    O::product_quantization_process(self.dims, self.ratio, self.bits, &p, c(u))
                },
                r,
            ))
        } else {
            Box::new(WindowReranker::new(
                opts.flat_pq_rerank_size,
                move |u, ()| {
                    O::product_quantization_process(self.dims, self.ratio, self.bits, &p, c(u))
                },
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
            Box::new(Window0Reranker::new(
                move |u, ()| {
                    O::product_quantization_process(self.dims, self.ratio, self.bits, &p, c(u))
                },
                r,
            ))
        } else {
            Box::new(WindowReranker::new(
                opts.ivf_pq_rerank_size,
                move |u, ()| {
                    O::product_quantization_process(self.dims, self.ratio, self.bits, &p, c(u))
                },
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
                move |u, i| {
                    O::product_quantization_process(self.dims, self.ratio, self.bits, &p[i], c(u))
                },
                r,
            ))
        } else {
            Box::new(WindowReranker::new(
                opts.ivf_pq_rerank_size,
                move |u, i| {
                    O::product_quantization_process(self.dims, self.ratio, self.bits, &p[i], c(u))
                },
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
        Box::new(Window0Reranker::new(
            move |u, ()| {
                O::product_quantization_process(self.dims, self.ratio, self.bits, &p, c(u))
            },
            r,
        ))
    }
}
