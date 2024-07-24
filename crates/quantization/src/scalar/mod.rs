pub mod operator;

use self::operator::OperatorScalarQuantization;
use crate::reranker::window::WindowReranker;
use crate::reranker::window_0::Window0Reranker;
use base::index::*;
use base::operator::*;
use base::scalar::*;
use base::search::Reranker;
use base::search::Vectors;
use base::vector::*;
use common::vec2::Vec2;
use num_traits::Float;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct ScalarQuantizer<O: OperatorScalarQuantization> {
    dims: u32,
    bits: u32,
    max: Vec<Scalar<O>>,
    min: Vec<Scalar<O>>,
    centroids: Vec2<Scalar<O>>,
}

impl<O: OperatorScalarQuantization> ScalarQuantizer<O> {
    pub fn train(
        vector_options: VectorOptions,
        scalar_quantization_options: ScalarQuantizationOptions,
        vectors: &impl Vectors<O>,
        transform: impl Fn(Borrowed<'_, O>) -> Owned<O> + Copy,
    ) -> Self {
        let dims = vector_options.dims;
        let bits = scalar_quantization_options.bits;
        let mut max = vec![Scalar::<O>::neg_infinity(); dims as usize];
        let mut min = vec![Scalar::<O>::infinity(); dims as usize];
        let n = vectors.len();
        for i in 0..n {
            let vector = transform(vectors.vector(i)).as_borrowed().to_vec();
            for j in 0..dims as usize {
                max[j] = std::cmp::max(max[j], vector[j]);
                min[j] = std::cmp::min(min[j], vector[j]);
            }
        }
        let mut centroids = Vec2::zeros((1 << bits, dims as usize));
        for p in 0..dims {
            let bas = min[p as usize];
            let del = max[p as usize] - min[p as usize];
            for j in 0_usize..(1 << bits) {
                let val = Scalar::<O>::from_f(F32(j as f32 / ((1 << bits) - 1) as f32));
                centroids[(j, p as usize)] = bas + val * del;
            }
        }
        Self {
            dims,
            bits,
            max,
            min,
            centroids,
        }
    }

    pub fn encode(&self, vector: &[Scalar<O>]) -> Vec<u8> {
        let dims = self.dims;
        let bits = self.bits;
        let mut codes = Vec::with_capacity(dims as usize);
        for i in 0..dims as usize {
            let del = self.max[i] - self.min[i];
            let w =
                (((vector[i] - self.min[i]) / del).to_f32() * (((1 << bits) - 1) as f32)) as u32;
            codes.push(w.clamp(0, 255) as u8);
        }
        let bytes = (self.dims * self.bits).div_ceil(8);
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
            1 => codes
                .array_chunks::<8>()
                .map(merge_8)
                .take(bytes as usize)
                .collect(),
            2 => codes
                .array_chunks::<4>()
                .map(merge_4)
                .take(bytes as usize)
                .collect(),
            4 => codes
                .array_chunks::<2>()
                .map(merge_2)
                .take(bytes as usize)
                .collect(),
            8 => codes.take(bytes as usize).collect(),
            _ => unreachable!(),
        }
    }

    pub fn bytes(&self) -> u32 {
        (self.dims * self.bits).div_ceil(8)
    }

    pub fn preprocess(&self, lhs: Borrowed<'_, O>) -> O::QuantizationPreprocessed {
        O::scalar_quantization_preprocess(self.dims, self.bits, &self.max, &self.min, lhs)
    }

    pub fn process(&self, preprocessed: &O::QuantizationPreprocessed, rhs: &[u8]) -> F32 {
        let dims = self.dims;
        match self.bits {
            1 => O::quantization_process(dims, 1, 1, preprocessed, |i| {
                ((rhs[i >> 3] >> ((i & 7) << 0)) & 1) as usize
            }),
            2 => O::quantization_process(dims, 1, 2, preprocessed, |i| {
                ((rhs[i >> 2] >> ((i & 3) << 1)) & 3) as usize
            }),
            4 => O::quantization_process(dims, 1, 4, preprocessed, |i| {
                ((rhs[i >> 1] >> ((i & 1) << 2)) & 15) as usize
            }),
            8 => O::quantization_process(dims, 1, 8, preprocessed, |i| rhs[i] as usize),
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
        let p =
            O::scalar_quantization_preprocess(self.dims, self.bits, &self.max, &self.min, vector);
        if opts.flat_sq_rerank_size == 0 {
            Box::new(Window0Reranker::new(move |u, ()| self.process(&p, c(u)), r))
        } else {
            Box::new(WindowReranker::new(
                opts.flat_sq_rerank_size,
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
        let p =
            O::scalar_quantization_preprocess(self.dims, self.bits, &self.max, &self.min, vector);
        if opts.ivf_sq_rerank_size == 0 {
            Box::new(Window0Reranker::new(move |u, ()| self.process(&p, c(u)), r))
        } else {
            Box::new(WindowReranker::new(
                opts.ivf_sq_rerank_size,
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
                O::scalar_quantization_preprocess(
                    self.dims,
                    self.bits,
                    &self.max,
                    &self.min,
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
        let p =
            O::scalar_quantization_preprocess(self.dims, self.bits, &self.max, &self.min, vector);
        Box::new(Window0Reranker::new(move |u, ()| self.process(&p, c(u)), r))
    }
}
