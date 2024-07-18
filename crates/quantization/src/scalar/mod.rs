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
        Self {
            dims,
            bits,
            max,
            min,
        }
    }

    pub fn encode(&self, vector: &[Scalar<O>]) -> Vec<u8> {
        let dims = self.dims;
        let mut result = vec![0u8; dims as usize];
        for i in 0..dims as usize {
            let w =
                (((vector[i] - self.min[i]) / (self.max[i] - self.min[i])).to_f32() * 256.0) as u32;
            result[i] = w.clamp(0, 255) as u8;
        }
        result
    }

    pub fn bytes(&self) -> u32 {
        (self.dims * self.bits).div_ceil(8)
    }

    pub fn preprocess(&self, lhs: Borrowed<'_, O>) -> O::ScalarQuantizationPreprocessed {
        O::scalar_quantization_preprocess(self.dims, self.bits, &self.max, &self.min, lhs)
    }

    pub fn process(&self, preprocessed: &O::ScalarQuantizationPreprocessed, rhs: &[u8]) -> F32 {
        O::scalar_quantization_process(self.dims, self.bits, preprocessed, rhs)
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
            Box::new(Window0Reranker::new(
                move |u, ()| O::scalar_quantization_process(self.dims, self.bits, &p, c(u)),
                r,
            ))
        } else {
            Box::new(WindowReranker::new(
                opts.flat_sq_rerank_size,
                move |u, ()| O::scalar_quantization_process(self.dims, self.bits, &p, c(u)),
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
            Box::new(Window0Reranker::new(
                move |u, ()| O::scalar_quantization_process(self.dims, self.bits, &p, c(u)),
                r,
            ))
        } else {
            Box::new(WindowReranker::new(
                opts.ivf_sq_rerank_size,
                move |u, ()| O::scalar_quantization_process(self.dims, self.bits, &p, c(u)),
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
                move |u, i| O::scalar_quantization_process(self.dims, self.bits, &p[i], c(u)),
                r,
            ))
        } else {
            Box::new(WindowReranker::new(
                opts.ivf_pq_rerank_size,
                move |u, i| O::scalar_quantization_process(self.dims, self.bits, &p[i], c(u)),
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
        Box::new(Window0Reranker::new(
            move |u, ()| O::scalar_quantization_process(self.dims, self.bits, &p, c(u)),
            r,
        ))
    }
}
