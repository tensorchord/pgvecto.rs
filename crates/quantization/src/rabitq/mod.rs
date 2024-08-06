use std::cmp::Reverse;
use std::ops::{Div, Range};

use self::operator::OperatorRabitq;
use crate::reranker::error::ErrorFlatReranker;
use crate::reranker::window_0::Window0GraphReranker;
use base::index::{RabitqQuantizationOptions, VectorOptions};
use base::operator::{Borrowed, Owned, Scalar};
use base::scalar::{ScalarLike, F32};
use base::search::{RerankerPop, RerankerPush, Vectors};
use base::vector::{VectorBorrowed, VectorOwned};

use num_traits::{Float, One, Zero};
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};

pub mod operator;

const EPSILON: f32 = 1.9;
const THETA_LOG_DIM: u32 = 4;
const DEFAULT_X_DOT_PRODUCT: f32 = 0.8;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct RabitqQuantizer<O: OperatorRabitq> {
    dim: u32,
    dim_pad_64: u32,
    projection: Vec<Vec<Scalar<O>>>,
    binary_vec_x: Vec<Vec<u64>>,
    distance_to_centroid_square: Vec<Scalar<O>>,
    rand_bias: Vec<Scalar<O>>,
    error_bound: Vec<Scalar<O>>,
    factor_ip: Vec<Scalar<O>>,
    factor_ppc: Vec<Scalar<O>>,
}

impl<O: OperatorRabitq> RabitqQuantizer<O> {
    pub fn train(
        vector_options: VectorOptions,
        _options: RabitqQuantizationOptions,
        vectors: &impl Vectors<O>,
        transform: impl Fn(Borrowed<'_, O>) -> Owned<O> + Copy,
    ) -> Self {
        let dim_pad = (vector_options.dims + 63) / 64 * 64;
        let mut rand_bias = Vec::with_capacity(dim_pad as usize);
        let mut rng = thread_rng();
        for _ in 0..dim_pad {
            rand_bias.push(Scalar::<O>::from_f32(rng.gen()));
        }
        let projection = O::gen_random_orthogonal(dim_pad as usize);
        let n = vectors.len() as usize;
        let mut distance_to_centroid = vec![Scalar::<O>::zero(); n];
        let mut distance_to_centroid_square = vec![Scalar::<O>::zero(); n];
        let mut quantized_x = vec![vec![Scalar::<O>::zero(); dim_pad as usize]; n];
        for i in 0..n {
            let vector = transform(vectors.vector(i as u32)).as_borrowed().to_vec();
            distance_to_centroid_square[i] = O::vector_dot_product(&vector, &vector);
            distance_to_centroid[i] = distance_to_centroid_square[i].sqrt();
            for j in 0..vector_options.dims as usize {
                quantized_x[i][j] = O::vector_dot_product(&projection[j], &vector);
            }
        }
        let mut binary_vec_x = Vec::with_capacity(n);
        let mut signed_x = Vec::with_capacity(n);
        for i in 0..(n) {
            binary_vec_x.push(O::vector_binarize_u64(&quantized_x[i]));
            signed_x.push(O::vector_binarize_one(&quantized_x[i]));
        }
        let mut dot_product_x = vec![Scalar::<O>::zero(); n];
        for i in 0..(n) {
            let norm = O::vector_dot_product(&quantized_x[i], &quantized_x[i]).sqrt()
                * Scalar::<O>::from_f32(dim_pad as f32).sqrt();
            dot_product_x[i] = if norm.is_normal() {
                O::vector_dot_product(&quantized_x[i], &signed_x[i]).div(norm)
            } else {
                Scalar::<O>::from_f32(DEFAULT_X_DOT_PRODUCT)
            }
        }

        let mut error_bound = Vec::with_capacity(n);
        let mut factor_ip = Vec::with_capacity(n);
        let mut factor_ppc = Vec::with_capacity(n);
        let error_base = Scalar::<O>::from_f32(2.0 * EPSILON / (dim_pad as f32 - 1.0).sqrt());
        let dim_pad_sqrt = Scalar::<O>::from_f32(dim_pad as f32).sqrt();
        let one_vec = vec![Scalar::<O>::one(); dim_pad as usize];
        for i in 0..n {
            let xc_over_dot_product = distance_to_centroid[i] / dot_product_x[i];
            error_bound.push(
                error_base
                    * (xc_over_dot_product * xc_over_dot_product - distance_to_centroid_square[i])
                        .sqrt(),
            );
            let ip = Scalar::<O>::from_f32(-2.0) / dim_pad_sqrt * xc_over_dot_product;
            factor_ip.push(ip);
            factor_ppc.push(ip * O::vector_dot_product(&one_vec, &signed_x[i]));
        }

        Self {
            dim: vector_options.dims,
            dim_pad_64: dim_pad,
            projection,
            binary_vec_x,
            distance_to_centroid_square,
            rand_bias,
            error_bound,
            factor_ip,
            factor_ppc,
        }
    }

    pub fn width(&self) -> usize {
        (self.dim / 64) as usize
    }

    pub fn encode(&self, _vector: &[Scalar<O>]) -> Vec<u8> {
        unimplemented!()
    }

    pub fn preprocess(&self, lhs: Borrowed<'_, O>) -> O::RabitqQuantizationPreprocessed {
        O::rabit_quantization_preprocess(
            self.dim_pad_64 as usize,
            lhs,
            &self.projection,
            &self.rand_bias,
        )
    }

    pub fn process(&self, preprocessed: &O::RabitqQuantizationPreprocessed, i: u32) -> F32 {
        O::rabit_quantization_process(
            self.distance_to_centroid_square[i as usize],
            self.factor_ppc[i as usize],
            self.factor_ip[i as usize],
            self.error_bound[i as usize],
            &self.binary_vec_x[i as usize],
            preprocessed,
        )
        .0
    }

    pub fn process_lowerbound(
        &self,
        preprocessed: &O::RabitqQuantizationPreprocessed,
        i: u32,
    ) -> F32 {
        O::rabit_quantization_process(
            self.distance_to_centroid_square[i as usize],
            self.factor_ppc[i as usize],
            self.factor_ip[i as usize],
            self.error_bound[i as usize],
            &self.binary_vec_x[i as usize],
            preprocessed,
        )
        .1
    }

    pub fn push_batch(
        &self,
        preprocessed: &O::RabitqQuantizationPreprocessed,
        rhs: Range<u32>,
        heap: &mut Vec<(Reverse<F32>, u32)>,
        _codes: &[u8],
        _packed_codes: &[u8],
    ) {
        heap.extend(rhs.map(|u| (Reverse(self.process_lowerbound(preprocessed, u)), u)));
    }

    pub fn flat_rerank<'a, T: 'a>(
        &'a self,
        heap: Vec<(Reverse<F32>, u32)>,
        r: impl Fn(u32) -> (F32, T) + 'a,
    ) -> impl RerankerPop<T> + 'a {
        ErrorFlatReranker::new(heap, r)
    }

    pub fn graph_rerank<'a, T: 'a>(
        &'a self,
        vector: Borrowed<'a, O>,
        r: impl Fn(u32) -> (F32, T) + 'a,
    ) -> impl RerankerPop<T> + RerankerPush + 'a {
        let p = O::rabit_quantization_preprocess(
            self.dim as usize,
            vector,
            &self.projection,
            &self.rand_bias,
        );
        Window0GraphReranker::new(move |u| self.process(&p, u), r)
    }
}
