use std::ops::Div;

use crate::reranker::window::WindowReranker;
use crate::reranker::window_0::Window0Reranker;

use self::operator::OperatorRaBitQ;
use base::index::{RaBitQuantizationOptions, SearchOptions, VectorOptions};
use base::operator::{Borrowed, Owned, Scalar};
use base::scalar::{ScalarLike, F32};
use base::search::{Reranker, Vectors};
use base::vector::{VectorBorrowed, VectorOwned};

use nalgebra::debug::RandomOrthogonal;
use nalgebra::{Dim, Dyn};
use num_traits::{Float, One, ToPrimitive, Zero};
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};

pub mod operator;

const EPSILON: f32 = 1.9;
const THETA_LOG_DIM: u32 = 4;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct RaBitQuantizer<O: OperatorRaBitQ> {
    dim: u32,
    dim_pad_64: u32,
    projection: Vec<Vec<Scalar<O>>>,
    binary_vec_x: Vec<Vec<u64>>,
    distance_to_centroid: Vec<Scalar<O>>,
    distance_to_centroid_square: Vec<Scalar<O>>,
    dot_product_x: Vec<Scalar<O>>,
    rand_bias: Vec<Scalar<O>>,
    error_bound: Vec<Scalar<O>>,
    factor_ip: Vec<Scalar<O>>,
    factor_ppc: Vec<Scalar<O>>,
}

impl<O: OperatorRaBitQ> RaBitQuantizer<O> {
    pub fn train(
        vector_options: VectorOptions,
        options: RaBitQuantizationOptions,
        vectors: &impl Vectors<O>,
        transform: impl Fn(Borrowed<'_, O>) -> Owned<O> + Copy,
    ) -> Self {
        let dim_pad = (vector_options.dims + 63) / 64 * 64;
        let mut rand_bias = Vec::with_capacity(dim_pad as usize);
        let mut rng = thread_rng();
        for i in 0..dim_pad {
            rand_bias.push(Scalar::<O>::from_f32(rng.gen()));
        }
        let projection = gen_random_orthogonal::<O>(dim_pad as usize);
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
            dot_product_x[i] = O::vector_dot_product(&quantized_x[i], &signed_x[i]).div(norm);
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
                    * (xc_over_dot_product * xc_over_dot_product - distance_to_centroid_square[i]),
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
            distance_to_centroid,
            dot_product_x,
            rand_bias,
            error_bound,
            factor_ip,
            factor_ppc,
        }
    }

    pub fn width(&self) -> usize {
        (self.dim / 64) as usize
    }

    pub fn encode(&self, vector: &[Scalar<O>]) -> Vec<u8> {
        unimplemented!()
    }

    pub fn preprocess(&self, lhs: Borrowed<'_, O>) -> O::RabitQuantizationPreprocessed {
        unimplemented!()
    }

    pub fn process(&self, preprocessed: &O::RabitQuantizationPreprocessed, rhs: &[u8]) -> F32 {
        unimplemented!()
    }

    pub fn flat_rerank<'a, T: 'a>(
        &'a self,
        vector: Borrowed<'a, O>,
        opts: &'a SearchOptions,
        c: impl Fn(u32) -> &'a [u8] + 'a,
        r: impl Fn(u32) -> (F32, T) + 'a,
    ) -> Box<dyn Reranker<T> + 'a> {
        unimplemented!()
    }

    pub fn ivf_naive_rerank<'a, T: 'a>(
        &'a self,
        vector: Borrowed<'a, O>,
        opts: &'a SearchOptions,
        c: impl Fn(u32) -> &'a [u8] + 'a,
        r: impl Fn(u32) -> (F32, T) + 'a,
    ) -> Box<dyn Reranker<T> + 'a> {
        unimplemented!()
    }

    pub fn ivf_residual_rerank<'a, T: 'a>(
        &'a self,
        vectors: Vec<Owned<O>>,
        opts: &'a SearchOptions,
        r: impl Fn(u32) -> (F32, T) + 'a,
    ) -> Box<dyn Reranker<T, usize> + 'a> {
        let n = vectors.len(); // number of selected centroids
        let mut quantized_y = vec![vec![Scalar::<O>::zero(); self.dim_pad_64 as usize]; n];
        let mut distance_to_centroid_square_y = vec![Scalar::<O>::zero(); n];
        for i in 0..n {
            let vector = vectors[i].as_borrowed().to_vec();
            for j in 0..self.dim_pad_64 as usize {
                quantized_y[i][j] = O::vector_dot_product(&self.projection[j], &vector);
            }
            distance_to_centroid_square_y[i] =
                O::vector_dot_product(&quantized_y[i], &quantized_y[i]);
        }

        // calculate the lower bound and delta for each centroid
        let mut value_lower_bound = vec![Scalar::<O>::infinity(); n];
        let mut value_upper_bound = vec![Scalar::<O>::neg_infinity(); n];
        let mut value_delta = vec![Scalar::<O>::zero(); n];
        for i in 0..n {
            for j in 0..self.dim_pad_64 as usize {
                value_lower_bound[i] = Float::min(value_lower_bound[i], quantized_y[i][j]);
                value_upper_bound[i] = Float::max(value_upper_bound[i], quantized_y[i][j]);
            }
            value_delta[i] = (value_upper_bound[i] - value_lower_bound[i])
                / Scalar::<O>::from_f32(THETA_LOG_DIM as f32 - 1.0);
        }

        // scalar quantization
        let mut quantized_y_scalar = vec![vec![0u8; self.dim_pad_64 as usize]; n];
        let mut scalar_sum = vec![0u32; n];
        for i in 0..n {
            for j in 0..self.dim_pad_64 as usize {
                quantized_y_scalar[i][j] = ((quantized_y[i][j] - value_lower_bound[i])
                    * value_delta[i]
                    + self.rand_bias[j])
                    .to_u8()
                    .expect("failed to convert a Scalar value to u8");
                scalar_sum[i] += quantized_y_scalar[i][j] as u32;
            }
        }
        // product quantization
        let mut binary_vec_y: Vec<Vec<u64>> = Vec::with_capacity(n);
        for i in 0..n {
            binary_vec_y.push(O::query_vector_binarize_u64(&quantized_y_scalar[i]));
        }

        // `xi` is the index of x, `ci` is the index of the selected centroid
        if opts.ivf_rabit_rerank_size == 0 {
            Box::new(Window0Reranker::new(
                move |xi, ci| {
                    O::rabit_quantization_process(
                        self.distance_to_centroid_square[xi as usize],
                        distance_to_centroid_square_y[ci],
                        self.factor_ppc[xi as usize],
                        self.factor_ip[xi as usize],
                        self.error_bound[xi as usize],
                        value_lower_bound[ci],
                        value_delta[ci],
                        Scalar::<O>::from_f32(scalar_sum[ci] as f32),
                        &self.binary_vec_x[xi as usize],
                        &binary_vec_y[ci],
                    )
                },
                r,
            ))
        } else {
            Box::new(WindowReranker::new(
                opts.ivf_rabit_rerank_size,
                move |xi, ci| {
                    O::rabit_quantization_process(
                        self.distance_to_centroid_square[xi as usize],
                        distance_to_centroid_square_y[ci],
                        self.factor_ppc[xi as usize],
                        self.factor_ip[xi as usize],
                        self.error_bound[xi as usize],
                        value_lower_bound[ci],
                        value_delta[ci],
                        Scalar::<O>::from_f32(scalar_sum[ci] as f32),
                        &self.binary_vec_x[xi as usize],
                        binary_vec_y[ci].as_slice(),
                    )
                },
                r,
            ))
        }
    }

    pub fn graph_rerank<'a, T: 'a>(
        &'a self,
        vector: Borrowed<'a, O>,
        opts: &'a SearchOptions,
        c: impl Fn(u32) -> &'a [u8] + 'a,
        r: impl Fn(u32) -> (F32, T) + 'a,
    ) -> Box<dyn Reranker<T> + 'a> {
        unimplemented!()
    }
}

fn gen_random_orthogonal<O: OperatorRaBitQ>(dim: usize) -> Vec<Vec<Scalar<O>>> {
    let mut rng = thread_rng();
    let random_orth: RandomOrthogonal<f32, Dyn> =
        RandomOrthogonal::new(Dim::from_usize(dim), || rng.gen());
    let random_matrix = random_orth.unwrap();
    let mut projection = vec![Vec::with_capacity(dim); dim];
    // use the transpose of the random matrix as the inverse of the orthogonal matrix,
    // but we need to transpose it again to make it efficient for the dot production
    for (i, vec) in random_matrix.row_iter().enumerate() {
        for val in vec.iter() {
            projection[i].push(Scalar::<O>::from_f32(*val));
        }
    }

    projection
}
