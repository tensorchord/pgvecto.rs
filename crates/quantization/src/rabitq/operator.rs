use base::operator::{Borrowed, Operator, Scalar};
use base::scalar::{ScalarLike, F32};
use base::vector::VectorBorrowed;

use nalgebra::debug::RandomOrthogonal;
use nalgebra::{Dim, Dyn};
use num_traits::{Float, One, ToPrimitive, Zero};
use rand::{thread_rng, Rng};

use super::THETA_LOG_DIM;

pub trait OperatorRabitq: Operator {
    type RabitqQuantizationPreprocessed;

    fn vector_dot_product(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> Scalar<Self>;
    fn vector_binarize_u64(vec: &[Scalar<Self>]) -> Vec<u64>;
    fn vector_binarize_one(vec: &[Scalar<Self>]) -> Vec<Scalar<Self>>;
    fn query_vector_binarize_u64(vec: &[u8]) -> Vec<u64>;
    fn gen_random_orthogonal(dim: usize) -> Vec<Vec<Scalar<Self>>>;
    fn rabit_quantization_process(
        x_centroid_square: Scalar<Self>,
        factor_ppc: Scalar<Self>,
        factor_ip: Scalar<Self>,
        error_bound: Scalar<Self>,
        binary_x: &[u64],
        p: &Self::RabitqQuantizationPreprocessed,
    ) -> (F32, F32);
    fn rabit_quantization_preprocess(
        dim: usize,
        vec: Borrowed<'_, Self>,
        projection: &[Vec<Scalar<Self>>],
        rand_bias: &[Scalar<Self>],
    ) -> Self::RabitqQuantizationPreprocessed;
}

impl<O: Operator> OperatorRabitq for O {
    // (distance_square, lower_bound, delta, scalar_sum, binary_vec_y)
    type RabitqQuantizationPreprocessed = (Scalar<O>, Scalar<O>, Scalar<O>, Scalar<O>, Vec<u64>);

    fn rabit_quantization_preprocess(
        dim: usize,
        vec: Borrowed<'_, Self>,
        projection: &[Vec<Scalar<Self>>],
        rand_bias: &[Scalar<Self>],
    ) -> Self::RabitqQuantizationPreprocessed {
        let mut quantized_y = Vec::with_capacity(dim);
        let vector = vec.to_vec();
        for i in 0..dim {
            quantized_y.push(Self::vector_dot_product(&projection[i], &vector));
        }
        let distance_to_centroid_square = Self::vector_dot_product(&quantized_y, &quantized_y);
        let mut lower_bound = Scalar::<O>::infinity();
        let mut upper_bound = Scalar::<O>::neg_infinity();
        for i in 0..dim {
            lower_bound = Float::min(lower_bound, quantized_y[i]);
            upper_bound = Float::max(upper_bound, quantized_y[i]);
        }
        let delta =
            (upper_bound - lower_bound) / Scalar::<O>::from_f32((1 << THETA_LOG_DIM) as f32 - 1.0);

        // scalar quantization
        let mut quantized_y_scalar = vec![0u8; dim];
        let mut scalar_sum = 0u32;
        let one_over_delta = Scalar::<O>::one() / delta;
        for i in 0..dim {
            quantized_y_scalar[i] = ((quantized_y[i] - lower_bound) * one_over_delta
                + rand_bias[i])
                .to_u8()
                .expect("failed to convert a Scalar value to u8");
            scalar_sum += quantized_y_scalar[i] as u32;
        }
        let binary_vec_y = O::query_vector_binarize_u64(&quantized_y_scalar);
        (
            distance_to_centroid_square,
            lower_bound,
            delta,
            Scalar::<O>::from_f32(scalar_sum as f32),
            binary_vec_y,
        )
    }

    fn gen_random_orthogonal(dim: usize) -> Vec<Vec<Scalar<Self>>> {
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

    fn vector_dot_product(lhs: &[Scalar<O>], rhs: &[Scalar<O>]) -> Scalar<O> {
        let mut sum = Scalar::<O>::zero();
        let length = std::cmp::min(lhs.len(), rhs.len());
        for i in 0..length {
            sum += lhs[i] * rhs[i];
        }
        sum
    }

    // binarize vector to 0 or 1 in binary format stored in u64
    fn vector_binarize_u64(vec: &[Scalar<Self>]) -> Vec<u64> {
        let mut binary = vec![0u64; (vec.len() + 63) / 64];
        let zero = Scalar::<O>::zero();
        for i in 0..vec.len() {
            if vec[i] > zero {
                binary[i / 64] |= 1 << (i % 64);
            }
        }
        binary
    }

    // binarize vector to +1 or -1
    fn vector_binarize_one(vec: &[Scalar<Self>]) -> Vec<Scalar<Self>> {
        let mut binary = vec![Scalar::<O>::one(); vec.len()];
        let zero = Scalar::<O>::zero();
        for i in 0..vec.len() {
            if vec[i] <= zero {
                binary[i] = -Scalar::<O>::one();
            }
        }
        binary
    }

    fn query_vector_binarize_u64(vec: &[u8]) -> Vec<u64> {
        let length = vec.len();
        let mut binary = vec![0u64; length * THETA_LOG_DIM as usize / 64];
        for j in 0..THETA_LOG_DIM as usize {
            for i in 0..length {
                binary[(i + j * length) / 64] |= (((vec[i] >> j) & 1) as u64) << (i % 64);
            }
        }
        binary
    }

    fn rabit_quantization_process(
        x_centroid_square: Scalar<Self>,
        factor_ppc: Scalar<Self>,
        factor_ip: Scalar<Self>,
        error_bound: Scalar<Self>,
        binary_x: &[u64],
        p: &Self::RabitqQuantizationPreprocessed,
    ) -> (F32, F32) {
        let estimate = (x_centroid_square
            + p.0
            + p.1 * factor_ppc
            + (Scalar::<O>::from_f32(2.0 * asymmetric_binary_dot_product(binary_x, &p.4) as f32)
                - p.3)
                * factor_ip
                * p.2)
            .to_f();
        let err = (error_bound * p.0.sqrt()).to_f();
        (estimate, estimate - err)
    }
}

fn binary_dot_product(x: &[u64], y: &[u64]) -> u32 {
    let mut res = 0;
    for i in 0..x.len() {
        res += (x[i] & y[i]).count_ones();
    }
    res
}

fn asymmetric_binary_dot_product(x: &[u64], y: &[u64]) -> u32 {
    let mut res = 0;
    let length = x.len();
    for i in 0..THETA_LOG_DIM as usize {
        res += binary_dot_product(x, &y[i * length..(i + 1) * length]) << i;
    }
    res
}
