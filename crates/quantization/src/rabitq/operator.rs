use base::operator::{Operator, Owned, Scalar};
use base::scalar::{ScalarLike, F32};
use num_traits::{Float, One, Zero};

use super::THETA_LOG_DIM;

pub trait OperatorRaBitQ: Operator {
    type RabitQuantizationPreprocessed;

    fn vector_dot_product(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> Scalar<Self>;
    fn vector_binarize_u64(vec: &[Scalar<Self>]) -> Vec<u64>;
    fn vector_binarize_one(vec: &[Scalar<Self>]) -> Vec<Scalar<Self>>;
    fn query_vector_binarize_u64(vec: &[u8]) -> Vec<u64>;
    fn rabit_quantization_process(
        x_centroid_square: Scalar<Self>,
        y_centroid_square: Scalar<Self>,
        factor_ppc: Scalar<Self>,
        factor_ip: Scalar<Self>,
        error_bound: Scalar<Self>,
        value_lower_bound: Scalar<Self>,
        value_delta: Scalar<Self>,
        scalar_sum: Scalar<Self>,
        binary_x: &[u64],
        binary_y: &[u64],
    ) -> F32;
    fn rabit_quantization_preprocess(
        dim: usize,
    ) -> Self::RabitQuantizationPreprocessed;
}

impl<O: Operator> OperatorRaBitQ for O {
    type RabitQuantizationPreprocessed = (Scalar<O>, Scalar<O>, Scalar<O>, Scalar<O>, Vec<u64>);

    fn rabit_quantization_preprocess(dim: usize) -> Self::RabitQuantizationPreprocessed {
        // let mut quantized_y = Vec::with_capacity(dim);
        // for i in 0..dim {
        //     quantized_y.push(Self::vector_dot_product(lhs, rhs))
        // }
        unimplemented!()
    }

    fn vector_dot_product(lhs: &[Scalar<O>], rhs: &[Scalar<O>]) -> Scalar<O> {
        let mut sum = Scalar::<O>::zero();
        for i in 0..std::cmp::min(lhs.len(), rhs.len()) {
            sum += lhs[i] * rhs[i];
        }
        sum
    }

    // binarize vector to 0 or 1 in binary format stored in u64
    fn vector_binarize_u64(vec: &[Scalar<Self>]) -> Vec<u64> {
        let mut binary = vec![0u64, (vec.len() as u64 + 63) / 64];
        for i in 0..vec.len() {
            if vec[i].is_sign_positive() {
                binary[i / 64] |= 1 << (i % 64);
            }
        }
        binary
    }

    // binarize vector to +1 or -1
    fn vector_binarize_one(vec: &[Scalar<Self>]) -> Vec<Scalar<Self>> {
        let mut binary = vec![Scalar::<O>::one(); vec.len()];
        for i in 0..vec.len() {
            if vec[i].is_sign_negative() {
                binary[i] = -Scalar::<O>::one();
            }
        }
        binary
    }

    fn query_vector_binarize_u64(vec: &[u8]) -> Vec<u64> {
        let mut binary = Vec::with_capacity(vec.len() * (THETA_LOG_DIM as usize) / 64);
        // TODO: implement with SIMD
        binary
    }

    fn rabit_quantization_process(
        x_centroid_square: Scalar<Self>,
        y_centroid_square: Scalar<Self>,
        factor_ppc: Scalar<Self>,
        factor_ip: Scalar<Self>,
        error_bound: Scalar<Self>,
        value_lower_bound: Scalar<Self>,
        value_delta: Scalar<Self>,
        scalar_sum: Scalar<Self>,
        binary_x: &[u64],
        binary_y: &[u64],
    ) -> F32 {
        let estimate = x_centroid_square * y_centroid_square
            + value_lower_bound * factor_ppc
            + (Scalar::<O>::from_f32(
                2.0 * asymmetric_binary_dot_product(&binary_x, &binary_y) as f32,
            ) - scalar_sum)
                * factor_ip
                * value_delta;
        (estimate - (error_bound * y_centroid_square.sqrt())).to_f()
    }
}

fn asymmetric_binary_dot_product(x: &[u64], y: &[u64]) -> u32 {
    let mut res = 0;
    let length = x.len();
    for i in 0..THETA_LOG_DIM as usize {
        let mut layer = 0;
        for j in 0..x.len() {
            layer += (x[j] & y[j + i * length]).count_ones();
        }
        res += layer << i;
    }
    res
}
