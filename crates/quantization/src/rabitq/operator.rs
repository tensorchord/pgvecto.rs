use base::operator::{Operator, Owned, Scalar};
use num_traits::{Float, One, Zero};

pub trait OperatorRaBitQ: Operator {
    type RabitQuantizationPreprocessed;

    fn vector_dot_product(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> Scalar<Self>;
    fn vector_binarize_u64(vec: &[Scalar<Self>]) -> Vec<u64>;
    fn vector_binarize_one(vec: &[Scalar<Self>]) -> Vec<Scalar<Self>>;
}

impl<O: Operator> OperatorRaBitQ for O {
    type RabitQuantizationPreprocessed = Owned<O>;

    fn vector_dot_product(lhs: &[Scalar<O>], rhs: &[Scalar<O>]) -> Scalar<O> {
        let mut sum = Scalar::<O>::zero();
        for i in 0..std::cmp::min(lhs.len(), rhs.len()) {
            sum += lhs[i] * rhs[i];
        }
        sum
    }

    fn vector_binarize_u64(vec: &[Scalar<Self>]) -> Vec<u64> {
        let mut binary = vec![0u64, (vec.len() as u64 + 63) / 64];
        for i in 0..vec.len() {
            if vec[i].is_sign_positive() {
                binary[i / 64] |= 1 << (i % 64);
            }
        }
        binary
    }

    fn vector_binarize_one(vec: &[Scalar<Self>]) -> Vec<Scalar<Self>> {
        let mut binary = vec![Scalar::<O>::one(); vec.len()];
        for i in 0..vec.len() {
            if vec[i].is_sign_negative() {
                binary[i] = -Scalar::<O>::one();
            }
        }
        binary
    }
}
