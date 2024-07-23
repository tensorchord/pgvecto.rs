use base::operator::*;
use base::scalar::{ScalarLike, F32};
use base::vector::*;
use num_traits::Float;
use num_traits::Zero;
use quantization::operator::OperatorQuantization;
use storage::OperatorStorage;

pub trait OperatorIvf: OperatorQuantization + OperatorStorage {
    fn elkan_k_means_normalize(_: &mut [Scalar<Self>]) {}
    fn vector_sub(lhs: Borrowed<'_, Self>, rhs: &[Scalar<Self>]) -> Owned<Self>;
}

impl OperatorIvf for BVecf32Dot {
    fn vector_sub(_lhs: Borrowed<'_, Self>, _rhs: &[Scalar<Self>]) -> Owned<Self> {
        unimplemented!()
    }
}

impl OperatorIvf for BVecf32Cos {
    fn elkan_k_means_normalize(vector: &mut [Scalar<Self>]) {
        let n = vector.len();
        let mut dot = F32::zero();
        for i in 0..n {
            dot += vector[i].to_f() * vector[i].to_f();
        }
        let l = dot.sqrt();
        for i in 0..n {
            vector[i] /= Scalar::<Self>::from_f(l);
        }
    }
    fn vector_sub(_lhs: Borrowed<'_, Self>, _rhs: &[Scalar<Self>]) -> Owned<Self> {
        unimplemented!()
    }
}

impl OperatorIvf for BVecf32Jaccard {
    fn vector_sub(_lhs: Borrowed<'_, Self>, _rhs: &[Scalar<Self>]) -> Owned<Self> {
        unimplemented!()
    }
}

impl OperatorIvf for BVecf32L2 {
    fn vector_sub(_lhs: Borrowed<'_, Self>, _rhs: &[Scalar<Self>]) -> Owned<Self> {
        unimplemented!()
    }
}

impl OperatorIvf for SVecf32Dot {
    fn vector_sub(_lhs: Borrowed<'_, Self>, _rhs: &[Scalar<Self>]) -> Owned<Self> {
        unimplemented!()
    }
}

impl OperatorIvf for SVecf32Cos {
    fn elkan_k_means_normalize(vector: &mut [Scalar<Self>]) {
        let n = vector.len();
        let mut dot = F32::zero();
        for i in 0..n {
            dot += vector[i].to_f() * vector[i].to_f();
        }
        let l = dot.sqrt();
        for i in 0..n {
            vector[i] /= Scalar::<Self>::from_f(l);
        }
    }
    fn vector_sub(_lhs: Borrowed<'_, Self>, _rhs: &[Scalar<Self>]) -> Owned<Self> {
        unimplemented!()
    }
}

impl OperatorIvf for SVecf32L2 {
    fn vector_sub(lhs: Borrowed<'_, Self>, rhs: &[Scalar<Self>]) -> Owned<Self> {
        assert_eq!(lhs.dims() as usize, rhs.len());
        let n = lhs.dims();
        let mut indexes = Vec::new();
        let mut values = Vec::new();
        let mut j = 0_usize;
        for i in 0..n {
            if lhs.indexes().get(j).copied() == Some(i) {
                let val = lhs.values()[j] - rhs[j];
                if !val.is_zero() {
                    indexes.push(i);
                    values.push(val);
                }
                j += 1;
            } else {
                let val = -rhs[j];
                if !val.is_zero() {
                    indexes.push(i);
                    values.push(val);
                }
            }
        }
        SVecf32Owned::new(lhs.dims(), indexes, values)
    }
}

impl OperatorIvf for Vecf32Dot {
    fn vector_sub(_lhs: Borrowed<'_, Self>, _rhs: &[Scalar<Self>]) -> Owned<Self> {
        unimplemented!()
    }
}

impl OperatorIvf for Vecf32Cos {
    fn elkan_k_means_normalize(vector: &mut [Scalar<Self>]) {
        let n = vector.len();
        let mut dot = F32::zero();
        for i in 0..n {
            dot += vector[i].to_f() * vector[i].to_f();
        }
        let l = dot.sqrt();
        for i in 0..n {
            vector[i] /= Scalar::<Self>::from_f(l);
        }
    }
    fn vector_sub(_lhs: Borrowed<'_, Self>, _rhs: &[Scalar<Self>]) -> Owned<Self> {
        unimplemented!()
    }
}

impl OperatorIvf for Vecf32L2 {
    fn vector_sub(lhs: Borrowed<'_, Self>, rhs: &[Scalar<Self>]) -> Owned<Self> {
        lhs.operator_minus(Vecf32Borrowed::new(rhs))
    }
}

impl OperatorIvf for Vecf16Dot {
    fn vector_sub(_lhs: Borrowed<'_, Self>, _rhs: &[Scalar<Self>]) -> Owned<Self> {
        unimplemented!()
    }
}

impl OperatorIvf for Vecf16Cos {
    fn elkan_k_means_normalize(vector: &mut [Scalar<Self>]) {
        let n = vector.len();
        let mut dot = F32::zero();
        for i in 0..n {
            dot += vector[i].to_f() * vector[i].to_f();
        }
        let l = dot.sqrt();
        for i in 0..n {
            vector[i] /= Scalar::<Self>::from_f(l);
        }
    }
    fn vector_sub(_lhs: Borrowed<'_, Self>, _rhs: &[Scalar<Self>]) -> Owned<Self> {
        unimplemented!()
    }
}

impl OperatorIvf for Vecf16L2 {
    fn vector_sub(lhs: Borrowed<'_, Self>, rhs: &[Scalar<Self>]) -> Owned<Self> {
        lhs.operator_minus(Vecf16Borrowed::new(rhs))
    }
}

impl OperatorIvf for Veci8Dot {
    fn vector_sub(_lhs: Borrowed<'_, Self>, _rhs: &[Scalar<Self>]) -> Owned<Self> {
        unimplemented!()
    }
}

impl OperatorIvf for Veci8Cos {
    fn elkan_k_means_normalize(vector: &mut [Scalar<Self>]) {
        let n = vector.len();
        let mut dot = F32::zero();
        for i in 0..n {
            dot += vector[i].to_f() * vector[i].to_f();
        }
        let l = dot.sqrt();
        for i in 0..n {
            vector[i] /= Scalar::<Self>::from_f(l);
        }
    }
    fn vector_sub(_lhs: Borrowed<'_, Self>, _rhs: &[Scalar<Self>]) -> Owned<Self> {
        unimplemented!()
    }
}

impl OperatorIvf for Veci8L2 {
    fn vector_sub(_lhs: Borrowed<'_, Self>, _rhs: &[Scalar<Self>]) -> Owned<Self> {
        unimplemented!()
    }
}
