use base::operator::*;
use base::vector::*;
use num_traits::Zero;
use quantization::operator::OperatorQuantization;
use storage::OperatorStorage;

pub trait OperatorIvf: OperatorQuantization + OperatorStorage {
    const RESIDUAL: bool;
    fn residual(lhs: Borrowed<'_, Self>, rhs: &[Scalar<Self>]) -> Owned<Self>;
    fn residual_dense(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> Owned<Self>;
}

impl OperatorIvf for BVecf32Dot {
    const RESIDUAL: bool = false;
    fn residual(_lhs: Borrowed<'_, Self>, _rhs: &[Scalar<Self>]) -> Owned<Self> {
        unimplemented!()
    }
    fn residual_dense(_lhs: &[Scalar<Self>], _rhs: &[Scalar<Self>]) -> Owned<Self> {
        unimplemented!()
    }
}

impl OperatorIvf for BVecf32Cos {
    const RESIDUAL: bool = false;
    fn residual(_lhs: Borrowed<'_, Self>, _rhs: &[Scalar<Self>]) -> Owned<Self> {
        unimplemented!()
    }
    fn residual_dense(_lhs: &[Scalar<Self>], _rhs: &[Scalar<Self>]) -> Owned<Self> {
        unimplemented!()
    }
}

impl OperatorIvf for BVecf32Jaccard {
    const RESIDUAL: bool = false;
    fn residual(_lhs: Borrowed<'_, Self>, _rhs: &[Scalar<Self>]) -> Owned<Self> {
        unimplemented!()
    }
    fn residual_dense(_lhs: &[Scalar<Self>], _rhs: &[Scalar<Self>]) -> Owned<Self> {
        unimplemented!()
    }
}

impl OperatorIvf for BVecf32L2 {
    const RESIDUAL: bool = false;
    fn residual(_lhs: Borrowed<'_, Self>, _rhs: &[Scalar<Self>]) -> Owned<Self> {
        unimplemented!()
    }
    fn residual_dense(_lhs: &[Scalar<Self>], _rhs: &[Scalar<Self>]) -> Owned<Self> {
        unimplemented!()
    }
}

impl OperatorIvf for SVecf32Dot {
    const RESIDUAL: bool = false;
    fn residual(_lhs: Borrowed<'_, Self>, _rhs: &[Scalar<Self>]) -> Owned<Self> {
        unimplemented!()
    }
    fn residual_dense(_lhs: &[Scalar<Self>], _rhs: &[Scalar<Self>]) -> Owned<Self> {
        unimplemented!()
    }
}

impl OperatorIvf for SVecf32Cos {
    const RESIDUAL: bool = false;
    fn residual(_lhs: Borrowed<'_, Self>, _rhs: &[Scalar<Self>]) -> Owned<Self> {
        unimplemented!()
    }
    fn residual_dense(_lhs: &[Scalar<Self>], _rhs: &[Scalar<Self>]) -> Owned<Self> {
        unimplemented!()
    }
}

impl OperatorIvf for SVecf32L2 {
    const RESIDUAL: bool = true;
    fn residual(lhs: Borrowed<'_, Self>, rhs: &[Scalar<Self>]) -> Owned<Self> {
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
    fn residual_dense(_lhs: &[Scalar<Self>], _rhs: &[Scalar<Self>]) -> Owned<Self> {
        unimplemented!()
    }
}

impl OperatorIvf for Vecf32Dot {
    const RESIDUAL: bool = false;
    fn residual(_lhs: Borrowed<'_, Self>, _rhs: &[Scalar<Self>]) -> Owned<Self> {
        unimplemented!()
    }
    fn residual_dense(_lhs: &[Scalar<Self>], _rhs: &[Scalar<Self>]) -> Owned<Self> {
        unimplemented!()
    }
}

impl OperatorIvf for Vecf32Cos {
    const RESIDUAL: bool = false;
    fn residual(_lhs: Borrowed<'_, Self>, _rhs: &[Scalar<Self>]) -> Owned<Self> {
        unimplemented!()
    }
    fn residual_dense(_lhs: &[Scalar<Self>], _rhs: &[Scalar<Self>]) -> Owned<Self> {
        unimplemented!()
    }
}

impl OperatorIvf for Vecf32L2 {
    const RESIDUAL: bool = true;
    fn residual(lhs: Borrowed<'_, Self>, rhs: &[Scalar<Self>]) -> Owned<Self> {
        lhs.operator_minus(Vecf32Borrowed::new(rhs))
    }
    fn residual_dense(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> Owned<Self> {
        let mut res = vec![Scalar::<Self>::zero(); lhs.len()];
        for i in 0..lhs.len() {
            res[i] = lhs[i] - rhs[i];
        }
        Vecf32Owned::new(res)
    }
}

impl OperatorIvf for Vecf16Dot {
    const RESIDUAL: bool = false;
    fn residual(_lhs: Borrowed<'_, Self>, _rhs: &[Scalar<Self>]) -> Owned<Self> {
        unimplemented!()
    }
    fn residual_dense(_lhs: &[Scalar<Self>], _rhs: &[Scalar<Self>]) -> Owned<Self> {
        unimplemented!()
    }
}

impl OperatorIvf for Vecf16Cos {
    const RESIDUAL: bool = false;
    fn residual(_lhs: Borrowed<'_, Self>, _rhs: &[Scalar<Self>]) -> Owned<Self> {
        unimplemented!()
    }
    fn residual_dense(_lhs: &[Scalar<Self>], _rhs: &[Scalar<Self>]) -> Owned<Self> {
        unimplemented!()
    }
}

impl OperatorIvf for Vecf16L2 {
    const RESIDUAL: bool = true;
    fn residual(lhs: Borrowed<'_, Self>, rhs: &[Scalar<Self>]) -> Owned<Self> {
        lhs.operator_minus(Vecf16Borrowed::new(rhs))
    }
    fn residual_dense(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> Owned<Self> {
        let mut res = vec![Scalar::<Self>::zero(); lhs.len()];
        for i in 0..lhs.len() {
            res[i] = lhs[i] - rhs[i];
        }
        Vecf16Owned::new(res)
    }
}
