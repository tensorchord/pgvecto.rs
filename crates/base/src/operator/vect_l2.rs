use crate::distance::*;
use crate::operator::*;
use crate::simd::ScalarLike;
use crate::vector::*;
use std::marker::PhantomData;

#[derive(Debug, Clone, Copy)]
pub struct VectL2<S>(std::convert::Infallible, PhantomData<fn(S) -> S>);

impl<S: ScalarLike> Operator for VectL2<S> {
    type Vector = VectOwned<S>;

    fn distance(lhs: VectBorrowed<'_, S>, rhs: VectBorrowed<'_, S>) -> Distance {
        lhs.operator_l2(rhs)
    }
}
