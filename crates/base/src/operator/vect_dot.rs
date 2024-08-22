use crate::distance::*;
use crate::operator::*;
use crate::scalar::ScalarLike;
use crate::vector::*;
use std::marker::PhantomData;

#[derive(Debug, Clone, Copy)]
pub struct VectDot<S>(std::convert::Infallible, PhantomData<fn(S) -> S>);

impl<S: ScalarLike> Operator for VectDot<S> {
    type Vector = VectOwned<S>;

    fn distance(lhs: VectBorrowed<'_, S>, rhs: VectBorrowed<'_, S>) -> Distance {
        lhs.operator_dot(rhs)
    }
}
