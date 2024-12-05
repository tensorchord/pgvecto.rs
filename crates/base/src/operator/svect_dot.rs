use crate::distance::*;
use crate::operator::*;
use crate::simd::ScalarLike;
use crate::vector::*;
use std::marker::PhantomData;

#[derive(Debug, Clone, Copy)]
pub struct SVectDot<S>(std::convert::Infallible, PhantomData<fn(S) -> S>);

impl<S: ScalarLike> Operator for SVectDot<S> {
    type Vector = SVectOwned<S>;

    fn distance(lhs: Borrowed<'_, Self>, rhs: Borrowed<'_, Self>) -> Distance {
        lhs.operator_dot(rhs)
    }
}
