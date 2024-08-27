use crate::distance::*;
use crate::operator::*;
use crate::scalar::ScalarLike;
use crate::vector::*;
use std::marker::PhantomData;

#[derive(Debug, Clone, Copy)]
pub struct SVectL2<S>(std::convert::Infallible, PhantomData<fn(S) -> S>);

impl<S: ScalarLike> Operator for SVectL2<S> {
    type Vector = SVectOwned<S>;

    fn distance(lhs: SVectBorrowed<'_, S>, rhs: SVectBorrowed<'_, S>) -> Distance {
        lhs.operator_l2(rhs)
    }
}
