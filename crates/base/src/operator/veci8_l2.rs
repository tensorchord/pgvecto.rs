use crate::distance::*;
use crate::operator::*;
use crate::scalar::*;
use crate::vector::*;

#[derive(Debug, Clone, Copy)]
pub enum Veci8L2 {}

impl Operator for Veci8L2 {
    type VectorOwned = Veci8Owned;

    const DISTANCE_KIND: DistanceKind = DistanceKind::Dot;

    fn distance(lhs: Borrowed<'_, Self>, rhs: Borrowed<'_, Self>) -> F32 {
        veci8::sl2(&lhs, &rhs)
    }
}
