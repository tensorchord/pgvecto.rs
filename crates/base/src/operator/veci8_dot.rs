use crate::distance::*;
use crate::operator::*;
use crate::scalar::*;
use crate::vector::*;

#[derive(Debug, Clone, Copy)]
pub enum Veci8Dot {}

impl Operator for Veci8Dot {
    type VectorOwned = Veci8Owned;

    const DISTANCE_KIND: DistanceKind = DistanceKind::Dot;

    fn distance(lhs: Borrowed<'_, Self>, rhs: Borrowed<'_, Self>) -> F32 {
        veci8::dot(&lhs, &rhs) * (-1.0)
    }
}
