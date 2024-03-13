use crate::distance::*;
use crate::operator::*;
use crate::scalar::*;
use crate::vector::*;

#[derive(Debug, Clone, Copy)]
pub enum SVecf32Dot {}

impl Operator for SVecf32Dot {
    type VectorOwned = SVecf32Owned;

    const DISTANCE_KIND: DistanceKind = DistanceKind::Dot;

    fn distance(lhs: Borrowed<'_, Self>, rhs: Borrowed<'_, Self>) -> F32 {
        svecf32::dot(lhs, rhs) * (-1.0)
    }
}
