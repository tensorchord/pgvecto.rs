use crate::distance::*;
use crate::operator::*;
use crate::scalar::*;
use crate::vector::*;

#[derive(Debug, Clone, Copy)]
pub enum BVecf32Dot {}

impl Operator for BVecf32Dot {
    type VectorOwned = BVecf32Owned;

    const DISTANCE_KIND: DistanceKind = DistanceKind::Dot;

    fn distance(lhs: Borrowed<'_, Self>, rhs: Borrowed<'_, Self>) -> F32 {
        bvecf32::dot(lhs, rhs) * (-1.0)
    }
}
