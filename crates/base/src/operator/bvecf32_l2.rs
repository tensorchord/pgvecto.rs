use crate::distance::*;
use crate::operator::*;
use crate::scalar::*;
use crate::vector::*;

#[derive(Debug, Clone, Copy)]
pub enum BVecf32L2 {}

impl Operator for BVecf32L2 {
    type VectorOwned = BVecf32Owned;

    const DISTANCE_KIND: DistanceKind = DistanceKind::L2;

    fn distance(lhs: Borrowed<'_, Self>, rhs: Borrowed<'_, Self>) -> F32 {
        bvecf32::sl2(lhs, rhs)
    }
}
