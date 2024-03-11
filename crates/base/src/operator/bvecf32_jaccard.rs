use crate::distance::*;
use crate::operator::*;
use crate::scalar::*;
use crate::vector::*;

#[derive(Debug, Clone, Copy)]
pub enum BVecf32Jaccard {}

impl Operator for BVecf32Jaccard {
    type VectorOwned = BVecf32Owned;

    const DISTANCE_KIND: DistanceKind = DistanceKind::Jaccard;

    fn distance(lhs: Borrowed<'_, Self>, rhs: Borrowed<'_, Self>) -> F32 {
        F32(1.) - bvecf32::jaccard(lhs, rhs)
    }
}
