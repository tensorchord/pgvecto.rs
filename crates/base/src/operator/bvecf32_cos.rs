use crate::distance::*;
use crate::operator::*;
use crate::scalar::*;
use crate::vector::*;

#[derive(Debug, Clone, Copy)]
pub enum BVecf32Cos {}

impl Operator for BVecf32Cos {
    type VectorOwned = BVecf32Owned;

    const DISTANCE_KIND: DistanceKind = DistanceKind::Cos;

    fn distance(lhs: Borrowed<'_, Self>, rhs: Borrowed<'_, Self>) -> F32 {
        F32(1.0) - bvecf32::cosine(lhs, rhs)
    }
}
