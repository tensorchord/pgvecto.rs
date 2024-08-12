use crate::distance::*;
use crate::operator::*;
use crate::scalar::*;
use crate::vector::*;

#[derive(Debug, Clone, Copy)]
pub enum BVectorJaccard {}

impl Operator for BVectorJaccard {
    type VectorOwned = BVectorOwned;

    const DISTANCE_KIND: DistanceKind = DistanceKind::Jaccard;

    fn distance(lhs: Borrowed<'_, Self>, rhs: Borrowed<'_, Self>) -> F32 {
        F32(1.0) - bvector::jaccard(lhs, rhs)
    }
}
