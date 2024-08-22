use crate::distance::*;
use crate::operator::*;
use crate::vector::*;

#[derive(Debug, Clone, Copy)]
pub enum BVectorJaccard {}

impl Operator for BVectorJaccard {
    type VectorOwned = BVectorOwned;

    const DISTANCE_KIND: DistanceKind = DistanceKind::Jaccard;

    fn distance(lhs: Borrowed<'_, Self>, rhs: Borrowed<'_, Self>) -> Distance {
        lhs.operator_jaccard(rhs)
    }
}
