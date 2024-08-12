use crate::distance::*;
use crate::operator::*;
use crate::scalar::*;
use crate::vector::*;

#[derive(Debug, Clone, Copy)]
pub enum BVectorHamming {}

impl Operator for BVectorHamming {
    type VectorOwned = BVectorOwned;

    const DISTANCE_KIND: DistanceKind = DistanceKind::Hamming;

    fn distance(lhs: Borrowed<'_, Self>, rhs: Borrowed<'_, Self>) -> F32 {
        bvector::hamming(lhs, rhs)
    }
}
