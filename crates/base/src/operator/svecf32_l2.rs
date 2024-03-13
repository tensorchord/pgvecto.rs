use crate::distance::*;
use crate::operator::*;
use crate::scalar::*;
use crate::vector::*;

#[derive(Debug, Clone, Copy)]
pub enum SVecf32L2 {}

impl Operator for SVecf32L2 {
    type VectorOwned = SVecf32Owned;

    const DISTANCE_KIND: DistanceKind = DistanceKind::L2;

    fn distance(lhs: SVecf32Borrowed<'_>, rhs: SVecf32Borrowed<'_>) -> F32 {
        svecf32::sl2(lhs, rhs)
    }
}
