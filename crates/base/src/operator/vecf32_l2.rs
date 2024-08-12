use crate::distance::*;
use crate::operator::*;
use crate::scalar::*;
use crate::vector::*;

#[derive(Debug, Clone, Copy)]
pub enum Vecf32L2 {}

impl Operator for Vecf32L2 {
    type VectorOwned = Vecf32Owned;

    const DISTANCE_KIND: DistanceKind = DistanceKind::L2;

    fn distance(lhs: Vecf32Borrowed<'_>, rhs: Vecf32Borrowed<'_>) -> F32 {
        lhs.operator_l2(rhs)
    }
}
