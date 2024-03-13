use crate::distance::*;
use crate::operator::*;
use crate::scalar::*;
use crate::vector::*;

#[derive(Debug, Clone, Copy)]
pub enum Vecf16L2 {}

impl Operator for Vecf16L2 {
    type VectorOwned = Vecf16Owned;

    const DISTANCE_KIND: DistanceKind = DistanceKind::L2;

    fn distance(lhs: Vecf16Borrowed<'_>, rhs: Vecf16Borrowed<'_>) -> F32 {
        vecf16::sl2(lhs.slice(), rhs.slice())
    }
}
