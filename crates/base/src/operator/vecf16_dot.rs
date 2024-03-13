use crate::distance::*;
use crate::operator::*;
use crate::scalar::*;
use crate::vector::*;

#[derive(Debug, Clone, Copy)]
pub enum Vecf16Dot {}

impl Operator for Vecf16Dot {
    type VectorOwned = Vecf16Owned;

    const DISTANCE_KIND: DistanceKind = DistanceKind::Dot;

    fn distance(lhs: Vecf16Borrowed<'_>, rhs: Vecf16Borrowed<'_>) -> F32 {
        vecf16::dot(lhs.slice(), rhs.slice()) * (-1.0)
    }
}
