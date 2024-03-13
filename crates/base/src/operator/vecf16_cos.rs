use crate::distance::*;
use crate::operator::*;
use crate::scalar::*;
use crate::vector::*;

#[derive(Debug, Clone, Copy)]
pub enum Vecf16Cos {}

impl Operator for Vecf16Cos {
    type VectorOwned = Vecf16Owned;

    const DISTANCE_KIND: DistanceKind = DistanceKind::Cos;

    fn distance(lhs: Vecf16Borrowed<'_>, rhs: Vecf16Borrowed<'_>) -> F32 {
        F32(1.0) - vecf16::cosine(lhs.slice(), rhs.slice())
    }
}
