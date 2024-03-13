use crate::distance::*;
use crate::operator::*;
use crate::scalar::*;
use crate::vector::*;

#[derive(Debug, Clone, Copy)]
pub enum Vecf32Cos {}

impl Operator for Vecf32Cos {
    type VectorOwned = Vecf32Owned;

    const DISTANCE_KIND: DistanceKind = DistanceKind::Cos;

    fn distance(lhs: Vecf32Borrowed<'_>, rhs: Vecf32Borrowed<'_>) -> F32 {
        F32(1.0) - vecf32::cosine(lhs.slice(), rhs.slice())
    }
}
