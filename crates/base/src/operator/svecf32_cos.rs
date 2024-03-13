use crate::distance::*;
use crate::operator::*;
use crate::scalar::*;
use crate::vector::*;

#[derive(Debug, Clone, Copy)]
pub enum SVecf32Cos {}

impl Operator for SVecf32Cos {
    type VectorOwned = SVecf32Owned;

    const DISTANCE_KIND: DistanceKind = DistanceKind::Cos;

    fn distance(lhs: Borrowed<'_, Self>, rhs: Borrowed<'_, Self>) -> F32 {
        F32(1.0) - svecf32::cosine(lhs, rhs)
    }
}
