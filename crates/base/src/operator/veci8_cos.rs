use crate::distance::*;
use crate::operator::*;
use crate::scalar::*;
use crate::vector::*;

#[derive(Debug, Clone, Copy)]
pub enum Veci8Cos {}

impl Operator for Veci8Cos {
    type VectorOwned = Veci8Owned;

    const DISTANCE_KIND: DistanceKind = DistanceKind::Cos;

    fn distance(lhs: Borrowed<'_, Self>, rhs: Borrowed<'_, Self>) -> F32 {
        F32(1.0) - veci8::cosine(&lhs, &rhs)
    }
}
