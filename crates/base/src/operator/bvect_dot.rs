use crate::distance::*;
use crate::operator::*;
use crate::vector::*;

#[derive(Debug, Clone, Copy)]
pub enum BVectorDot {}

impl Operator for BVectorDot {
    type Vector = BVectOwned;

    fn distance(lhs: Borrowed<'_, Self>, rhs: Borrowed<'_, Self>) -> Distance {
        lhs.operator_dot(rhs)
    }
}
