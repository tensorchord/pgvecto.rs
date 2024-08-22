use crate::distance::*;
use crate::operator::*;
use crate::vector::*;

#[derive(Debug, Clone, Copy)]
pub enum BVectorJaccard {}

impl Operator for BVectorJaccard {
    type Vector = BVectOwned;

    fn distance(lhs: Borrowed<'_, Self>, rhs: Borrowed<'_, Self>) -> Distance {
        lhs.operator_jaccard(rhs)
    }
}
