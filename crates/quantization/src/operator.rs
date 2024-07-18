use crate::product::operator::OperatorProductQuantization;
use crate::rabitq::operator::OperatorRaBitQ;
use crate::scalar::operator::OperatorScalarQuantization;
use crate::trivial::operator::OperatorTrivialQuantization;
use base::operator::*;

pub trait OperatorQuantization:
    OperatorTrivialQuantization
    + OperatorScalarQuantization
    + OperatorProductQuantization
    + OperatorRaBitQ
{
}

impl OperatorQuantization for BVecf32Cos {}
impl OperatorQuantization for BVecf32Dot {}
impl OperatorQuantization for BVecf32Jaccard {}
impl OperatorQuantization for BVecf32L2 {}
impl OperatorQuantization for SVecf32Cos {}
impl OperatorQuantization for SVecf32Dot {}
impl OperatorQuantization for SVecf32L2 {}
impl OperatorQuantization for Vecf16Cos {}
impl OperatorQuantization for Vecf16Dot {}
impl OperatorQuantization for Vecf16L2 {}
impl OperatorQuantization for Vecf32Cos {}
impl OperatorQuantization for Vecf32Dot {}
impl OperatorQuantization for Vecf32L2 {}
impl OperatorQuantization for Veci8Cos {}
impl OperatorQuantization for Veci8Dot {}
impl OperatorQuantization for Veci8L2 {}
