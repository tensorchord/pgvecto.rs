use crate::product::operator::OperatorProductQuantization;
use crate::scalar::operator::OperatorScalarQuantization;
use base::operator::*;

pub trait OperatorQuantization: OperatorScalarQuantization + OperatorProductQuantization {}

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
