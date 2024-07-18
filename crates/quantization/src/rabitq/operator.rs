use base::operator::Operator;

pub trait OperatorRaBitQ: Operator {}

impl<O: Operator> OperatorRaBitQ for O {}
