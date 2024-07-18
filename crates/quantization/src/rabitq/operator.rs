use base::operator::{Operator, Owned};

pub trait OperatorRaBitQ: Operator {
    type RabitQuantizationPreprocessed;
}

impl<O: Operator> OperatorRaBitQ for O {
    type RabitQuantizationPreprocessed = Owned<O>;
}
