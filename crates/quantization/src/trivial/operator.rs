use base::distance::Distance;
use base::operator::*;
use base::vector::VectorBorrowed;
use base::vector::VectorOwned;

pub trait OperatorTrivialQuantization: Operator {
    type TrivialQuantizationPreprocessed;

    fn trivial_quantization_preprocess(
        lhs: Borrowed<'_, Self>,
    ) -> Self::TrivialQuantizationPreprocessed;

    fn trivial_quantization_process(
        preprocessed: &Self::TrivialQuantizationPreprocessed,
        rhs: Borrowed<'_, Self>,
    ) -> Distance;
}

impl<O: Operator> OperatorTrivialQuantization for O {
    type TrivialQuantizationPreprocessed = Owned<O>;

    fn trivial_quantization_preprocess(
        lhs: Borrowed<'_, Self>,
    ) -> Self::TrivialQuantizationPreprocessed {
        lhs.own()
    }

    fn trivial_quantization_process(
        preprocessed: &Self::TrivialQuantizationPreprocessed,
        rhs: Borrowed<'_, Self>,
    ) -> Distance {
        O::distance(preprocessed.as_borrowed(), rhs)
    }
}
