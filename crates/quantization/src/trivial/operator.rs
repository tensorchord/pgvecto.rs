use base::operator::*;
use base::scalar::*;
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
    ) -> F32;
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
    ) -> F32 {
        O::distance(preprocessed.as_borrowed(), rhs)
    }
}
