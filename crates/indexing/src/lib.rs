pub mod sealed;

pub use sealed::SealedIndexing;

use base::operator::Operator;
use ivf::operator::OperatorIvf;
use quantization::product::OperatorProductQuantization;
use quantization::rabitq::OperatorRabitqQuantization;
use quantization::rabitq4::OperatorRabitq4Quantization;
use quantization::rabitq8::OperatorRabitq8Quantization;
use quantization::scalar::OperatorScalarQuantization;
use quantization::scalar4::OperatorScalar4Quantization;
use quantization::scalar8::OperatorScalar8Quantization;
use sparse_inverted_index::operator::OperatorSparseInvertedIndex;

pub trait OperatorIndexing
where
    Self: Operator,
    Self: OperatorIvf,
    Self: OperatorSparseInvertedIndex,
    Self: OperatorScalarQuantization,
    Self: OperatorProductQuantization,
    Self: OperatorRabitqQuantization,
    Self: OperatorRabitq4Quantization,
    Self: OperatorRabitq8Quantization,
    Self: OperatorScalar4Quantization,
    Self: OperatorScalar8Quantization,
{
}

impl<T> OperatorIndexing for T
where
    Self: Operator,
    Self: OperatorIvf,
    Self: OperatorSparseInvertedIndex,
    Self: OperatorScalarQuantization,
    Self: OperatorProductQuantization,
    Self: OperatorRabitqQuantization,
    Self: OperatorRabitq4Quantization,
    Self: OperatorRabitq8Quantization,
    Self: OperatorScalar4Quantization,
    Self: OperatorScalar8Quantization,
{
}
