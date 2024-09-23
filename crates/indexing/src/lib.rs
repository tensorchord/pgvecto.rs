pub mod sealed;

use quantization::rabitq::OperatorRabitqQuantization;
pub use sealed::SealedIndexing;

use base::operator::Operator;
use ivf::operator::OperatorIvf;
use quantization::product::OperatorProductQuantization;
use quantization::scalar::OperatorScalarQuantization;
use sparse_inverted_index::operator::OperatorSparseInvertedIndex;

pub trait OperatorIndexing
where
    Self: Operator,
    Self: OperatorIvf,
    Self: OperatorSparseInvertedIndex,
    Self: OperatorScalarQuantization,
    Self: OperatorProductQuantization,
    Self: OperatorRabitqQuantization,
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
{
}
