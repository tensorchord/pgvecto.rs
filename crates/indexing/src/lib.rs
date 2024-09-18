pub mod sealed;

pub use sealed::SealedIndexing;

use base::operator::Operator;
use inverted::operator::OperatorInvertedIndex;
use ivf::operator::OperatorIvf;
use quantization::product::OperatorProductQuantization;
use quantization::scalar::OperatorScalarQuantization;
use rabitq::operator::OperatorRabitq;

pub trait OperatorIndexing
where
    Self: Operator,
    Self: OperatorIvf,
    Self: OperatorInvertedIndex,
    Self: OperatorRabitq,
    Self: OperatorScalarQuantization,
    Self: OperatorProductQuantization,
{
}

impl<T> OperatorIndexing for T
where
    Self: Operator,
    Self: OperatorIvf,
    Self: OperatorInvertedIndex,
    Self: OperatorRabitq,
    Self: OperatorScalarQuantization,
    Self: OperatorProductQuantization,
{
}
