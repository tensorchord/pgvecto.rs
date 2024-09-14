pub mod sealed;

use quantization::rabitq::OperatorRabitqQuantization;
pub use sealed::SealedIndexing;

use base::operator::Operator;
use inverted::operator::OperatorInvertedIndex;
use ivf::operator::OperatorIvf;
use quantization::product::OperatorProductQuantization;
use quantization::scalar::OperatorScalarQuantization;

pub trait OperatorIndexing
where
    Self: Operator,
    Self: OperatorIvf,
    Self: OperatorInvertedIndex,
    Self: OperatorScalarQuantization,
    Self: OperatorProductQuantization,
    Self: OperatorRabitqQuantization,
{
}

impl<T> OperatorIndexing for T
where
    Self: Operator,
    Self: OperatorIvf,
    Self: OperatorInvertedIndex,
    Self: OperatorScalarQuantization,
    Self: OperatorProductQuantization,
    Self: OperatorRabitqQuantization,
{
}
