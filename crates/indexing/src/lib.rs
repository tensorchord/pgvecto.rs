pub mod sealed;

pub use sealed::SealedIndexing;

use base::operator::Operator;
use inverted::operator::OperatorInvertedIndex;
use ivf::operator::OperatorIvf;
use rabitq::operator::OperatorRabitq;

pub trait OperatorIndexing:
    Operator + OperatorIvf + OperatorInvertedIndex + OperatorRabitq
{
}

impl<T: Operator + OperatorIvf + OperatorInvertedIndex + OperatorRabitq> OperatorIndexing for T {}
