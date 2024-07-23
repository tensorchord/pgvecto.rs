use base::operator::*;
use base::scalar::F32;
use quantization::operator::OperatorQuantization;
use storage::OperatorStorage;

pub trait OperatorInverted: OperatorQuantization + OperatorStorage {
    fn to_index_vec(vec: Borrowed<'_, Self>) -> Vec<(u32, F32)>;
}

impl OperatorInverted for SVecf32Dot {
    fn to_index_vec(vec: Borrowed<'_, Self>) -> Vec<(u32, F32)> {
        std::iter::zip(vec.indexes().to_vec(), vec.values().to_vec()).collect()
    }
}

macro_rules! unimpl_operator_inverted {
    ($t:ty) => {
        impl OperatorInverted for $t {
            fn to_index_vec(_: Borrowed<'_, Self>) -> Vec<(u32, F32)> {
                unimplemented!()
            }
        }
    };
}

unimpl_operator_inverted!(SVecf32Cos);
unimpl_operator_inverted!(SVecf32L2);
unimpl_operator_inverted!(BVecf32Cos);
unimpl_operator_inverted!(BVecf32Dot);
unimpl_operator_inverted!(BVecf32Jaccard);
unimpl_operator_inverted!(BVecf32L2);
unimpl_operator_inverted!(Vecf32Cos);
unimpl_operator_inverted!(Vecf32Dot);
unimpl_operator_inverted!(Vecf32L2);
unimpl_operator_inverted!(Vecf16Cos);
unimpl_operator_inverted!(Vecf16Dot);
unimpl_operator_inverted!(Vecf16L2);
