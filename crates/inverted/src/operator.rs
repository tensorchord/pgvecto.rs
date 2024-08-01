use base::operator::*;
use base::scalar::F32;
use quantization::operator::OperatorQuantization;
use storage::OperatorStorage;

use std::iter::{zip, Empty};

pub trait OperatorInvertedIndex: OperatorQuantization + OperatorStorage {
    fn to_index_vec(vec: Borrowed<'_, Self>) -> impl Iterator<Item = (u32, F32)>;
}

impl OperatorInvertedIndex for SVecf32Dot {
    fn to_index_vec(vec: Borrowed<'_, Self>) -> impl Iterator<Item = (u32, F32)> {
        zip(vec.indexes().to_vec(), vec.values().to_vec())
    }
}

macro_rules! unimpl_operator_inverted_sparse {
    ($t:ty) => {
        impl OperatorInvertedIndex for $t {
            fn to_index_vec(_: Borrowed<'_, Self>) -> impl Iterator<Item = (u32, F32)> {
                #![allow(unreachable_code)]
                unimplemented!() as Empty<(u32, F32)>
            }
        }
    };
}

unimpl_operator_inverted_sparse!(SVecf32Cos);
unimpl_operator_inverted_sparse!(SVecf32L2);
unimpl_operator_inverted_sparse!(BVecf32Cos);
unimpl_operator_inverted_sparse!(BVecf32Dot);
unimpl_operator_inverted_sparse!(BVecf32Jaccard);
unimpl_operator_inverted_sparse!(BVecf32L2);
unimpl_operator_inverted_sparse!(Vecf32Cos);
unimpl_operator_inverted_sparse!(Vecf32Dot);
unimpl_operator_inverted_sparse!(Vecf32L2);
unimpl_operator_inverted_sparse!(Vecf16Cos);
unimpl_operator_inverted_sparse!(Vecf16Dot);
unimpl_operator_inverted_sparse!(Vecf16L2);
