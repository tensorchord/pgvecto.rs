use base::operator::*;
use base::scalar::F32;
use quantization::operator::OperatorQuantization;
use storage::OperatorStorage;

use std::iter::{zip, Empty};

pub trait OperatorInvertedIndex: OperatorQuantization + OperatorStorage {
    fn to_index_vec(vec: Borrowed<'_, Self>) -> impl Iterator<Item = (u32, F32)> + '_;
}

impl OperatorInvertedIndex for SVecf32Dot {
    fn to_index_vec(vec: Borrowed<'_, Self>) -> impl Iterator<Item = (u32, F32)> + '_ {
        zip(vec.indexes().iter().copied(), vec.values().iter().copied())
    }
}

macro_rules! unimpl_operator_inverted_index {
    ($t:ty) => {
        impl OperatorInvertedIndex for $t {
            fn to_index_vec(_: Borrowed<'_, Self>) -> impl Iterator<Item = (u32, F32)> + '_ {
                #![allow(unreachable_code)]
                unimplemented!() as Empty<(u32, F32)>
            }
        }
    };
}

unimpl_operator_inverted_index!(SVecf32L2);
unimpl_operator_inverted_index!(BVectorDot);
unimpl_operator_inverted_index!(BVectorJaccard);
unimpl_operator_inverted_index!(BVectorHamming);
unimpl_operator_inverted_index!(Vecf32Dot);
unimpl_operator_inverted_index!(Vecf32L2);
unimpl_operator_inverted_index!(Vecf16Dot);
unimpl_operator_inverted_index!(Vecf16L2);
