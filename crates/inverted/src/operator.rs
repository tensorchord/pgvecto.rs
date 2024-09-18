use base::{operator::*, scalar::ScalarLike};
use std::iter::{zip, Empty};
use storage::OperatorStorage;

pub trait OperatorInvertedIndex: OperatorStorage {
    fn to_index_vec(vec: Borrowed<'_, Self>) -> impl Iterator<Item = (u32, f32)> + '_;
}

impl OperatorInvertedIndex for SVectDot<f32> {
    fn to_index_vec(vec: Borrowed<'_, Self>) -> impl Iterator<Item = (u32, f32)> + '_ {
        zip(vec.indexes().iter().copied(), vec.values().iter().copied())
    }
}

macro_rules! unimpl_operator_inverted_index {
    ($t:ty) => {
        impl OperatorInvertedIndex for $t {
            fn to_index_vec(_: Borrowed<'_, Self>) -> impl Iterator<Item = (u32, f32)> + '_ {
                #![allow(unreachable_code)]
                unimplemented!() as Empty<(u32, f32)>
            }
        }
    };
}

impl<S: ScalarLike> OperatorInvertedIndex for VectDot<S> {
    fn to_index_vec(_: Borrowed<'_, Self>) -> impl Iterator<Item = (u32, f32)> + '_ {
        #![allow(unreachable_code)]
        unimplemented!() as Empty<(u32, f32)>
    }
}

impl<S: ScalarLike> OperatorInvertedIndex for VectL2<S> {
    fn to_index_vec(_: Borrowed<'_, Self>) -> impl Iterator<Item = (u32, f32)> + '_ {
        #![allow(unreachable_code)]
        unimplemented!() as Empty<(u32, f32)>
    }
}

unimpl_operator_inverted_index!(SVectL2<f32>);
unimpl_operator_inverted_index!(BVectorDot);
unimpl_operator_inverted_index!(BVectorJaccard);
unimpl_operator_inverted_index!(BVectorHamming);
