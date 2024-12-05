use base::operator::*;
use base::simd::ScalarLike;
use std::iter::{zip, Empty};
use storage::OperatorStorage;

pub trait OperatorSparseInvertedIndex: OperatorStorage {
    fn to_index_vec(vec: Borrowed<'_, Self>) -> impl Iterator<Item = (u32, f32)> + '_;
}

impl<S: ScalarLike> OperatorSparseInvertedIndex for SVectDot<S> {
    fn to_index_vec(vector: Borrowed<'_, Self>) -> impl Iterator<Item = (u32, f32)> + '_ {
        zip(
            vector.indexes().iter().copied(),
            vector.values().iter().copied().map(S::to_f32),
        )
    }
}

impl<S: ScalarLike> OperatorSparseInvertedIndex for SVectL2<S> {
    fn to_index_vec(_: Borrowed<'_, Self>) -> impl Iterator<Item = (u32, f32)> + '_ {
        #![allow(unreachable_code)]
        unimplemented!() as Empty<(u32, f32)>
    }
}

impl<S: ScalarLike> OperatorSparseInvertedIndex for VectDot<S> {
    fn to_index_vec(_: Borrowed<'_, Self>) -> impl Iterator<Item = (u32, f32)> + '_ {
        #![allow(unreachable_code)]
        unimplemented!() as Empty<(u32, f32)>
    }
}

impl<S: ScalarLike> OperatorSparseInvertedIndex for VectL2<S> {
    fn to_index_vec(_: Borrowed<'_, Self>) -> impl Iterator<Item = (u32, f32)> + '_ {
        #![allow(unreachable_code)]
        unimplemented!() as Empty<(u32, f32)>
    }
}

macro_rules! unimpl_operator_inverted_index {
    ($t:ty) => {
        impl OperatorSparseInvertedIndex for $t {
            #![allow(unreachable_code)]
            fn to_index_vec(_: Borrowed<'_, Self>) -> impl Iterator<Item = (u32, f32)> + '_ {
                unimplemented!() as Empty<(u32, f32)>
            }
        }
    };
}

unimpl_operator_inverted_index!(BVectorDot);
unimpl_operator_inverted_index!(BVectorJaccard);
unimpl_operator_inverted_index!(BVectorHamming);
