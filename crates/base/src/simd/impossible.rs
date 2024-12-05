use crate::simd::ScalarLike;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, PartialOrd)]
pub enum Impossible {}

impl Default for Impossible {
    fn default() -> Self {
        unimplemented!()
    }
}

impl ScalarLike for Impossible {
    fn zero() -> Self {
        unimplemented!()
    }

    fn infinity() -> Self {
        unimplemented!()
    }

    fn mask(self, _: bool) -> Self {
        unimplemented!()
    }

    fn scalar_neg(_: Self) -> Self {
        unimplemented!()
    }

    fn scalar_add(_: Self, _: Self) -> Self {
        unimplemented!()
    }

    fn scalar_sub(_: Self, _: Self) -> Self {
        unimplemented!()
    }

    fn scalar_mul(_: Self, _: Self) -> Self {
        unimplemented!()
    }

    fn scalar_is_sign_positive(self) -> bool {
        unimplemented!()
    }

    fn scalar_is_sign_negative(self) -> bool {
        unimplemented!()
    }

    fn from_f32(_: f32) -> Self {
        unimplemented!()
    }

    fn to_f32(self) -> f32 {
        unimplemented!()
    }

    fn reduce_or_of_is_zero(_this: &[Self]) -> bool {
        unimplemented!()
    }

    fn reduce_sum_of_x(_this: &[Self]) -> f32 {
        unimplemented!()
    }

    fn reduce_sum_of_abs_x(_this: &[Self]) -> f32 {
        unimplemented!()
    }

    fn reduce_sum_of_x2(_this: &[Self]) -> f32 {
        unimplemented!()
    }

    fn reduce_min_max_of_x(_this: &[Self]) -> (f32, f32) {
        unimplemented!()
    }

    fn reduce_sum_of_xy(_lhs: &[Self], _rhs: &[Self]) -> f32 {
        unimplemented!()
    }

    fn reduce_sum_of_d2(_lhs: &[Self], _rhs: &[Self]) -> f32 {
        unimplemented!()
    }

    fn reduce_sum_of_sparse_xy(
        _lidx: &[u32],
        _lval: &[Self],
        _ridx: &[u32],
        _rval: &[Self],
    ) -> f32 {
        unimplemented!()
    }

    fn reduce_sum_of_sparse_d2(
        _lidx: &[u32],
        _lval: &[Self],
        _ridx: &[u32],
        _rval: &[Self],
    ) -> f32 {
        unimplemented!()
    }

    fn vector_from_f32(_this: &[f32]) -> Vec<Self> {
        unimplemented!()
    }

    fn vector_to_f32(_this: &[Self]) -> Vec<f32> {
        unimplemented!()
    }

    #[allow(unreachable_code)]
    fn vector_to_f32_borrowed(_: &[Self]) -> impl AsRef<[f32]> {
        unimplemented!() as Vec<f32>
    }

    fn vector_add(_lhs: &[Self], _rhs: &[Self]) -> Vec<Self> {
        unimplemented!()
    }

    fn vector_add_inplace(_lhs: &mut [Self], _rhs: &[Self]) {
        unimplemented!()
    }

    fn vector_sub(_lhs: &[Self], _rhs: &[Self]) -> Vec<Self> {
        unimplemented!()
    }

    fn vector_mul(_lhs: &[Self], _rhs: &[Self]) -> Vec<Self> {
        unimplemented!()
    }

    fn vector_mul_scalar(_lhs: &[Self], _rhs: f32) -> Vec<Self> {
        unimplemented!()
    }

    fn vector_mul_scalar_inplace(_lhs: &mut [Self], _rhs: f32) {
        unimplemented!()
    }

    fn vector_abs_inplace(_this: &mut [Self]) {
        unimplemented!()
    }

    fn kmeans_helper(_this: &mut [Self], _x: f32, _y: f32) {
        unimplemented!()
    }
}
