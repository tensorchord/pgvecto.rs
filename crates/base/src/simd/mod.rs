pub mod bit;
pub mod emulate;
mod f16;
mod f32;
pub mod fast_scan;
pub mod impossible;
pub mod packed_u4;
pub mod quantize;
pub mod u8;

pub trait ScalarLike:
    Copy
    + Send
    + Sync
    + std::fmt::Debug
    + serde::Serialize
    + for<'a> serde::Deserialize<'a>
    + Default
    + crate::pod::Pod
    + 'static
    + PartialEq
    + PartialOrd
{
    fn zero() -> Self;
    fn infinity() -> Self;
    fn mask(self, m: bool) -> Self;
    fn scalar_neg(this: Self) -> Self;
    fn scalar_add(lhs: Self, rhs: Self) -> Self;
    fn scalar_sub(lhs: Self, rhs: Self) -> Self;
    fn scalar_mul(lhs: Self, rhs: Self) -> Self;
    fn scalar_is_sign_positive(self) -> bool;
    fn scalar_is_sign_negative(self) -> bool;

    fn from_f32(x: f32) -> Self;
    fn to_f32(self) -> f32;

    fn reduce_or_of_is_zero(this: &[Self]) -> bool;
    fn reduce_sum_of_x(this: &[Self]) -> f32;
    fn reduce_sum_of_abs_x(this: &[Self]) -> f32;
    fn reduce_sum_of_x2(this: &[Self]) -> f32;
    fn reduce_min_max_of_x(this: &[Self]) -> (f32, f32);

    fn reduce_sum_of_xy(lhs: &[Self], rhs: &[Self]) -> f32;
    fn reduce_sum_of_d2(lhs: &[Self], rhs: &[Self]) -> f32;

    fn reduce_sum_of_sparse_xy(lidx: &[u32], lval: &[Self], ridx: &[u32], rval: &[Self]) -> f32;
    fn reduce_sum_of_sparse_d2(lidx: &[u32], lval: &[Self], ridx: &[u32], rval: &[Self]) -> f32;

    fn vector_from_f32(this: &[f32]) -> Vec<Self>;
    fn vector_to_f32(this: &[Self]) -> Vec<f32>;
    fn vector_to_f32_borrowed(this: &[Self]) -> impl AsRef<[f32]>;
    fn vector_add(lhs: &[Self], rhs: &[Self]) -> Vec<Self>;
    fn vector_add_inplace(lhs: &mut [Self], rhs: &[Self]);
    fn vector_sub(lhs: &[Self], rhs: &[Self]) -> Vec<Self>;
    fn vector_mul(lhs: &[Self], rhs: &[Self]) -> Vec<Self>;
    fn vector_mul_scalar(lhs: &[Self], rhs: f32) -> Vec<Self>;
    fn vector_mul_scalar_inplace(lhs: &mut [Self], rhs: f32);
    fn vector_abs_inplace(this: &mut [Self]);

    fn kmeans_helper(this: &mut [Self], x: f32, y: f32);
}

pub fn enable() {
    detect::init();
}
