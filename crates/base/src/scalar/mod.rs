mod f32;
mod half_f16;

use std::iter::Sum;

pub use f32::F32;
pub use half_f16::F16;

pub trait ScalarLike:
    Copy
    + Send
    + Sync
    + std::fmt::Debug
    + serde::Serialize
    + for<'a> serde::Deserialize<'a>
    + Ord
    + num_traits::Float
    + num_traits::NumAssignOps
    + Default
    + crate::pod::Pod
    + Sum
{
    fn from_f32(x: f32) -> Self;
    fn to_f32(self) -> f32;
    fn from_f(x: F32) -> Self;
    fn to_f(self) -> F32;

    fn impl_l2(lhs: &[Self], rhs: &[Self]) -> F32;
}
