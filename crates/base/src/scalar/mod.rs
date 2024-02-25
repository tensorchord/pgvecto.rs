mod f16;
mod f32;
mod i8;

pub use f16::F16;
pub use f32::F32;
pub use i8::I8;

pub trait ScalarLike:
    Copy
    + Send
    + Sync
    + std::fmt::Debug
    + std::fmt::Display
    + serde::Serialize
    + for<'a> serde::Deserialize<'a>
    + Ord
    + bytemuck::Zeroable
    + bytemuck::Pod
    + num_traits::Float
    + num_traits::Zero
    + num_traits::NumOps
    + num_traits::NumAssignOps
{
    fn from_f32(x: f32) -> Self;
    fn to_f32(self) -> f32;
    fn from_f(x: F32) -> Self;
    fn to_f(self) -> F32;
}
