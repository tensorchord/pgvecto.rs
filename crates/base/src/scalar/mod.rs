mod f16;
mod f32;

pub use f16::F16;
pub use f32::F32;

pub trait FloatCast: Sized {
    fn from_f32(x: f32) -> Self;
    fn to_f32(self) -> f32;
    fn from_f(x: F32) -> Self {
        Self::from_f32(x.0)
    }
    fn to_f(self) -> F32 {
        F32(Self::to_f32(self))
    }
}
