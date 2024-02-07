mod f16;
mod f16_cos;
mod f16_dot;
mod f16_l2;
mod f32_cos;
mod f32_dot;
mod f32_l2;

pub use f16_cos::F16Cos;
pub use f16_dot::F16Dot;
pub use f16_l2::F16L2;
pub use f32_cos::F32Cos;
pub use f32_dot::F32Dot;
pub use f32_l2::F32L2;

use crate::prelude::*;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display};

pub trait G: Copy + Debug + 'static {
    type Scalar: Copy
        + Send
        + Sync
        + Debug
        + Display
        + Serialize
        + for<'a> Deserialize<'a>
        + Ord
        + bytemuck::Zeroable
        + bytemuck::Pod
        + Float
        + Zero
        + num_traits::NumOps
        + num_traits::NumAssignOps
        + FloatCast;
    const DISTANCE: Distance;
    type L2: G<Scalar = Self::Scalar>;

    fn distance(lhs: &[Self::Scalar], rhs: &[Self::Scalar]) -> F32;
    fn elkan_k_means_normalize(vector: &mut [Self::Scalar]);
    fn elkan_k_means_distance(lhs: &[Self::Scalar], rhs: &[Self::Scalar]) -> F32;
    fn scalar_quantization_distance(
        dims: u16,
        max: &[Self::Scalar],
        min: &[Self::Scalar],
        lhs: &[Self::Scalar],
        rhs: &[u8],
    ) -> F32;
    fn scalar_quantization_distance2(
        dims: u16,
        max: &[Self::Scalar],
        min: &[Self::Scalar],
        lhs: &[u8],
        rhs: &[u8],
    ) -> F32;
    fn product_quantization_distance(
        dims: u16,
        ratio: u16,
        centroids: &[Self::Scalar],
        lhs: &[Self::Scalar],
        rhs: &[u8],
    ) -> F32;
    fn product_quantization_distance2(
        dims: u16,
        ratio: u16,
        centroids: &[Self::Scalar],
        lhs: &[u8],
        rhs: &[u8],
    ) -> F32;
    fn product_quantization_distance_with_delta(
        dims: u16,
        ratio: u16,
        centroids: &[Self::Scalar],
        lhs: &[Self::Scalar],
        rhs: &[u8],
        delta: &[Self::Scalar],
    ) -> F32;
}

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DynamicVector {
    F32(Vec<F32>),
    F16(Vec<F16>),
}

impl From<Vec<F32>> for DynamicVector {
    fn from(value: Vec<F32>) -> Self {
        Self::F32(value)
    }
}

impl From<Vec<F16>> for DynamicVector {
    fn from(value: Vec<F16>) -> Self {
        Self::F16(value)
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Distance {
    L2,
    Cos,
    Dot,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Kind {
    F32,
    F16,
}

pub fn squared_norm<S: G>(dims: u16, vec: &[S::Scalar]) -> F32 {
    let mut result = F32::zero();
    for i in 0..dims as usize {
        result += F32((vec[i] * vec[i]).to_f32());
    }
    result
}

pub fn inner_product<S: G>(dims: u16, lhs: &[S::Scalar], rhs: &[S::Scalar]) -> F32 {
    let mut result = F32::zero();
    for i in 0..dims as usize {
        result += F32((lhs[i] * rhs[i]).to_f32());
    }
    result
}
