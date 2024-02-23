mod f16;
mod f16_cos;
mod f16_dot;
mod f16_l2;
mod f32;
mod f32_cos;
mod f32_dot;
mod f32_l2;
pub mod i8;
mod i8_cos;
mod i8_dot;
mod i8_l2;
mod sparse_f32;
mod sparse_f32_cos;
mod sparse_f32_dot;
mod sparse_f32_l2;

pub use f16_cos::F16Cos;
pub use f16_dot::F16Dot;
pub use f16_l2::F16L2;
pub use f32_cos::F32Cos;
pub use f32_dot::F32Dot;
pub use f32_l2::F32L2;
pub use i8::{dequantization, quantization};
pub use i8_cos::I8Cos;
pub use i8_dot::I8Dot;
pub use i8_l2::I8L2;
pub use sparse_f32_cos::SparseF32Cos;
pub use sparse_f32_dot::SparseF32Dot;
pub use sparse_f32_l2::SparseF32L2;

use crate::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    fmt::{Debug, Display},
};

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
    type Storage: for<'a> Storage<VectorRef<'a> = Self::VectorRef<'a>>;
    type L2: for<'a> G<Scalar = Self::Scalar, VectorRef<'a> = &'a [Self::Scalar]>;
    type VectorOwned: Vector + Clone + Serialize + for<'a> Deserialize<'a>;
    type VectorRef<'a>: Vector + Copy + 'a
    where
        Self: 'a;

    const DISTANCE: Distance;
    const KIND: Kind;

    fn owned_to_ref(vector: &Self::VectorOwned) -> Self::VectorRef<'_>;
    fn ref_to_owned(vector: Self::VectorRef<'_>) -> Self::VectorOwned;
    fn to_dense(vector: Self::VectorRef<'_>) -> Cow<'_, [Self::Scalar]>;
    fn distance(lhs: Self::VectorRef<'_>, rhs: Self::VectorRef<'_>) -> F32;

    fn elkan_k_means_normalize(vector: &mut [Self::Scalar]);
    fn elkan_k_means_normalize2(vector: &mut Self::VectorOwned);
    fn elkan_k_means_distance(lhs: &[Self::Scalar], rhs: &[Self::Scalar]) -> F32;
    fn elkan_k_means_distance2(lhs: Self::VectorRef<'_>, rhs: &[Self::Scalar]) -> F32;

    fn scalar_quantization_distance(
        dims: u16,
        max: &[Self::Scalar],
        min: &[Self::Scalar],
        lhs: Self::VectorRef<'_>,
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
        lhs: Self::VectorRef<'_>,
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
        lhs: Self::VectorRef<'_>,
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

pub trait Vector {
    fn dims(&self) -> u16;
}

impl<T> Vector for Vec<T> {
    fn dims(&self) -> u16 {
        self.len().try_into().unwrap()
    }
}

impl<'a, T> Vector for &'a [T] {
    fn dims(&self) -> u16 {
        self.len().try_into().unwrap()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DynamicVector {
    F32(Vec<F32>),
    F16(Vec<F16>),
    SparseF32(SparseF32),
    I8(VecI8Owned),
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

impl From<SparseF32> for DynamicVector {
    fn from(value: SparseF32) -> Self {
        Self::SparseF32(value)
    }
}

impl From<VecI8Owned> for DynamicVector {
    fn from(value: VecI8Owned) -> Self {
        Self::I8(value)
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
    SparseF32,
    //TODO: I8
    I8,
}
