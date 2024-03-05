mod bvecf32;
mod bvecf32_cos;
mod bvecf32_dot;
mod bvecf32_jaccard;
mod bvecf32_l2;
mod svecf32;
mod svecf32_cos;
mod svecf32_dot;
mod svecf32_l2;
mod vecf16;
mod vecf16_cos;
mod vecf16_dot;
mod vecf16_l2;
mod vecf32;
mod vecf32_cos;
mod vecf32_dot;
mod vecf32_l2;
mod veci8;
mod veci8_cos;
mod veci8_dot;
mod veci8_l2;

pub use bvecf32_cos::BVecf32Cos;
pub use bvecf32_dot::BVecf32Dot;
pub use bvecf32_jaccard::BVecf32Jaccard;
pub use bvecf32_l2::BVecf32L2;
pub use svecf32_cos::SVecf32Cos;
pub use svecf32_dot::SVecf32Dot;
pub use svecf32_l2::SVecf32L2;
pub use vecf16_cos::Vecf16Cos;
pub use vecf16_dot::Vecf16Dot;
pub use vecf16_l2::Vecf16L2;
pub use vecf32_cos::Vecf32Cos;
pub use vecf32_dot::Vecf32Dot;
pub use vecf32_l2::Vecf32L2;
pub use veci8_cos::Veci8Cos;
pub use veci8_dot::Veci8Dot;
pub use veci8_l2::Veci8L2;

use crate::distance::*;
use crate::scalar::*;
use crate::vector::*;

pub trait GlobalElkanKMeans: Global {
    type VectorNormalized: VectorOwned;

    fn elkan_k_means_normalize(vector: &mut [Scalar<Self>]);
    fn elkan_k_means_normalize2(vector: Borrowed<'_, Self>) -> Self::VectorNormalized;
    fn elkan_k_means_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32;
    fn elkan_k_means_distance2(
        lhs: <Self::VectorNormalized as VectorOwned>::Borrowed<'_>,
        rhs: &[Scalar<Self>],
    ) -> F32;
}

pub trait GlobalScalarQuantization: Global {
    fn scalar_quantization_distance(
        dims: u16,
        max: &[Scalar<Self>],
        min: &[Scalar<Self>],
        lhs: Borrowed<'_, Self>,
        rhs: &[u8],
    ) -> F32;
    fn scalar_quantization_distance2(
        dims: u16,
        max: &[Scalar<Self>],
        min: &[Scalar<Self>],
        lhs: &[u8],
        rhs: &[u8],
    ) -> F32;
}

pub trait GlobalProductQuantization: Global {
    type ProductQuantizationL2: Global<VectorOwned = Self::VectorOwned>
        + GlobalElkanKMeans
        + GlobalProductQuantization;
    fn product_quantization_distance(
        dims: u32,
        ratio: u32,
        centroids: &[Scalar<Self>],
        lhs: Borrowed<'_, Self>,
        rhs: &[u8],
    ) -> F32;
    fn product_quantization_distance2(
        dims: u32,
        ratio: u32,
        centroids: &[Scalar<Self>],
        lhs: &[u8],
        rhs: &[u8],
    ) -> F32;
    fn product_quantization_distance_with_delta(
        dims: u32,
        ratio: u32,
        centroids: &[Scalar<Self>],
        lhs: Borrowed<'_, Self>,
        rhs: &[u8],
        delta: &[Scalar<Self>],
    ) -> F32;
    fn product_quantization_l2_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32;
    fn product_quantization_dense_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32;
}

pub trait Global: Copy + 'static {
    type VectorOwned: VectorOwned;

    const VECTOR_KIND: VectorKind;
    const DISTANCE_KIND: DistanceKind;

    fn distance(lhs: Borrowed<'_, Self>, rhs: Borrowed<'_, Self>) -> F32;
}

pub type Owned<T> = <T as Global>::VectorOwned;
pub type Borrowed<'a, T> = <<T as Global>::VectorOwned as VectorOwned>::Borrowed<'a>;
pub type Scalar<T> = <<T as Global>::VectorOwned as VectorOwned>::Scalar;
