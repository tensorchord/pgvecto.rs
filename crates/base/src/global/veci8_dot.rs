use super::*;
use crate::distance::*;
use crate::scalar::*;
use crate::vector::*;
use num_traits::Float;

#[derive(Debug, Clone, Copy)]
pub enum Veci8Dot {}

impl Global for Veci8Dot {
    type VectorOwned = Veci8Owned;

    const VECTOR_KIND: VectorKind = VectorKind::Veci8;
    const DISTANCE_KIND: DistanceKind = DistanceKind::Dot;

    fn distance(lhs: Borrowed<'_, Self>, rhs: Borrowed<'_, Self>) -> F32 {
        super::veci8::dot_distance(&lhs, &rhs) * (-1.0)
    }
}

impl GlobalElkanKMeans for Veci8Dot {
    type VectorNormalized = Self::VectorOwned;

    fn elkan_k_means_normalize(vector: &mut [Scalar<Self>]) {
        super::vecf32::l2_normalize(vector)
    }

    fn elkan_k_means_normalize2(vector: Borrowed<'_, Self>) -> Veci8Owned {
        vector.normalize()
    }

    fn elkan_k_means_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        super::vecf32::dot(lhs, rhs).acos()
    }

    fn elkan_k_means_distance2(lhs: Borrowed<'_, Self>, rhs: &[Scalar<Self>]) -> F32 {
        super::veci8::dot_2(lhs, rhs).acos()
    }
}

impl GlobalScalarQuantization for Veci8Dot {
    fn scalar_quantization_distance(
        _dims: u16,
        _max: &[Scalar<Self>],
        _min: &[Scalar<Self>],
        _lhs: Borrowed<'_, Self>,
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }
    fn scalar_quantization_distance2(
        _dims: u16,
        _max: &[Scalar<Self>],
        _min: &[Scalar<Self>],
        _lhs: &[u8],
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }
}

impl GlobalProductQuantization for Veci8Dot {
    type ProductQuantizationL2 = Veci8Dot;

    fn product_quantization_distance(
        _dims: u32,
        _ratio: u32,
        _centroids: &[Scalar<Self>],
        _lhs: Borrowed<'_, Self>,
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }
    fn product_quantization_distance2(
        _dims: u32,
        _ratio: u32,
        _centroids: &[Scalar<Self>],
        _lhs: &[u8],
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }
    fn product_quantization_distance_with_delta(
        _dims: u32,
        _ratio: u32,
        _centroids: &[Scalar<Self>],
        _lhs: Borrowed<'_, Self>,
        _rhs: &[u8],
        _delta: &[Scalar<Self>],
    ) -> F32 {
        unimplemented!()
    }
    fn product_quantization_l2_distance(_lhs: &[Scalar<Self>], _rhs: &[Scalar<Self>]) -> F32 {
        unimplemented!()
    }
    fn product_quantization_dense_distance(_lhs: &[Scalar<Self>], _rhs: &[Scalar<Self>]) -> F32 {
        unimplemented!()
    }
}
