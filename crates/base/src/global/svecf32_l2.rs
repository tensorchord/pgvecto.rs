use super::*;
use crate::distance::*;
use crate::scalar::*;
use crate::vector::*;
use num_traits::Float;

#[derive(Debug, Clone, Copy)]
pub enum SVecf32L2 {}

impl Global for SVecf32L2 {
    type VectorOwned = SVecf32Owned;

    const VECTOR_KIND: VectorKind = VectorKind::SVecf32;
    const DISTANCE_KIND: DistanceKind = DistanceKind::L2;

    fn distance(lhs: SVecf32Borrowed<'_>, rhs: SVecf32Borrowed<'_>) -> F32 {
        super::svecf32::sl2(lhs, rhs)
    }
}

impl GlobalElkanKMeans for SVecf32L2 {
    type VectorNormalized = Self::VectorOwned;

    fn elkan_k_means_normalize(_: &mut [Scalar<Self>]) {}

    fn elkan_k_means_normalize2(vector: SVecf32Borrowed<'_>) -> SVecf32Owned {
        vector.for_own()
    }

    fn elkan_k_means_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        super::vecf32::sl2(lhs, rhs).sqrt()
    }

    fn elkan_k_means_distance2(lhs: SVecf32Borrowed<'_>, rhs: &[Scalar<Self>]) -> F32 {
        super::svecf32::sl2_2(lhs, rhs).sqrt()
    }
}

impl GlobalScalarQuantization for SVecf32L2 {
    fn scalar_quantization_distance(
        _dims: u16,
        _max: &[Scalar<Self>],
        _min: &[Scalar<Self>],
        _lhs: SVecf32Borrowed<'_>,
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

impl GlobalProductQuantization for SVecf32L2 {
    type ProductQuantizationL2 = SVecf32L2;

    fn product_quantization_distance(
        _dims: u32,
        _ratio: u32,
        _centroids: &[Scalar<Self>],
        _lhs: SVecf32Borrowed<'_>,
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
        _lhs: SVecf32Borrowed<'_>,
        _rhs: &[u8],
        _delta: &[Scalar<Self>],
    ) -> F32 {
        unimplemented!()
    }

    fn product_quantization_l2_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        super::vecf32::sl2(lhs, rhs)
    }

    fn product_quantization_dense_distance(_: &[Scalar<Self>], _: &[Scalar<Self>]) -> F32 {
        unimplemented!()
    }
}
