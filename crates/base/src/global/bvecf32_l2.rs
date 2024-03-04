use super::*;
use crate::distance::*;
use crate::scalar::*;
use crate::vector::*;
use num_traits::Float;

#[derive(Debug, Clone, Copy)]
pub enum BVecf32L2 {}

impl Global for BVecf32L2 {
    type VectorOwned = BVecf32Owned;

    const VECTOR_KIND: VectorKind = VectorKind::BVecf32;
    const DISTANCE_KIND: DistanceKind = DistanceKind::L2;

    fn distance(lhs: Borrowed<'_, Self>, rhs: Borrowed<'_, Self>) -> F32 {
        super::bvecf32::sl2(lhs, rhs)
    }
}

impl GlobalElkanKMeans for BVecf32L2 {
    type VectorNormalized = Vecf32Owned;

    fn elkan_k_means_normalize(vector: &mut [Scalar<Self>]) {
        super::vecf32::l2_normalize(vector)
    }

    fn elkan_k_means_normalize2(vector: Borrowed<'_, Self>) -> Vecf32Owned {
        Vecf32Owned::new(vector.to_vec())
    }

    fn elkan_k_means_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        super::vecf32::sl2(lhs, rhs).sqrt()
    }

    fn elkan_k_means_distance2(lhs: Vecf32Borrowed<'_>, rhs: &[Scalar<Self>]) -> F32 {
        super::vecf32::sl2(lhs.slice(), rhs).sqrt()
    }
}

impl GlobalScalarQuantization for BVecf32L2 {
    fn scalar_quantization_distance(
        _dims: u16,
        _max: &[F32],
        _min: &[F32],
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

impl GlobalProductQuantization for BVecf32L2 {
    type ProductQuantizationL2 = BVecf32L2;

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

    fn product_quantization_l2_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        super::vecf32::sl2(lhs, rhs)
    }

    fn product_quantization_dense_distance(_: &[Scalar<Self>], _: &[Scalar<Self>]) -> F32 {
        unimplemented!()
    }
}
