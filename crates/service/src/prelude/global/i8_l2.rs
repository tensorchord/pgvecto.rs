use std::borrow::Cow;

use crate::prelude::*;

use self::storage::I8QuantMmap;

#[derive(Debug, Clone, Copy)]
pub enum I8L2 {}

impl G for I8L2 {
    // For i8 quantization, the scalar type is used for type in kmeans, it use F32 to store the centroids and examples in the kmeans.
    type Scalar = F32;
    type Storage = I8QuantMmap;
    type L2 = F32L2;
    type VectorOwned = VecI8Owned;
    type VectorRef<'a> = VecI8Ref<'a>;

    const DISTANCE: Distance = Distance::Cos;
    const KIND: Kind = Kind::I8;

    fn owned_to_ref(vector: &VecI8Owned) -> VecI8Ref<'_> {
        VecI8Ref::from(vector)
    }

    fn ref_to_owned(vector: VecI8Ref<'_>) -> VecI8Owned {
        VecI8Owned::from(vector)
    }

    // For i8 quantization, the to_dense function is used to convert the quantized vector to the original vector which is needed in the kmeans.
    fn to_dense(vector: Self::VectorRef<'_>) -> Cow<'_, [Self::Scalar]> {
        Cow::Owned(dequantization(vector.data, vector.alpha, vector.offset))
    }

    fn distance(lhs: VecI8Ref<'_>, rhs: VecI8Ref<'_>) -> F32 {
        super::i8::l2_distance(&lhs, &rhs)
    }

    fn elkan_k_means_normalize(vector: &mut [Self::Scalar]) {
        super::f32::l2_normalize(vector)
    }

    fn elkan_k_means_normalize2(vector: &mut Self::VectorOwned) {
        super::i8::l2_normalize(vector)
    }

    fn elkan_k_means_distance(lhs: &[Self::Scalar], rhs: &[Self::Scalar]) -> F32 {
        super::f32::sl2(lhs, rhs).sqrt()
    }

    fn elkan_k_means_distance2(lhs: Self::VectorRef<'_>, rhs: &[Self::Scalar]) -> F32 {
        super::i8::l2_2(lhs, rhs)
    }

    fn scalar_quantization_distance(
        _dims: u16,
        _max: &[Self::Scalar],
        _min: &[Self::Scalar],
        _lhs: Self::VectorRef<'_>,
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }

    fn scalar_quantization_distance2(
        _dims: u16,
        _max: &[Self::Scalar],
        _min: &[Self::Scalar],
        _lhs: &[u8],
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }

    fn product_quantization_distance(
        _dims: u16,
        _ratio: u16,
        _centroids: &[Self::Scalar],
        _lhs: Self::VectorRef<'_>,
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }

    fn product_quantization_distance2(
        _dims: u16,
        _ratio: u16,
        _centroids: &[Self::Scalar],
        _lhs: &[u8],
        _rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }

    fn product_quantization_distance_with_delta(
        _dims: u16,
        _ratio: u16,
        _centroids: &[Self::Scalar],
        _lhs: Self::VectorRef<'_>,
        _rhs: &[u8],
        _delta: &[Self::Scalar],
    ) -> F32 {
        unimplemented!()
    }
}
