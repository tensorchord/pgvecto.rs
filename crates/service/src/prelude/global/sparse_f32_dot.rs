use crate::prelude::*;

#[derive(Debug, Clone, Copy)]
pub enum SparseF32Dot {}

impl G for SparseF32Dot {
    type Element = SparseF32Element;

    type Scalar = F32;

    type Storage = SparseMmap;

    type L2 = F32L2;

    fn distance(lhs: &[Self::Element], rhs: &[Self::Element]) -> F32 {
        super::sparse_f32::dot(lhs, rhs) * (-1.0)
    }

    fn elkan_k_means_normalize(vector: &mut [Self::Scalar]) {
        super::f32::l2_normalize(vector)
    }

    fn elkan_k_means_normalize2(vector: &mut [Self::Element]) {
        super::sparse_f32::l2_normalize(vector)
    }

    fn elkan_k_means_distance(lhs: &[Self::Scalar], rhs: &[Self::Scalar]) -> F32 {
        super::f32::dot(lhs, rhs).acos()
    }

    fn elkan_k_means_distance2(lhs: &[Self::Element], rhs: &[Self::Scalar]) -> F32 {
        super::sparse_f32::dot_2(lhs, rhs).acos()
    }

    fn scalar_quantization_distance(
        _dims: u16,
        _max: &[Self::Scalar],
        _min: &[Self::Scalar],
        _lhs: &[Self::Element],
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
        _lhs: &[Self::Element],
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
        _lhs: &[Self::Element],
        _rhs: &[u8],
        _delta: &[Self::Scalar],
    ) -> F32 {
        unimplemented!()
    }
}
