use crate::prelude::*;

#[derive(Debug, Clone, Copy)]
pub enum SparseF32Cos {}

impl G for SparseF32Cos {
    type Element = SparseF32Element;

    type Scalar = F32;

    type Storage = SparseMmap;

    type L2 = F32L2;

    fn distance(lhs: &[Self::Element], rhs: &[Self::Element]) -> F32 {
        F32(1.0) - super::sparse_f32::cosine(lhs, rhs)
    }

    fn elkan_k_means_normalize(vector: &mut [Self::Scalar]) {
        super::f32::l2_normalize(vector)
    }

    fn elkan_k_means_distance(lhs: &[Self::Scalar], rhs: &[Self::Scalar]) -> F32 {
        super::f32::dot(lhs, rhs).acos()
    }

    #[allow(unused_variables)]
    fn scalar_quantization_distance(
        dims: u16,
        max: &[Self::Scalar],
        min: &[Self::Scalar],
        lhs: &[Self::Element],
        rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }

    #[allow(unused_variables)]
    fn scalar_quantization_distance2(
        dims: u16,
        max: &[Self::Scalar],
        min: &[Self::Scalar],
        lhs: &[u8],
        rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }

    #[allow(unused_variables)]
    fn product_quantization_distance(
        dims: u16,
        ratio: u16,
        centroids: &[Self::Scalar],
        lhs: &[Self::Element],
        rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }

    #[allow(unused_variables)]
    fn product_quantization_distance2(
        dims: u16,
        ratio: u16,
        centroids: &[Self::Scalar],
        lhs: &[u8],
        rhs: &[u8],
    ) -> F32 {
        unimplemented!()
    }

    #[allow(unused_variables)]
    fn product_quantization_distance_with_delta(
        dims: u16,
        ratio: u16,
        centroids: &[Self::Scalar],
        lhs: &[Self::Element],
        rhs: &[u8],
        delta: &[Self::Scalar],
    ) -> F32 {
        unimplemented!()
    }
}
