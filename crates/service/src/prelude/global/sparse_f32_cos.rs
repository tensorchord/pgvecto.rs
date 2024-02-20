use std::borrow::Cow;

use crate::prelude::*;

#[derive(Debug, Clone, Copy)]
pub enum SparseF32Cos {}

impl G for SparseF32Cos {
    type Scalar = F32;
    type Storage = SparseMmap;
    type L2 = F32L2;
    type VectorOwned = SparseF32;
    type VectorRef<'a> = SparseF32Ref<'a>;
    type VectorNormalized = SparseF32;

    const DISTANCE: Distance = Distance::Cos;
    const KIND: Kind = Kind::SparseF32;

    fn owned_to_ref(vector: &SparseF32) -> SparseF32Ref<'_> {
        SparseF32Ref::from(vector)
    }

    fn ref_to_owned(vector: SparseF32Ref<'_>) -> SparseF32 {
        SparseF32::from(vector)
    }

    fn to_scalar_vec(vector: Self::VectorRef<'_>) -> Cow<'_, [F32]> {
        Cow::Owned(vector.to_dense())
    }

    fn distance(lhs: Self::VectorRef<'_>, rhs: Self::VectorRef<'_>) -> F32 {
        F32(1.0) - super::sparse_f32::cosine(lhs, rhs)
    }

    fn elkan_k_means_normalize(vector: &mut [Self::Scalar]) {
        super::f32::l2_normalize(vector)
    }

    fn elkan_k_means_normalize2(vector: Self::VectorRef<'_>) -> SparseF32 {
        let mut vector = SparseF32::from(vector);
        super::sparse_f32::l2_normalize(&mut vector);
        vector
    }

    fn elkan_k_means_distance(lhs: &[Self::Scalar], rhs: &[Self::Scalar]) -> F32 {
        super::f32::dot(lhs, rhs).acos()
    }

    fn elkan_k_means_distance2(lhs: &SparseF32, rhs: &[Self::Scalar]) -> F32 {
        super::sparse_f32::dot_2(SparseF32Ref::from(lhs), rhs).acos()
    }
}
