use crate::prelude::*;
use std::borrow::Cow;

#[derive(Debug, Clone, Copy)]
pub enum BinaryL2 {}

impl G for BinaryL2 {
    type Scalar = F32;
    type Storage = BinaryMmap;
    type L2 = F32L2;
    type VectorOwned = BinaryVec;
    type VectorRef<'a> = BinaryVecRef<'a>;
    type VectorNormalized = Vec<F32>;

    const DISTANCE: Distance = Distance::Cos;
    const KIND: Kind = Kind::F32;

    fn owned_to_ref(vector: &BinaryVec) -> BinaryVecRef<'_> {
        BinaryVecRef::from(vector)
    }

    fn ref_to_owned(vector: BinaryVecRef<'_>) -> BinaryVec {
        BinaryVec::from(vector)
    }

    fn to_scalar_vec(vector: Self::VectorRef<'_>) -> Cow<'_, [F32]> {
        Cow::Owned(Vec::from(vector))
    }

    fn distance(lhs: Self::VectorRef<'_>, rhs: Self::VectorRef<'_>) -> F32 {
        super::binary::sl2(lhs, rhs)
    }

    fn elkan_k_means_normalize(_: &mut [F32]) {}

    fn elkan_k_means_normalize2(vector: Self::VectorRef<'_>) -> Vec<F32> {
        Vec::from(vector)
    }

    fn elkan_k_means_distance(lhs: &[F32], rhs: &[F32]) -> F32 {
        super::f32::dot(lhs, rhs).acos()
    }

    fn elkan_k_means_distance2(lhs: &Vec<F32>, rhs: &[F32]) -> F32 {
        super::f32::dot(lhs, rhs).acos()
    }
}
