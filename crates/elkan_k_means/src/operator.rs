use base::operator::*;
use base::scalar::*;
use base::vector::*;
use num_traits::Float;

pub trait OperatorElkanKMeans: Operator {
    type VectorNormalized: VectorOwned;

    fn elkan_k_means_normalize(vector: &mut [Scalar<Self>]);
    fn elkan_k_means_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32;
}

impl OperatorElkanKMeans for BVecf32Cos {
    type VectorNormalized = Vecf32Owned;

    fn elkan_k_means_normalize(vector: &mut [Scalar<Self>]) {
        vecf32::l2_normalize(vector)
    }

    fn elkan_k_means_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::dot(lhs, rhs).acos()
    }
}

impl OperatorElkanKMeans for BVecf32Dot {
    type VectorNormalized = Vecf32Owned;

    fn elkan_k_means_normalize(vector: &mut [Scalar<Self>]) {
        vecf32::l2_normalize(vector)
    }

    fn elkan_k_means_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::dot(lhs, rhs).acos()
    }
}

impl OperatorElkanKMeans for BVecf32Jaccard {
    type VectorNormalized = Vecf32Owned;

    fn elkan_k_means_normalize(vector: &mut [Scalar<Self>]) {
        vecf32::l2_normalize(vector)
    }

    fn elkan_k_means_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::sl2(lhs, rhs).sqrt()
    }
}

impl OperatorElkanKMeans for BVecf32L2 {
    type VectorNormalized = Vecf32Owned;

    fn elkan_k_means_normalize(_: &mut [Scalar<Self>]) {}

    fn elkan_k_means_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::sl2(lhs, rhs).sqrt()
    }
}

impl OperatorElkanKMeans for SVecf32Cos {
    type VectorNormalized = Self::VectorOwned;

    fn elkan_k_means_normalize(vector: &mut [Scalar<Self>]) {
        vecf32::l2_normalize(vector)
    }

    fn elkan_k_means_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::dot(lhs, rhs).acos()
    }
}

impl OperatorElkanKMeans for SVecf32Dot {
    type VectorNormalized = Self::VectorOwned;

    fn elkan_k_means_normalize(vector: &mut [Scalar<Self>]) {
        vecf32::l2_normalize(vector)
    }

    fn elkan_k_means_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::dot(lhs, rhs).acos()
    }
}

impl OperatorElkanKMeans for SVecf32L2 {
    type VectorNormalized = Self::VectorOwned;

    fn elkan_k_means_normalize(_: &mut [Scalar<Self>]) {}

    fn elkan_k_means_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::sl2(lhs, rhs).sqrt()
    }
}

impl OperatorElkanKMeans for Vecf16Cos {
    type VectorNormalized = Self::VectorOwned;

    fn elkan_k_means_normalize(vector: &mut [Scalar<Self>]) {
        vecf16::l2_normalize(vector)
    }
    fn elkan_k_means_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf16::dot(lhs, rhs).acos()
    }
}

impl OperatorElkanKMeans for Vecf16Dot {
    type VectorNormalized = Self::VectorOwned;

    fn elkan_k_means_normalize(vector: &mut [Scalar<Self>]) {
        vecf16::l2_normalize(vector)
    }

    fn elkan_k_means_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf16::dot(lhs, rhs).acos()
    }
}

impl OperatorElkanKMeans for Vecf16L2 {
    type VectorNormalized = Self::VectorOwned;

    fn elkan_k_means_normalize(_: &mut [Scalar<Self>]) {}

    fn elkan_k_means_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf16::sl2(lhs, rhs).sqrt()
    }
}

impl OperatorElkanKMeans for Vecf32Cos {
    type VectorNormalized = Self::VectorOwned;

    fn elkan_k_means_normalize(vector: &mut [Scalar<Self>]) {
        vecf32::l2_normalize(vector)
    }

    fn elkan_k_means_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::dot(lhs, rhs).acos()
    }
}

impl OperatorElkanKMeans for Vecf32Dot {
    type VectorNormalized = Self::VectorOwned;

    fn elkan_k_means_normalize(vector: &mut [Scalar<Self>]) {
        vecf32::l2_normalize(vector)
    }

    fn elkan_k_means_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::dot(lhs, rhs).acos()
    }
}

impl OperatorElkanKMeans for Vecf32L2 {
    type VectorNormalized = Self::VectorOwned;

    fn elkan_k_means_normalize(_: &mut [Scalar<Self>]) {}

    fn elkan_k_means_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::sl2(lhs, rhs).sqrt()
    }
}

impl OperatorElkanKMeans for Veci8Cos {
    type VectorNormalized = Self::VectorOwned;

    fn elkan_k_means_normalize(vector: &mut [Scalar<Self>]) {
        vecf32::l2_normalize(vector)
    }

    fn elkan_k_means_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::dot(lhs, rhs).acos()
    }
}

impl OperatorElkanKMeans for Veci8Dot {
    type VectorNormalized = Self::VectorOwned;

    fn elkan_k_means_normalize(vector: &mut [Scalar<Self>]) {
        vecf32::l2_normalize(vector)
    }

    fn elkan_k_means_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::dot(lhs, rhs).acos()
    }
}

impl OperatorElkanKMeans for Veci8L2 {
    type VectorNormalized = Self::VectorOwned;

    fn elkan_k_means_normalize(_: &mut [Scalar<Self>]) {}

    fn elkan_k_means_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::sl2(lhs, rhs).sqrt()
    }
}
