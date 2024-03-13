use base::operator::*;
use base::scalar::*;
use base::vector::*;
use num_traits::Float;

pub trait OperatorElkanKMeans: Operator {
    type VectorNormalized: VectorOwned;

    fn elkan_k_means_normalize(vector: &mut [Scalar<Self>]);
    fn elkan_k_means_normalize2(vector: Borrowed<'_, Self>) -> Self::VectorNormalized;
    fn elkan_k_means_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32;
    fn elkan_k_means_distance2(
        lhs: <Self::VectorNormalized as VectorOwned>::Borrowed<'_>,
        rhs: &[Scalar<Self>],
    ) -> F32;
}

impl OperatorElkanKMeans for BVecf32Cos {
    type VectorNormalized = Vecf32Owned;

    fn elkan_k_means_normalize(vector: &mut [Scalar<Self>]) {
        vecf32::l2_normalize(vector)
    }

    fn elkan_k_means_normalize2(vector: Borrowed<'_, Self>) -> Vecf32Owned {
        bvecf32::l2_normalize(vector)
    }

    fn elkan_k_means_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::dot(lhs, rhs).acos()
    }

    fn elkan_k_means_distance2(lhs: Vecf32Borrowed<'_>, rhs: &[Scalar<Self>]) -> F32 {
        vecf32::dot(lhs.slice(), rhs).acos()
    }
}

impl OperatorElkanKMeans for BVecf32Dot {
    type VectorNormalized = Vecf32Owned;

    fn elkan_k_means_normalize(vector: &mut [Scalar<Self>]) {
        vecf32::l2_normalize(vector)
    }

    fn elkan_k_means_normalize2(vector: Borrowed<'_, Self>) -> Vecf32Owned {
        bvecf32::l2_normalize(vector)
    }

    fn elkan_k_means_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::dot(lhs, rhs).acos()
    }

    fn elkan_k_means_distance2(lhs: Vecf32Borrowed<'_>, rhs: &[Scalar<Self>]) -> F32 {
        vecf32::dot(lhs.slice(), rhs).acos()
    }
}

impl OperatorElkanKMeans for BVecf32Jaccard {
    type VectorNormalized = Vecf32Owned;

    fn elkan_k_means_normalize(vector: &mut [Scalar<Self>]) {
        vecf32::l2_normalize(vector)
    }

    fn elkan_k_means_normalize2(vector: Borrowed<'_, Self>) -> Vecf32Owned {
        Vecf32Owned::new(vector.to_vec())
    }

    fn elkan_k_means_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::sl2(lhs, rhs).sqrt()
    }

    fn elkan_k_means_distance2(lhs: Vecf32Borrowed<'_>, rhs: &[Scalar<Self>]) -> F32 {
        vecf32::sl2(lhs.slice(), rhs).sqrt()
    }
}

impl OperatorElkanKMeans for BVecf32L2 {
    type VectorNormalized = Vecf32Owned;

    fn elkan_k_means_normalize(vector: &mut [Scalar<Self>]) {
        vecf32::l2_normalize(vector)
    }

    fn elkan_k_means_normalize2(vector: Borrowed<'_, Self>) -> Vecf32Owned {
        Vecf32Owned::new(vector.to_vec())
    }

    fn elkan_k_means_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::sl2(lhs, rhs).sqrt()
    }

    fn elkan_k_means_distance2(lhs: Vecf32Borrowed<'_>, rhs: &[Scalar<Self>]) -> F32 {
        vecf32::sl2(lhs.slice(), rhs).sqrt()
    }
}

impl OperatorElkanKMeans for SVecf32Cos {
    type VectorNormalized = Self::VectorOwned;

    fn elkan_k_means_normalize(vector: &mut [Scalar<Self>]) {
        vecf32::l2_normalize(vector)
    }

    fn elkan_k_means_normalize2(vector: Borrowed<'_, Self>) -> SVecf32Owned {
        let mut vector = vector.for_own();
        svecf32::l2_normalize(&mut vector);
        vector
    }

    fn elkan_k_means_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::dot(lhs, rhs).acos()
    }

    fn elkan_k_means_distance2(lhs: Borrowed<'_, Self>, rhs: &[Scalar<Self>]) -> F32 {
        svecf32::dot_2(lhs, rhs).acos()
    }
}

impl OperatorElkanKMeans for SVecf32Dot {
    type VectorNormalized = Self::VectorOwned;

    fn elkan_k_means_normalize(vector: &mut [Scalar<Self>]) {
        vecf32::l2_normalize(vector)
    }

    fn elkan_k_means_normalize2(vector: Borrowed<'_, Self>) -> SVecf32Owned {
        let mut vector = vector.for_own();
        svecf32::l2_normalize(&mut vector);
        vector
    }

    fn elkan_k_means_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::dot(lhs, rhs).acos()
    }

    fn elkan_k_means_distance2(lhs: Borrowed<'_, Self>, rhs: &[Scalar<Self>]) -> F32 {
        svecf32::dot_2(lhs, rhs).acos()
    }
}

impl OperatorElkanKMeans for SVecf32L2 {
    type VectorNormalized = Self::VectorOwned;

    fn elkan_k_means_normalize(_: &mut [Scalar<Self>]) {}

    fn elkan_k_means_normalize2(vector: SVecf32Borrowed<'_>) -> SVecf32Owned {
        vector.for_own()
    }

    fn elkan_k_means_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::sl2(lhs, rhs).sqrt()
    }

    fn elkan_k_means_distance2(lhs: SVecf32Borrowed<'_>, rhs: &[Scalar<Self>]) -> F32 {
        svecf32::sl2_2(lhs, rhs).sqrt()
    }
}

impl OperatorElkanKMeans for Vecf16Cos {
    type VectorNormalized = Self::VectorOwned;

    fn elkan_k_means_normalize(vector: &mut [F16]) {
        vecf16::l2_normalize(vector)
    }

    fn elkan_k_means_normalize2(vector: Vecf16Borrowed<'_>) -> Vecf16Owned {
        let mut vector = vector.for_own();
        vecf16::l2_normalize(vector.slice_mut());
        vector
    }

    fn elkan_k_means_distance(lhs: &[F16], rhs: &[F16]) -> F32 {
        vecf16::dot(lhs, rhs).acos()
    }

    fn elkan_k_means_distance2(lhs: Vecf16Borrowed<'_>, rhs: &[F16]) -> F32 {
        vecf16::dot(lhs.slice(), rhs).acos()
    }
}

impl OperatorElkanKMeans for Vecf16Dot {
    type VectorNormalized = Self::VectorOwned;

    fn elkan_k_means_normalize(vector: &mut [F16]) {
        vecf16::l2_normalize(vector)
    }

    fn elkan_k_means_normalize2(vector: Vecf16Borrowed<'_>) -> Vecf16Owned {
        let mut vector = vector.for_own();
        vecf16::l2_normalize(vector.slice_mut());
        vector
    }

    fn elkan_k_means_distance(lhs: &[F16], rhs: &[F16]) -> F32 {
        vecf16::dot(lhs, rhs).acos()
    }

    fn elkan_k_means_distance2(lhs: Vecf16Borrowed<'_>, rhs: &[F16]) -> F32 {
        vecf16::dot(lhs.slice(), rhs).acos()
    }
}

impl OperatorElkanKMeans for Vecf16L2 {
    type VectorNormalized = Self::VectorOwned;

    fn elkan_k_means_normalize(_: &mut [F16]) {}

    fn elkan_k_means_normalize2(vector: Vecf16Borrowed<'_>) -> Vecf16Owned {
        vector.for_own()
    }

    fn elkan_k_means_distance(lhs: &[F16], rhs: &[F16]) -> F32 {
        vecf16::sl2(lhs, rhs).sqrt()
    }

    fn elkan_k_means_distance2(lhs: Vecf16Borrowed<'_>, rhs: &[F16]) -> F32 {
        vecf16::sl2(lhs.slice(), rhs).sqrt()
    }
}

impl OperatorElkanKMeans for Vecf32Cos {
    type VectorNormalized = Self::VectorOwned;

    fn elkan_k_means_normalize(vector: &mut [F32]) {
        vecf32::l2_normalize(vector)
    }

    fn elkan_k_means_normalize2(vector: Vecf32Borrowed<'_>) -> Vecf32Owned {
        let mut vector = vector.for_own();
        vecf32::l2_normalize(vector.slice_mut());
        vector
    }

    fn elkan_k_means_distance(lhs: &[F32], rhs: &[F32]) -> F32 {
        vecf32::dot(lhs, rhs).acos()
    }

    fn elkan_k_means_distance2(lhs: Vecf32Borrowed<'_>, rhs: &[F32]) -> F32 {
        vecf32::dot(lhs.slice(), rhs).acos()
    }
}

impl OperatorElkanKMeans for Vecf32Dot {
    type VectorNormalized = Self::VectorOwned;

    fn elkan_k_means_normalize(vector: &mut [F32]) {
        vecf32::l2_normalize(vector)
    }

    fn elkan_k_means_normalize2(vector: Vecf32Borrowed<'_>) -> Vecf32Owned {
        let mut vector = vector.for_own();
        vecf32::l2_normalize(vector.slice_mut());
        vector
    }

    fn elkan_k_means_distance(lhs: &[F32], rhs: &[F32]) -> F32 {
        vecf32::dot(lhs, rhs).acos()
    }

    fn elkan_k_means_distance2(lhs: Vecf32Borrowed<'_>, rhs: &[F32]) -> F32 {
        vecf32::dot(lhs.slice(), rhs).acos()
    }
}

impl OperatorElkanKMeans for Vecf32L2 {
    type VectorNormalized = Self::VectorOwned;

    fn elkan_k_means_normalize(_: &mut [F32]) {}

    fn elkan_k_means_normalize2(vector: Vecf32Borrowed<'_>) -> Vecf32Owned {
        vector.for_own()
    }

    fn elkan_k_means_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::sl2(lhs, rhs).sqrt()
    }

    fn elkan_k_means_distance2(lhs: Vecf32Borrowed<'_>, rhs: &[Scalar<Self>]) -> F32 {
        vecf32::sl2(lhs.slice(), rhs).sqrt()
    }
}

impl OperatorElkanKMeans for Veci8Cos {
    type VectorNormalized = Self::VectorOwned;

    fn elkan_k_means_normalize(vector: &mut [Scalar<Self>]) {
        vecf32::l2_normalize(vector)
    }

    fn elkan_k_means_normalize2(vector: Borrowed<'_, Self>) -> Veci8Owned {
        vector.normalize()
    }

    fn elkan_k_means_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::dot(lhs, rhs).acos()
    }

    fn elkan_k_means_distance2(lhs: Borrowed<'_, Self>, rhs: &[Scalar<Self>]) -> F32 {
        veci8::dot_2(lhs, rhs).acos()
    }
}

impl OperatorElkanKMeans for Veci8Dot {
    type VectorNormalized = Self::VectorOwned;

    fn elkan_k_means_normalize(vector: &mut [Scalar<Self>]) {
        vecf32::l2_normalize(vector)
    }

    fn elkan_k_means_normalize2(vector: Borrowed<'_, Self>) -> Veci8Owned {
        vector.normalize()
    }

    fn elkan_k_means_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::dot(lhs, rhs).acos()
    }

    fn elkan_k_means_distance2(lhs: Borrowed<'_, Self>, rhs: &[Scalar<Self>]) -> F32 {
        veci8::dot_2(lhs, rhs).acos()
    }
}

impl OperatorElkanKMeans for Veci8L2 {
    type VectorNormalized = Self::VectorOwned;

    fn elkan_k_means_normalize(vector: &mut [Scalar<Self>]) {
        vecf32::l2_normalize(vector)
    }

    fn elkan_k_means_normalize2(vector: Borrowed<'_, Self>) -> Veci8Owned {
        vector.normalize()
    }

    fn elkan_k_means_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        vecf32::sl2(lhs, rhs).sqrt()
    }

    fn elkan_k_means_distance2(lhs: Borrowed<'_, Self>, rhs: &[Scalar<Self>]) -> F32 {
        veci8::l2_2(lhs, rhs).sqrt()
    }
}
