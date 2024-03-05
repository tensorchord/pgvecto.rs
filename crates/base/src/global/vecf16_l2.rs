use super::*;
use crate::distance::*;
use crate::scalar::*;
use crate::vector::*;
use num_traits::{Float, Zero};

#[derive(Debug, Clone, Copy)]
pub enum Vecf16L2 {}

impl Global for Vecf16L2 {
    type VectorOwned = Vecf16Owned;

    const VECTOR_KIND: VectorKind = VectorKind::Vecf16;
    const DISTANCE_KIND: DistanceKind = DistanceKind::L2;

    fn distance(lhs: Vecf16Borrowed<'_>, rhs: Vecf16Borrowed<'_>) -> F32 {
        super::vecf16::sl2(lhs.slice(), rhs.slice())
    }
}

impl GlobalElkanKMeans for Vecf16L2 {
    type VectorNormalized = Self::VectorOwned;

    fn elkan_k_means_normalize(_: &mut [F16]) {}

    fn elkan_k_means_normalize2(vector: Vecf16Borrowed<'_>) -> Vecf16Owned {
        vector.for_own()
    }

    fn elkan_k_means_distance(lhs: &[F16], rhs: &[F16]) -> F32 {
        super::vecf16::sl2(lhs, rhs).sqrt()
    }

    fn elkan_k_means_distance2(lhs: Vecf16Borrowed<'_>, rhs: &[F16]) -> F32 {
        super::vecf16::sl2(lhs.slice(), rhs).sqrt()
    }
}

impl GlobalScalarQuantization for Vecf16L2 {
    #[multiversion::multiversion(targets(
        "x86_64/x86-64-v4",
        "x86_64/x86-64-v3",
        "x86_64/x86-64-v2",
        "aarch64+neon"
    ))]
    fn scalar_quantization_distance<'a>(
        dims: u16,
        max: &[F16],
        min: &[F16],
        lhs: Vecf16Borrowed<'a>,
        rhs: &[u8],
    ) -> F32 {
        let lhs = lhs.slice();
        let mut result = F32::zero();
        for i in 0..dims as usize {
            let _x = lhs[i].to_f();
            let _y = (F32(rhs[i] as f32) / 256.0) * (max[i].to_f() - min[i].to_f()) + min[i].to_f();
            result += (_x - _y) * (_x - _y);
        }
        result
    }

    #[multiversion::multiversion(targets(
        "x86_64/x86-64-v4",
        "x86_64/x86-64-v3",
        "x86_64/x86-64-v2",
        "aarch64+neon"
    ))]
    fn scalar_quantization_distance2(
        dims: u16,
        max: &[F16],
        min: &[F16],
        lhs: &[u8],
        rhs: &[u8],
    ) -> F32 {
        let mut result = F32::zero();
        for i in 0..dims as usize {
            let _x = F32(lhs[i] as f32 / 256.0) * (max[i].to_f() - min[i].to_f()) + min[i].to_f();
            let _y = F32(rhs[i] as f32 / 256.0) * (max[i].to_f() - min[i].to_f()) + min[i].to_f();
            result += (_x - _y) * (_x - _y);
        }
        result
    }
}

impl GlobalProductQuantization for Vecf16L2 {
    type ProductQuantizationL2 = Vecf16L2;

    #[multiversion::multiversion(targets(
        "x86_64/x86-64-v4",
        "x86_64/x86-64-v3",
        "x86_64/x86-64-v2",
        "aarch64+neon"
    ))]
    fn product_quantization_distance<'a>(
        dims: u32,
        ratio: u32,
        centroids: &[F16],
        lhs: Vecf16Borrowed<'a>,
        rhs: &[u8],
    ) -> F32 {
        let lhs = lhs.slice();
        let width = dims.div_ceil(ratio);
        let mut result = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims - ratio * i);
            let lhs = &lhs[(i * ratio) as usize..][..k as usize];
            let rhsp = rhs[i as usize] as usize * dims as usize;
            let rhs = &centroids[rhsp..][(i * ratio) as usize..][..k as usize];
            result += super::vecf16::sl2(lhs, rhs);
        }
        result
    }

    #[multiversion::multiversion(targets(
        "x86_64/x86-64-v4",
        "x86_64/x86-64-v3",
        "x86_64/x86-64-v2",
        "aarch64+neon"
    ))]
    fn product_quantization_distance2(
        dims: u32,
        ratio: u32,
        centroids: &[F16],
        lhs: &[u8],
        rhs: &[u8],
    ) -> F32 {
        let width = dims.div_ceil(ratio);
        let mut result = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims - ratio * i);
            let lhsp = lhs[i as usize] as usize * dims as usize;
            let lhs = &centroids[lhsp..][(i * ratio) as usize..][..k as usize];
            let rhsp = rhs[i as usize] as usize * dims as usize;
            let rhs = &centroids[rhsp..][(i * ratio) as usize..][..k as usize];
            result += super::vecf16::sl2(lhs, rhs);
        }
        result
    }

    #[multiversion::multiversion(targets(
        "x86_64/x86-64-v4",
        "x86_64/x86-64-v3",
        "x86_64/x86-64-v2",
        "aarch64+neon"
    ))]
    fn product_quantization_distance_with_delta<'a>(
        dims: u32,
        ratio: u32,
        centroids: &[F16],
        lhs: Vecf16Borrowed<'a>,
        rhs: &[u8],
        delta: &[F16],
    ) -> F32 {
        let lhs = lhs.slice();
        let width = dims.div_ceil(ratio);
        let mut result = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims - ratio * i);
            let lhs = &lhs[(i * ratio) as usize..][..k as usize];
            let rhsp = rhs[i as usize] as usize * dims as usize;
            let rhs = &centroids[rhsp..][(i * ratio) as usize..][..k as usize];
            let del = &delta[(i * ratio) as usize..][..k as usize];
            result += distance_squared_l2_delta(lhs, rhs, del);
        }
        result
    }

    fn product_quantization_l2_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        super::vecf16::sl2(lhs, rhs)
    }

    fn product_quantization_dense_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        super::vecf16::sl2(lhs, rhs)
    }
}

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
fn distance_squared_l2_delta(lhs: &[F16], rhs: &[F16], del: &[F16]) -> F32 {
    assert!(lhs.len() == rhs.len());
    let n = lhs.len();
    let mut d2 = F32::zero();
    for i in 0..n {
        let d = lhs[i].to_f() - (rhs[i].to_f() + del[i].to_f());
        d2 += d * d;
    }
    d2
}
