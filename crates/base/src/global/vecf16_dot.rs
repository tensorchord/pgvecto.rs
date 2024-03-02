use super::*;
use crate::distance::*;
use crate::scalar::*;
use crate::vector::*;
use num_traits::{Float, Zero};

#[derive(Debug, Clone, Copy)]
pub enum Vecf16Dot {}

impl Global for Vecf16Dot {
    type VectorOwned = Vecf16Owned;

    const VECTOR_KIND: VectorKind = VectorKind::Vecf16;
    const DISTANCE_KIND: DistanceKind = DistanceKind::Dot;

    fn distance(lhs: Vecf16Borrowed<'_>, rhs: Vecf16Borrowed<'_>) -> F32 {
        super::vecf16::dot(lhs.slice(), rhs.slice()) * (-1.0)
    }
}

impl GlobalElkanKMeans for Vecf16Dot {
    fn elkan_k_means_normalize(vector: &mut [F16]) {
        super::vecf16::l2_normalize(vector)
    }

    fn elkan_k_means_normalize2(vector: Vecf16Borrowed<'_>) -> Vecf16Owned {
        let mut vector = vector.for_own();
        super::vecf16::l2_normalize(vector.slice_mut());
        vector
    }

    fn elkan_k_means_distance(lhs: &[F16], rhs: &[F16]) -> F32 {
        super::vecf16::dot(lhs, rhs).acos()
    }

    fn elkan_k_means_distance2(lhs: Vecf16Borrowed<'_>, rhs: &[F16]) -> F32 {
        super::vecf16::dot(lhs.slice(), rhs).acos()
    }
}

impl GlobalScalarQuantization for Vecf16Dot {
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
        let mut xy = F32::zero();
        for i in 0..dims as usize {
            let _x = lhs[i].to_f();
            let _y = F32(rhs[i] as f32 / 256.0) * (max[i].to_f() - min[i].to_f()) + min[i].to_f();
            xy += _x * _y;
        }
        xy * (-1.0)
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
        let mut xy = F32::zero();
        for i in 0..dims as usize {
            let _x = F32(lhs[i] as f32 / 256.0) * (max[i].to_f() - min[i].to_f()) + min[i].to_f();
            let _y = F32(rhs[i] as f32 / 256.0) * (max[i].to_f() - min[i].to_f()) + min[i].to_f();
            xy += _x * _y;
        }
        xy * (-1.0)
    }
}

impl GlobalProductQuantization for Vecf16Dot {
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
        let mut xy = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims - ratio * i);
            let lhs = &lhs[(i * ratio) as usize..][..k as usize];
            let rhsp = rhs[i as usize] as usize * dims as usize;
            let rhs = &centroids[rhsp..][(i * ratio) as usize..][..k as usize];
            let _xy = super::vecf16::dot(lhs, rhs);
            xy += _xy;
        }
        xy * (-1.0)
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
        let mut xy = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims - ratio * i);
            let lhsp = lhs[i as usize] as usize * dims as usize;
            let lhs = &centroids[lhsp..][(i * ratio) as usize..][..k as usize];
            let rhsp = rhs[i as usize] as usize * dims as usize;
            let rhs = &centroids[rhsp..][(i * ratio) as usize..][..k as usize];
            let _xy = super::vecf16::dot(lhs, rhs);
            xy += _xy;
        }
        xy * (-1.0)
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
        let mut xy = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims - ratio * i);
            let lhs = &lhs[(i * ratio) as usize..][..k as usize];
            let rhsp = rhs[i as usize] as usize * dims as usize;
            let rhs = &centroids[rhsp..][(i * ratio) as usize..][..k as usize];
            let del = &delta[(i * ratio) as usize..][..k as usize];
            let _xy = dot_delta(lhs, rhs, del);
            xy += _xy;
        }
        xy * (-1.0)
    }

    fn product_quantization_l2_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        super::vecf16::sl2(lhs, rhs)
    }

    fn product_quantization_dense_distance(lhs: &[Scalar<Self>], rhs: &[Scalar<Self>]) -> F32 {
        super::vecf16::dot(lhs, rhs) * (-1.0)
    }
}

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
fn dot_delta(lhs: &[F16], rhs: &[F16], del: &[F16]) -> F32 {
    assert!(lhs.len() == rhs.len());
    let n: usize = lhs.len();
    let mut xy = F32::zero();
    for i in 0..n {
        xy += lhs[i].to_f() * (rhs[i].to_f() + del[i].to_f());
    }
    xy
}
