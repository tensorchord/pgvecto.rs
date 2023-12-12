use super::G;
use crate::prelude::scalar::F32;
use crate::prelude::*;

#[derive(Debug, Clone, Copy)]
pub enum F16Dot {}

impl G for F16Dot {
    type Scalar = F16;

    const DISTANCE: Distance = Distance::Dot;

    type L2 = F16L2;

    fn distance(lhs: &[F16], rhs: &[F16]) -> F32 {
        super::f16::dot(lhs, rhs) * (-1.0)
    }

    fn elkan_k_means_normalize(vector: &mut [F16]) {
        l2_normalize(vector)
    }

    fn elkan_k_means_distance(lhs: &[F16], rhs: &[F16]) -> F32 {
        super::f16::dot(lhs, rhs).acos()
    }

    #[multiversion::multiversion(targets(
        "x86_64+avx512vl+avx512f+avx2+avx+ssse3+sse4.1+sse3+sse2+sse+fma",
        "x86_64+avx2+avx+ssse3+sse4.1+sse3+sse2+sse+fma",
        "x86_64+ssse3+sse4.1+sse3+sse2+sse+fma",
        "aarch64+neon"
    ))]
    fn scalar_quantization_distance(
        dims: u16,
        max: &[F16],
        min: &[F16],
        lhs: &[F16],
        rhs: &[u8],
    ) -> F32 {
        let mut xy = F32::zero();
        for i in 0..dims as usize {
            let _x = lhs[i].to_f();
            let _y = F32(rhs[i] as f32 / 256.0) * (max[i].to_f() - min[i].to_f()) + min[i].to_f();
            xy += _x * _y;
        }
        xy * (-1.0)
    }

    #[multiversion::multiversion(targets(
        "x86_64+avx512vl+avx512f+avx2+avx+ssse3+sse4.1+sse3+sse2+sse+fma",
        "x86_64+avx2+avx+ssse3+sse4.1+sse3+sse2+sse+fma",
        "x86_64+ssse3+sse4.1+sse3+sse2+sse+fma",
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

    #[multiversion::multiversion(targets(
        "x86_64+avx512vl+avx512f+avx2+avx+ssse3+sse4.1+sse3+sse2+sse+fma",
        "x86_64+avx2+avx+ssse3+sse4.1+sse3+sse2+sse+fma",
        "x86_64+ssse3+sse4.1+sse3+sse2+sse+fma",
        "aarch64+neon"
    ))]
    fn product_quantization_distance(
        dims: u16,
        ratio: u16,
        centroids: &[F16],
        lhs: &[F16],
        rhs: &[u8],
    ) -> F32 {
        let width = dims.div_ceil(ratio);
        let mut xy = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims - ratio * i);
            let lhs = &lhs[(i * ratio) as usize..][..k as usize];
            let rhsp = rhs[i as usize] as usize * dims as usize;
            let rhs = &centroids[rhsp..][(i * ratio) as usize..][..k as usize];
            let _xy = super::f16::dot(lhs, rhs);
            xy += _xy;
        }
        xy * (-1.0)
    }

    #[multiversion::multiversion(targets(
        "x86_64+avx512vl+avx512f+avx2+avx+ssse3+sse4.1+sse3+sse2+sse+fma",
        "x86_64+avx2+avx+ssse3+sse4.1+sse3+sse2+sse+fma",
        "x86_64+ssse3+sse4.1+sse3+sse2+sse+fma",
        "aarch64+neon"
    ))]
    fn product_quantization_distance2(
        dims: u16,
        ratio: u16,
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
            let _xy = super::f16::dot(lhs, rhs);
            xy += _xy;
        }
        xy * (-1.0)
    }

    #[multiversion::multiversion(targets(
        "x86_64+avx512vl+avx512f+avx2+avx+ssse3+sse4.1+sse3+sse2+sse+fma",
        "x86_64+avx2+avx+ssse3+sse4.1+sse3+sse2+sse+fma",
        "x86_64+ssse3+sse4.1+sse3+sse2+sse+fma",
        "aarch64+neon"
    ))]
    fn product_quantization_distance_with_delta(
        dims: u16,
        ratio: u16,
        centroids: &[F16],
        lhs: &[F16],
        rhs: &[u8],
        delta: &[F16],
    ) -> F32 {
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
}

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64+avx512vl+avx512f+avx2+avx+ssse3+sse4.1+sse3+sse2+sse+fma",
    "x86_64+avx2+avx+ssse3+sse4.1+sse3+sse2+sse+fma",
    "x86_64+ssse3+sse4.1+sse3+sse2+sse+fma",
    "aarch64+neon"
))]
fn length(vector: &[F16]) -> F16 {
    let n = vector.len();
    let mut dot = F16::zero();
    for i in 0..n {
        dot += vector[i] * vector[i];
    }
    dot.sqrt()
}

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64+avx512vl+avx512f+avx2+avx+ssse3+sse4.1+sse3+sse2+sse+fma",
    "x86_64+avx2+avx+ssse3+sse4.1+sse3+sse2+sse+fma",
    "x86_64+ssse3+sse4.1+sse3+sse2+sse+fma",
    "aarch64+neon"
))]
fn l2_normalize(vector: &mut [F16]) {
    let n = vector.len();
    let l = length(vector);
    for i in 0..n {
        vector[i] /= l;
    }
}

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64+avx512vl+avx512f+avx2+avx+ssse3+sse4.1+sse3+sse2+sse+fma",
    "x86_64+avx2+avx+ssse3+sse4.1+sse3+sse2+sse+fma",
    "x86_64+ssse3+sse4.1+sse3+sse2+sse+fma",
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
