use super::G;
use crate::prelude::scalar::F16;
use crate::prelude::scalar::F32;
use crate::prelude::*;

#[derive(Debug, Clone, Copy)]
pub enum F16L2 {}

impl G for F16L2 {
    type Scalar = F16;

    const DISTANCE: Distance = Distance::L2;

    type L2 = F16L2;

    fn distance(lhs: &[F16], rhs: &[F16]) -> F32 {
        super::f16::sl2(lhs, rhs)
    }

    fn elkan_k_means_normalize(_: &mut [F16]) {}

    fn elkan_k_means_distance(lhs: &[F16], rhs: &[F16]) -> F32 {
        super::f16::sl2(lhs, rhs).sqrt()
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
        let mut result = F32::zero();
        for i in 0..dims as usize {
            let _x = lhs[i].to_f();
            let _y = (F32(rhs[i] as f32) / 256.0) * (max[i].to_f() - min[i].to_f()) + min[i].to_f();
            result += (_x - _y) * (_x - _y);
        }
        result
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
        let mut result = F32::zero();
        for i in 0..dims as usize {
            let _x = F32(lhs[i] as f32 / 256.0) * (max[i].to_f() - min[i].to_f()) + min[i].to_f();
            let _y = F32(rhs[i] as f32 / 256.0) * (max[i].to_f() - min[i].to_f()) + min[i].to_f();
            result += (_x - _y) * (_x - _y);
        }
        result
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
        let mut result = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims - ratio * i);
            let lhs = &lhs[(i * ratio) as usize..][..k as usize];
            let rhsp = rhs[i as usize] as usize * dims as usize;
            let rhs = &centroids[rhsp..][(i * ratio) as usize..][..k as usize];
            result += super::f16::sl2(lhs, rhs);
        }
        result
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
        let mut result = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims - ratio * i);
            let lhsp = lhs[i as usize] as usize * dims as usize;
            let lhs = &centroids[lhsp..][(i * ratio) as usize..][..k as usize];
            let rhsp = rhs[i as usize] as usize * dims as usize;
            let rhs = &centroids[rhsp..][(i * ratio) as usize..][..k as usize];
            result += super::f16::sl2(lhs, rhs);
        }
        result
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
}

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64+avx512vl+avx512f+avx2+avx+ssse3+sse4.1+sse3+sse2+sse+fma",
    "x86_64+avx2+avx+ssse3+sse4.1+sse3+sse2+sse+fma",
    "x86_64+ssse3+sse4.1+sse3+sse2+sse+fma",
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
