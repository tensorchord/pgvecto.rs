use crate::prelude::*;
use std::borrow::Cow;

#[derive(Debug, Clone, Copy)]
pub enum F32L2 {}

impl G for F32L2 {
    type Scalar = F32;
    type Storage = DenseMmap<F32>;
    type L2 = F32L2;
    type VectorOwned = Vec<F32>;
    type VectorRef<'a> = &'a [F32];

    const DISTANCE: Distance = Distance::L2;
    const KIND: Kind = Kind::F32;

    fn owned_to_ref(vector: &Vec<F32>) -> &[F32] {
        vector
    }

    fn ref_to_owned(vector: &[F32]) -> Vec<F32> {
        vector.to_vec()
    }

    fn to_dense(vector: Self::VectorRef<'_>) -> Cow<'_, [F32]> {
        Cow::Borrowed(vector)
    }

    fn distance(lhs: &[F32], rhs: &[F32]) -> F32 {
        super::f32::sl2(lhs, rhs)
    }

    fn distance2(lhs: &[F32], rhs: &[F32]) -> F32 {
        super::f32::sl2(lhs, rhs)
    }

    fn elkan_k_means_normalize(_: &mut [F32]) {}

    fn elkan_k_means_normalize2(_: &mut Vec<F32>) {}

    fn elkan_k_means_distance(lhs: &[F32], rhs: &[F32]) -> F32 {
        super::f32::sl2(lhs, rhs).sqrt()
    }

    fn elkan_k_means_distance2(lhs: &[F32], rhs: &[F32]) -> F32 {
        super::f32::sl2(lhs, rhs).sqrt()
    }

    #[multiversion::multiversion(targets(
        "x86_64/x86-64-v4",
        "x86_64/x86-64-v3",
        "x86_64/x86-64-v2",
        "aarch64+neon"
    ))]
    fn scalar_quantization_distance(
        dims: u16,
        max: &[F32],
        min: &[F32],
        lhs: &[F32],
        rhs: &[u8],
    ) -> F32 {
        let mut result = F32::zero();
        for i in 0..dims as usize {
            let _x = lhs[i];
            let _y = F32(rhs[i] as f32 / 256.0) * (max[i] - min[i]) + min[i];
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
        max: &[F32],
        min: &[F32],
        lhs: &[u8],
        rhs: &[u8],
    ) -> F32 {
        let mut result = F32::zero();
        for i in 0..dims as usize {
            let _x = F32(lhs[i] as f32 / 256.0) * (max[i] - min[i]) + min[i];
            let _y = F32(rhs[i] as f32 / 256.0) * (max[i] - min[i]) + min[i];
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
    fn product_quantization_distance(
        dims: u16,
        ratio: u16,
        centroids: &[F32],
        lhs: &[F32],
        rhs: &[u8],
    ) -> F32 {
        let width = dims.div_ceil(ratio);
        let mut result = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims - ratio * i);
            let lhs = &lhs[(i * ratio) as usize..][..k as usize];
            let rhsp = rhs[i as usize] as usize * dims as usize;
            let rhs = &centroids[rhsp..][(i * ratio) as usize..][..k as usize];
            result += super::f32::sl2(lhs, rhs);
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
        dims: u16,
        ratio: u16,
        centroids: &[F32],
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
            result += super::f32::sl2(lhs, rhs);
        }
        result
    }

    #[multiversion::multiversion(targets(
        "x86_64/x86-64-v4",
        "x86_64/x86-64-v3",
        "x86_64/x86-64-v2",
        "aarch64+neon"
    ))]
    fn product_quantization_distance_with_delta(
        dims: u16,
        ratio: u16,
        centroids: &[F32],
        lhs: &[F32],
        rhs: &[u8],
        delta: &[F32],
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
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
fn distance_squared_l2_delta(lhs: &[F32], rhs: &[F32], del: &[F32]) -> F32 {
    assert!(lhs.len() == rhs.len());
    let n = lhs.len();
    let mut d2 = F32::zero();
    for i in 0..n {
        let d = lhs[i] - (rhs[i] + del[i]);
        d2 += d * d;
    }
    d2
}
