use super::G;
use crate::prelude::scalar::F32;
use crate::prelude::*;

#[derive(Debug, Clone, Copy)]
pub enum F32Dot {}

impl G for F32Dot {
    type Scalar = F32;

    const DISTANCE: Distance = Distance::Dot;

    type L2 = F32L2;

    fn distance(lhs: &[F32], rhs: &[F32]) -> F32 {
        dot(lhs, rhs) * (-1.0)
    }

    fn l2_distance(lhs: &[F32], rhs: &[F32]) -> F32 {
        super::f32_l2::distance_squared_l2(lhs, rhs)
    }

    fn elkan_k_means_normalize(vector: &mut [F32]) {
        l2_normalize(vector)
    }

    fn elkan_k_means_distance(lhs: &[F32], rhs: &[F32]) -> F32 {
        super::f32_dot::dot(lhs, rhs).acos()
    }

    #[multiversion::multiversion(targets = "simd")]
    fn scalar_quantization_distance(
        dims: u16,
        max: &[F32],
        min: &[F32],
        lhs: &[F32],
        rhs: &[u8],
    ) -> F32 {
        let mut xy = F32::zero();
        for i in 0..dims as usize {
            let _x = lhs[i];
            let _y = F32(rhs[i] as f32 / 256.0) * (max[i] - min[i]) + min[i];
            xy += _x * _y;
        }
        xy * (-1.0)
    }

    #[multiversion::multiversion(targets = "simd")]
    fn scalar_quantization_distance2(
        dims: u16,
        max: &[F32],
        min: &[F32],
        lhs: &[u8],
        rhs: &[u8],
    ) -> F32 {
        let mut xy = F32::zero();
        for i in 0..dims as usize {
            let _x = F32(lhs[i] as f32 / 256.0) * (max[i] - min[i]) + min[i];
            let _y = F32(rhs[i] as f32 / 256.0) * (max[i] - min[i]) + min[i];
            xy += _x * _y;
        }
        xy * (-1.0)
    }

    #[multiversion::multiversion(targets = "simd")]
    fn product_quantization_distance(
        dims: u16,
        ratio: u16,
        centroids: &[F32],
        lhs: &[F32],
        rhs: &[u8],
    ) -> F32 {
        let width = dims.div_ceil(ratio);
        let mut xy = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims - ratio * i);
            let lhs = &lhs[(i * ratio) as usize..][..k as usize];
            let rhsp = rhs[i as usize] as usize * dims as usize;
            let rhs = &centroids[rhsp..][(i * ratio) as usize..][..k as usize];
            let _xy = dot(lhs, rhs);
            xy += _xy;
        }
        xy * (-1.0)
    }

    #[multiversion::multiversion(targets = "simd")]
    fn product_quantization_distance2(
        dims: u16,
        ratio: u16,
        centroids: &[F32],
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
            let _xy = dot(lhs, rhs);
            xy += _xy;
        }
        xy * (-1.0)
    }

    #[multiversion::multiversion(targets = "simd")]
    fn product_quantization_distance_with_delta(
        dims: u16,
        ratio: u16,
        centroids: &[F32],
        lhs: &[F32],
        rhs: &[u8],
        delta: &[F32],
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
#[multiversion::multiversion(targets = "simd")]
fn length(vector: &[F32]) -> F32 {
    let n = vector.len();
    let mut dot = F32::zero();
    for i in 0..n {
        dot += vector[i] * vector[i];
    }
    dot.sqrt()
}

#[inline(always)]
#[multiversion::multiversion(targets = "simd")]
fn l2_normalize(vector: &mut [F32]) {
    let n = vector.len();
    let l = length(vector);
    for i in 0..n {
        vector[i] /= l;
    }
}

#[inline(always)]
#[multiversion::multiversion(targets = "simd")]
fn cosine(lhs: &[F32], rhs: &[F32]) -> F32 {
    assert!(lhs.len() == rhs.len());
    let n = lhs.len();
    let mut xy = F32::zero();
    let mut x2 = F32::zero();
    let mut y2 = F32::zero();
    for i in 0..n {
        xy += lhs[i] * rhs[i];
        x2 += lhs[i] * lhs[i];
        y2 += rhs[i] * rhs[i];
    }
    xy / (x2 * y2).sqrt()
}

#[inline(always)]
#[multiversion::multiversion(targets = "simd")]
pub fn dot(lhs: &[F32], rhs: &[F32]) -> F32 {
    assert!(lhs.len() == rhs.len());
    let n = lhs.len();
    let mut xy = F32::zero();
    for i in 0..n {
        xy += lhs[i] * rhs[i];
    }
    xy
}

#[inline(always)]
#[multiversion::multiversion(targets = "simd")]
fn dot_delta(lhs: &[F32], rhs: &[F32], del: &[F32]) -> F32 {
    assert!(lhs.len() == rhs.len());
    let n: usize = lhs.len();
    let mut xy = F32::zero();
    for i in 0..n {
        xy += lhs[i] * (rhs[i] + del[i]);
    }
    xy
}
