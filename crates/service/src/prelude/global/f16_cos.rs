use super::G;
use crate::prelude::scalar::F32;
use crate::prelude::*;

#[derive(Debug, Clone, Copy)]
pub enum F16Cos {}

impl G for F16Cos {
    type Scalar = F16;

    const DISTANCE: Distance = Distance::Cos;

    type L2 = F16L2;

    fn distance(lhs: &[F16], rhs: &[F16]) -> F32 {
        super::f16::cosine(lhs, rhs) * (-1.0)
    }

    fn elkan_k_means_normalize(vector: &mut [F16]) {
        l2_normalize(vector)
    }

    fn elkan_k_means_distance(lhs: &[F16], rhs: &[F16]) -> F32 {
        super::f16::dot(lhs, rhs).acos()
    }

    #[multiversion::multiversion(targets = "simd")]
    fn scalar_quantization_distance(
        dims: u16,
        max: &[F16],
        min: &[F16],
        lhs: &[F16],
        rhs: &[u8],
    ) -> F32 {
        let mut xy = F32::zero();
        let mut x2 = F32::zero();
        let mut y2 = F32::zero();
        for i in 0..dims as usize {
            let _x = lhs[i].to_f();
            let _y = F32(rhs[i] as f32 / 256.0) * (max[i].to_f() - min[i].to_f()) + min[i].to_f();
            xy += _x * _y;
            x2 += _x * _x;
            y2 += _y * _y;
        }
        xy / (x2 * y2).sqrt() * (-1.0)
    }

    #[multiversion::multiversion(targets = "simd")]
    fn scalar_quantization_distance2(
        dims: u16,
        max: &[F16],
        min: &[F16],
        lhs: &[u8],
        rhs: &[u8],
    ) -> F32 {
        let mut xy = F32::zero();
        let mut x2 = F32::zero();
        let mut y2 = F32::zero();
        for i in 0..dims as usize {
            let _x = F32(lhs[i] as f32 / 256.0) * (max[i].to_f() - min[i].to_f()) + min[i].to_f();
            let _y = F32(rhs[i] as f32 / 256.0) * (max[i].to_f() - min[i].to_f()) + min[i].to_f();
            xy += _x * _y;
            x2 += _x * _x;
            y2 += _y * _y;
        }
        xy / (x2 * y2).sqrt() * (-1.0)
    }

    #[multiversion::multiversion(targets = "simd")]
    fn product_quantization_distance(
        dims: u16,
        ratio: u16,
        centroids: &[F16],
        lhs: &[F16],
        rhs: &[u8],
    ) -> F32 {
        let width = dims.div_ceil(ratio);
        let mut xy = F32::zero();
        let mut x2 = F32::zero();
        let mut y2 = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims - ratio * i);
            let lhs = &lhs[(i * ratio) as usize..][..k as usize];
            let rhsp = rhs[i as usize] as usize * dims as usize;
            let rhs = &centroids[rhsp..][(i * ratio) as usize..][..k as usize];
            let (_xy, _x2, _y2) = xy_x2_y2(lhs, rhs);
            xy += _xy;
            x2 += _x2;
            y2 += _y2;
        }
        xy / (x2 * y2).sqrt() * (-1.0)
    }

    #[multiversion::multiversion(targets = "simd")]
    fn product_quantization_distance2(
        dims: u16,
        ratio: u16,
        centroids: &[F16],
        lhs: &[u8],
        rhs: &[u8],
    ) -> F32 {
        let width = dims.div_ceil(ratio);
        let mut xy = F32::zero();
        let mut x2 = F32::zero();
        let mut y2 = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims - ratio * i);
            let lhsp = lhs[i as usize] as usize * dims as usize;
            let lhs = &centroids[lhsp..][(i * ratio) as usize..][..k as usize];
            let rhsp = rhs[i as usize] as usize * dims as usize;
            let rhs = &centroids[rhsp..][(i * ratio) as usize..][..k as usize];
            let (_xy, _x2, _y2) = xy_x2_y2(lhs, rhs);
            xy += _xy;
            x2 += _x2;
            y2 += _y2;
        }
        xy / (x2 * y2).sqrt() * (-1.0)
    }

    #[multiversion::multiversion(targets = "simd")]
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
        let mut x2 = F32::zero();
        let mut y2 = F32::zero();
        for i in 0..width {
            let k = std::cmp::min(ratio, dims - ratio * i);
            let lhs = &lhs[(i * ratio) as usize..][..k as usize];
            let rhsp = rhs[i as usize] as usize * dims as usize;
            let rhs = &centroids[rhsp..][(i * ratio) as usize..][..k as usize];
            let del = &delta[(i * ratio) as usize..][..k as usize];
            let (_xy, _x2, _y2) = xy_x2_y2_delta(lhs, rhs, del);
            xy += _xy;
            x2 += _x2;
            y2 += _y2;
        }
        xy / (x2 * y2).sqrt() * (-1.0)
    }
}

#[inline(always)]
#[multiversion::multiversion(targets = "simd")]
fn length(vector: &[F16]) -> F16 {
    let n = vector.len();
    let mut dot = F16::zero();
    for i in 0..n {
        dot += vector[i] * vector[i];
    }
    dot.sqrt()
}

#[inline(always)]
#[multiversion::multiversion(targets = "simd")]
fn l2_normalize(vector: &mut [F16]) {
    let n = vector.len();
    let l = length(vector);
    for i in 0..n {
        vector[i] /= l;
    }
}

#[inline(always)]
#[multiversion::multiversion(targets = "simd")]
fn xy_x2_y2(lhs: &[F16], rhs: &[F16]) -> (F32, F32, F32) {
    assert!(lhs.len() == rhs.len());
    let n = lhs.len();
    let mut xy = F32::zero();
    let mut x2 = F32::zero();
    let mut y2 = F32::zero();
    for i in 0..n {
        xy += lhs[i].to_f() * rhs[i].to_f();
        x2 += lhs[i].to_f() * lhs[i].to_f();
        y2 += rhs[i].to_f() * rhs[i].to_f();
    }
    (xy, x2, y2)
}

#[inline(always)]
#[multiversion::multiversion(targets = "simd")]
fn xy_x2_y2_delta(lhs: &[F16], rhs: &[F16], del: &[F16]) -> (F32, F32, F32) {
    assert!(lhs.len() == rhs.len());
    let n = lhs.len();
    let mut xy = F32::zero();
    let mut x2 = F32::zero();
    let mut y2 = F32::zero();
    for i in 0..n {
        xy += lhs[i].to_f() * (rhs[i].to_f() + del[i].to_f());
        x2 += lhs[i].to_f() * lhs[i].to_f();
        y2 += (rhs[i].to_f() + del[i].to_f()) * (rhs[i].to_f() + del[i].to_f());
    }
    (xy, x2, y2)
}
