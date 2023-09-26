use crate::prelude::*;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

mod sealed {
    pub trait Sealed {}
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Distance {
    L2,
    Cosine,
    Dot,
}

impl Distance {
    pub fn distance(self, lhs: &[Scalar], rhs: &[Scalar]) -> Scalar {
        match self {
            Distance::L2 => distance_squared_l2(lhs, rhs),
            Distance::Cosine => distance_cosine(lhs, rhs) * (-1.0),
            Distance::Dot => distance_dot(lhs, rhs) * (-1.0),
        }
    }
    pub fn elkan_k_means_normalize(self, vector: &mut [Scalar]) {
        match self {
            Distance::L2 => (),
            Distance::Cosine => l2_normalize(vector),
            Distance::Dot => l2_normalize(vector),
        }
    }
    pub fn elkan_k_means_distance(self, lhs: &[Scalar], rhs: &[Scalar]) -> Scalar {
        match self {
            Distance::L2 => distance_squared_l2(lhs, rhs).sqrt(),
            Distance::Cosine => distance_dot(lhs, rhs).acos(),
            Distance::Dot => distance_dot(lhs, rhs).acos(),
        }
    }
    pub fn scalar_quantization_distance(
        self,
        dims: u16,
        max: &[Scalar],
        min: &[Scalar],
        lhs: &[Scalar],
        rhs: &[u8],
    ) -> Scalar {
        match self {
            Distance::L2 => {
                let mut result = Scalar::Z;
                for i in 0..dims as usize {
                    let _x = lhs[i];
                    let _y = Scalar(rhs[i] as Float / 256.0) * (max[i] - min[i]) + min[i];
                    result += (_x - _y) * (_x - _y);
                }
                result
            }
            Distance::Cosine => {
                let mut xy = Scalar::Z;
                let mut x2 = Scalar::Z;
                let mut y2 = Scalar::Z;
                for i in 0..dims as usize {
                    let _x = lhs[i];
                    let _y = Scalar(rhs[i] as Float / 256.0) * (max[i] - min[i]) + min[i];
                    xy += _x * _y;
                    x2 += _x * _x;
                    y2 += _y * _y;
                }
                xy / (x2 * y2).sqrt() * (-1.0)
            }
            Distance::Dot => {
                let mut xy = Scalar::Z;
                for i in 0..dims as usize {
                    let _x = lhs[i];
                    let _y = Scalar(rhs[i] as Float / 256.0) * (max[i] - min[i]) + min[i];
                    xy += _x * _y;
                }
                xy * (-1.0)
            }
        }
    }
    pub fn scalar_quantization_distance2(
        self,
        dims: u16,
        max: &[Scalar],
        min: &[Scalar],
        lhs: &[u8],
        rhs: &[u8],
    ) -> Scalar {
        match self {
            Distance::L2 => {
                let mut result = Scalar::Z;
                for i in 0..dims as usize {
                    let _x = Scalar(lhs[i] as Float / 256.0) * (max[i] - min[i]) + min[i];
                    let _y = Scalar(rhs[i] as Float / 256.0) * (max[i] - min[i]) + min[i];
                    result += (_x - _y) * (_x - _y);
                }
                result
            }
            Distance::Cosine => {
                let mut xy = Scalar::Z;
                let mut x2 = Scalar::Z;
                let mut y2 = Scalar::Z;
                for i in 0..dims as usize {
                    let _x = Scalar(lhs[i] as Float / 256.0) * (max[i] - min[i]) + min[i];
                    let _y = Scalar(rhs[i] as Float / 256.0) * (max[i] - min[i]) + min[i];
                    xy += _x * _y;
                    x2 += _x * _x;
                    y2 += _y * _y;
                }
                xy / (x2 * y2).sqrt() * (-1.0)
            }
            Distance::Dot => {
                let mut xy = Scalar::Z;
                for i in 0..dims as usize {
                    let _x = Scalar(lhs[i] as Float / 256.0) * (max[i] - min[i]) + min[i];
                    let _y = Scalar(rhs[i] as Float / 256.0) * (max[i] - min[i]) + min[i];
                    xy += _x * _y;
                }
                xy * (-1.0)
            }
        }
    }
    pub fn product_quantization_distance(
        self,
        dims: u16,
        ratio: u16,
        centroids: &[Scalar],
        lhs: &[Scalar],
        rhs: &[u8],
    ) -> Scalar {
        match self {
            Distance::L2 => {
                let width = dims.div_ceil(ratio);
                let mut result = Scalar::Z;
                for i in 0..width {
                    let k = std::cmp::min(ratio, dims - ratio * i);
                    let lhs = &lhs[(i * ratio) as usize..][..k as usize];
                    let rhsp = rhs[i as usize] as usize * dims as usize;
                    let rhs = &centroids[rhsp..][(i * ratio) as usize..][..k as usize];
                    result += distance_squared_l2(lhs, rhs);
                }
                result
            }
            Distance::Cosine => {
                let width = dims.div_ceil(ratio);
                let mut xy = Scalar::Z;
                let mut x2 = Scalar::Z;
                let mut y2 = Scalar::Z;
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
            Distance::Dot => {
                let width = dims.div_ceil(ratio);
                let mut xy = Scalar::Z;
                for i in 0..width {
                    let k = std::cmp::min(ratio, dims - ratio * i);
                    let lhs = &lhs[(i * ratio) as usize..][..k as usize];
                    let rhsp = rhs[i as usize] as usize * dims as usize;
                    let rhs = &centroids[rhsp..][(i * ratio) as usize..][..k as usize];
                    let _xy = distance_dot(lhs, rhs);
                    xy += _xy;
                }
                xy * (-1.0)
            }
        }
    }
    pub fn product_quantization_distance2(
        self,
        dims: u16,
        ratio: u16,
        centroids: &[Scalar],
        lhs: &[u8],
        rhs: &[u8],
    ) -> Scalar {
        match self {
            Distance::L2 => {
                let width = dims.div_ceil(ratio);
                let mut result = Scalar::Z;
                for i in 0..width {
                    let k = std::cmp::min(ratio, dims - ratio * i);
                    let lhsp = lhs[i as usize] as usize * dims as usize;
                    let lhs = &centroids[lhsp..][(i * ratio) as usize..][..k as usize];
                    let rhsp = rhs[i as usize] as usize * dims as usize;
                    let rhs = &centroids[rhsp..][(i * ratio) as usize..][..k as usize];
                    result += distance_squared_l2(lhs, rhs);
                }
                result
            }
            Distance::Cosine => {
                let width = dims.div_ceil(ratio);
                let mut xy = Scalar::Z;
                let mut x2 = Scalar::Z;
                let mut y2 = Scalar::Z;
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
            Distance::Dot => {
                let width = dims.div_ceil(ratio);
                let mut xy = Scalar::Z;
                for i in 0..width {
                    let k = std::cmp::min(ratio, dims - ratio * i);
                    let lhsp = lhs[i as usize] as usize * dims as usize;
                    let lhs = &centroids[lhsp..][(i * ratio) as usize..][..k as usize];
                    let rhsp = rhs[i as usize] as usize * dims as usize;
                    let rhs = &centroids[rhsp..][(i * ratio) as usize..][..k as usize];
                    let _xy = distance_dot(lhs, rhs);
                    xy += _xy;
                }
                xy * (-1.0)
            }
        }
    }
    pub fn product_quantization_distance_with_delta(
        self,
        dims: u16,
        ratio: u16,
        centroids: &[Scalar],
        lhs: &[Scalar],
        rhs: &[u8],
        delta: &[Scalar],
    ) -> Scalar {
        match self {
            Distance::L2 => {
                let width = dims.div_ceil(ratio);
                let mut result = Scalar::Z;
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
            Distance::Cosine => {
                let width = dims.div_ceil(ratio);
                let mut xy = Scalar::Z;
                let mut x2 = Scalar::Z;
                let mut y2 = Scalar::Z;
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
            Distance::Dot => {
                let width = dims.div_ceil(ratio);
                let mut xy = Scalar::Z;
                for i in 0..width {
                    let k = std::cmp::min(ratio, dims - ratio * i);
                    let lhs = &lhs[(i * ratio) as usize..][..k as usize];
                    let rhsp = rhs[i as usize] as usize * dims as usize;
                    let rhs = &centroids[rhsp..][(i * ratio) as usize..][..k as usize];
                    let del = &delta[(i * ratio) as usize..][..k as usize];
                    let _xy = distance_dot_delta(lhs, rhs, del);
                    xy += _xy;
                }
                xy * (-1.0)
            }
        }
    }
}

#[inline(always)]
fn distance_squared_l2(lhs: &[Scalar], rhs: &[Scalar]) -> Scalar {
    if lhs.len() != rhs.len() {
        panic!(
            "different vector dimensions {} and {}.",
            lhs.len(),
            rhs.len()
        );
    }
    let n = lhs.len();
    let mut d2 = Scalar::Z;
    for i in 0..n {
        let d = lhs[i] - rhs[i];
        d2 += d * d;
    }
    d2
}

#[inline(always)]
fn distance_cosine(lhs: &[Scalar], rhs: &[Scalar]) -> Scalar {
    if lhs.len() != rhs.len() {
        panic!(
            "different vector dimensions {} and {}.",
            lhs.len(),
            rhs.len()
        );
    }
    let n = lhs.len();
    let mut xy = Scalar::Z;
    let mut x2 = Scalar::Z;
    let mut y2 = Scalar::Z;
    for i in 0..n {
        xy += lhs[i] * rhs[i];
        x2 += lhs[i] * lhs[i];
        y2 += rhs[i] * rhs[i];
    }
    xy / (x2 * y2).sqrt()
}

#[inline(always)]
fn distance_dot(lhs: &[Scalar], rhs: &[Scalar]) -> Scalar {
    if lhs.len() != rhs.len() {
        panic!(
            "different vector dimensions {} and {}.",
            lhs.len(),
            rhs.len()
        );
    }
    let n = lhs.len();
    let mut xy = Scalar::Z;
    for i in 0..n {
        xy += lhs[i] * rhs[i];
    }
    xy
}

#[inline(always)]
fn xy_x2_y2(lhs: &[Scalar], rhs: &[Scalar]) -> (Scalar, Scalar, Scalar) {
    if lhs.len() != rhs.len() {
        panic!(
            "different vector dimensions {} and {}.",
            lhs.len(),
            rhs.len()
        );
    }
    let n = lhs.len();
    let mut xy = Scalar::Z;
    let mut x2 = Scalar::Z;
    let mut y2 = Scalar::Z;
    for i in 0..n {
        xy += lhs[i] * rhs[i];
        x2 += lhs[i] * lhs[i];
        y2 += rhs[i] * rhs[i];
    }
    (xy, x2, y2)
}

#[inline(always)]
fn length(vector: &[Scalar]) -> Scalar {
    let n = vector.len();
    let mut dot = Scalar::Z;
    for i in 0..n {
        dot += vector[i] * vector[i];
    }
    dot.sqrt()
}

#[inline(always)]
fn l2_normalize(vector: &mut [Scalar]) {
    let n = vector.len();
    let l = length(vector);
    for i in 0..n {
        vector[i] /= l;
    }
}

#[inline(always)]
fn distance_squared_l2_delta(lhs: &[Scalar], rhs: &[Scalar], del: &[Scalar]) -> Scalar {
    if lhs.len() != rhs.len() {
        panic!(
            "different vector dimensions {} and {}.",
            lhs.len(),
            rhs.len()
        );
    }
    let n = lhs.len();
    let mut d2 = Scalar::Z;
    for i in 0..n {
        let d = lhs[i] - (rhs[i] + del[i]);
        d2 += d * d;
    }
    d2
}

#[inline(always)]
fn xy_x2_y2_delta(lhs: &[Scalar], rhs: &[Scalar], del: &[Scalar]) -> (Scalar, Scalar, Scalar) {
    if lhs.len() != rhs.len() {
        panic!(
            "different vector dimensions {} and {}.",
            lhs.len(),
            rhs.len()
        );
    }
    let n = lhs.len();
    let mut xy = Scalar::Z;
    let mut x2 = Scalar::Z;
    let mut y2 = Scalar::Z;
    for i in 0..n {
        xy += lhs[i] * (rhs[i] + del[i]);
        x2 += lhs[i] * lhs[i];
        y2 += (rhs[i] + del[i]) * (rhs[i] + del[i]);
    }
    (xy, x2, y2)
}

#[inline(always)]
fn distance_dot_delta(lhs: &[Scalar], rhs: &[Scalar], del: &[Scalar]) -> Scalar {
    if lhs.len() != rhs.len() {
        panic!(
            "different vector dimensions {} and {}.",
            lhs.len(),
            rhs.len()
        );
    }
    let n = lhs.len();
    let mut xy = Scalar::Z;
    for i in 0..n {
        xy += lhs[i] * (rhs[i] + del[i]);
    }
    xy
}
