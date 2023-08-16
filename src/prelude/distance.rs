use crate::prelude::*;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::simd::f32x4;
use std::simd::SimdFloat;

const IS_VECTORIZARION_ENABLED: bool = is_vectorization_enabled();

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

pub trait DistanceFamily: sealed::Sealed + Copy + Default + Send + Sync + Unpin + 'static {
    fn distance(lhs: &[Scalar], rhs: &[Scalar]) -> Scalar;
    // elkan k means
    fn elkan_k_means_normalize(vector: &mut [Scalar]);
    fn elkan_k_means_distance(lhs: &[Scalar], rhs: &[Scalar]) -> Scalar;
    type QuantizationState: Debug
        + Copy
        + Send
        + Sync
        + serde::Serialize
        + for<'a> serde::Deserialize<'a>;
    // quantization
    const QUANTIZATION_INITIAL_STATE: Self::QuantizationState;
    fn quantization_new(lhs: &[Scalar], rhs: &[Scalar]) -> Self::QuantizationState;
    fn quantization_merge(
        lhs: Self::QuantizationState,
        rhs: Self::QuantizationState,
    ) -> Self::QuantizationState;
    fn quantization_append(
        state: Self::QuantizationState,
        lhs: Scalar,
        rhs: Scalar,
    ) -> Self::QuantizationState;
    fn quantization_finish(state: Self::QuantizationState) -> Scalar;
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct L2;

impl sealed::Sealed for L2 {}

impl DistanceFamily for L2 {
    #[inline(always)]
    fn distance(lhs: &[Scalar], rhs: &[Scalar]) -> Scalar {
        distance_squared_l2(lhs, rhs)
    }

    #[inline(always)]
    fn elkan_k_means_normalize(_: &mut [Scalar]) {}

    #[inline(always)]
    fn elkan_k_means_distance(lhs: &[Scalar], rhs: &[Scalar]) -> Scalar {
        distance_squared_l2(lhs, rhs)
    }

    type QuantizationState = Scalar;

    const QUANTIZATION_INITIAL_STATE: Scalar = Scalar::Z;

    #[inline(always)]
    fn quantization_new(lhs: &[Scalar], rhs: &[Scalar]) -> Self::QuantizationState {
        distance_squared_l2(lhs, rhs)
    }

    #[inline(always)]
    fn quantization_merge(lhs: Scalar, rhs: Scalar) -> Scalar {
        lhs + rhs
    }

    #[inline(always)]
    fn quantization_finish(state: Scalar) -> Scalar {
        state
    }

    #[inline(always)]
    fn quantization_append(
        result: Self::QuantizationState,
        lhs: Scalar,
        rhs: Scalar,
    ) -> Self::QuantizationState {
        result + (lhs - rhs) * (lhs - rhs)
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct Cosine;

impl sealed::Sealed for Cosine {}

impl DistanceFamily for Cosine {
    #[inline(always)]
    fn distance(lhs: &[Scalar], rhs: &[Scalar]) -> Scalar {
        distance_cosine(lhs, rhs) * (-1.0)
    }

    #[inline(always)]
    fn elkan_k_means_normalize(vector: &mut [Scalar]) {
        l2_normalize(vector)
    }

    #[inline(always)]
    fn elkan_k_means_distance(lhs: &[Scalar], rhs: &[Scalar]) -> Scalar {
        distance_dot(lhs, rhs).acos()
    }

    type QuantizationState = (Scalar, Scalar, Scalar);

    const QUANTIZATION_INITIAL_STATE: (Scalar, Scalar, Scalar) = (Scalar::Z, Scalar::Z, Scalar::Z);

    #[inline(always)]
    fn quantization_new(lhs: &[Scalar], rhs: &[Scalar]) -> (Scalar, Scalar, Scalar) {
        xy_x2_y2(lhs, rhs)
    }

    #[inline(always)]
    fn quantization_merge(
        (l_xy, l_x2, l_y2): (Scalar, Scalar, Scalar),
        (r_xy, r_x2, r_y2): (Scalar, Scalar, Scalar),
    ) -> (Scalar, Scalar, Scalar) {
        (l_xy + r_xy, l_x2 + r_x2, l_y2 + r_y2)
    }

    #[inline(always)]
    fn quantization_finish((xy, x2, y2): (Scalar, Scalar, Scalar)) -> Scalar {
        xy / (x2 * y2).sqrt() * (-1.0)
    }

    #[inline(always)]
    fn quantization_append(
        (xy, x2, y2): Self::QuantizationState,
        x: Scalar,
        y: Scalar,
    ) -> Self::QuantizationState {
        (xy + x * y, x2 + x * x, y2 + y * y)
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct Dot;

impl sealed::Sealed for Dot {}

impl DistanceFamily for Dot {
    #[inline(always)]
    fn distance(lhs: &[Scalar], rhs: &[Scalar]) -> Scalar {
        distance_dot(lhs, rhs) * (-1.0)
    }

    #[inline(always)]
    fn elkan_k_means_normalize(vector: &mut [Scalar]) {
        l2_normalize(vector)
    }

    #[inline(always)]
    fn elkan_k_means_distance(lhs: &[Scalar], rhs: &[Scalar]) -> Scalar {
        distance_dot(lhs, rhs).acos()
    }

    type QuantizationState = Scalar;

    const QUANTIZATION_INITIAL_STATE: Scalar = Scalar::Z;

    #[inline(always)]
    fn quantization_new(lhs: &[Scalar], rhs: &[Scalar]) -> Self::QuantizationState {
        distance_dot(lhs, rhs)
    }

    #[inline(always)]
    fn quantization_merge(lhs: Scalar, rhs: Scalar) -> Scalar {
        lhs + rhs
    }

    #[inline(always)]
    fn quantization_finish(state: Scalar) -> Scalar {
        state * (-1.0)
    }

    #[inline(always)]
    fn quantization_append(
        result: Self::QuantizationState,
        x: Scalar,
        y: Scalar,
    ) -> Self::QuantizationState {
        result + x * y
    }
}

#[allow(unreachable_code)]
#[inline(always)]
const fn is_vectorization_enabled() -> bool {
    #[cfg(all(
        any(target_arch = "x86", target_arch = "x86_64"),
        any(
            target_feature = "sse",
            target_feature = "sse2",
            target_feature = "sse3",
            target_feature = "ssse3",
            target_feature = "sse4.1",
            target_feature = "sse4.2",
            target_feature = "sse4a"
        )
    ))]
    {
        return true;
    }
    #[cfg(all(
        any(target_arch = "arm", target_arch = "aarch64"),
        target_feature = "neon"
    ))]
    {
        return true;
    }
    false
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
    if IS_VECTORIZARION_ENABLED {
        return distance_squared_l2_vec(lhs, rhs);
    }
    distance_squared_l2_scalar(lhs, rhs)
}

#[inline(always)]
fn distance_squared_l2_scalar(lhs: &[Scalar], rhs: &[Scalar]) -> Scalar {
    let n = lhs.len();
    let mut d2 = Scalar::Z;
    for i in 0..n {
        let d = lhs[i] - rhs[i];
        d2 += d * d;
    }
    d2
}

#[inline(always)]
fn distance_squared_l2_vec(lhs: &[Scalar], rhs: &[Scalar]) -> Scalar {
    let lhs_f32: Vec<f32> = lhs.iter().map(|&item| f32::from(item)).collect();
    let lhs_f32_slice: &[f32] = &lhs_f32;
    let (lhs_extra, lhs_chunks) = lhs_f32_slice.as_rchunks();

    let rhs_f32: Vec<f32> = rhs.iter().map(|&item| f32::from(item)).collect();
    let rhs_f32_slice: &[f32] = &rhs_f32;
    let (rhs_extra, rhs_chunks) = rhs_f32_slice.as_rchunks();

    let mut sums = [0.0; 4];
    for ((x, y), d) in std::iter::zip(lhs_extra, rhs_extra).zip(&mut sums) {
        let diff = x - y;
        *d = diff * diff;
    }

    let mut sums = f32x4::from_array(sums);
    std::iter::zip(lhs_chunks, rhs_chunks).for_each(|(x, y)| {
        let diff = f32x4::from_array(*x) - f32x4::from_array(*y);
        sums += diff * diff;
    });

    Scalar(sums.reduce_sum())
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
    if IS_VECTORIZARION_ENABLED {
        return distance_cosine_vec(lhs, rhs);
    }
    distance_cosine_scalar(lhs, rhs)
}

#[inline(always)]
fn distance_cosine_scalar(lhs: &[Scalar], rhs: &[Scalar]) -> Scalar {
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
fn distance_cosine_vec(lhs: &[Scalar], rhs: &[Scalar]) -> Scalar {
    let lhs_f32: Vec<f32> = lhs.iter().map(|&item| f32::from(item)).collect();
    let lhs_f32_slice: &[f32] = &lhs_f32;
    let (lhs_extra, lhs_chunks) = lhs_f32_slice.as_rchunks();

    let rhs_f32: Vec<f32> = rhs.iter().map(|&item| f32::from(item)).collect();
    let rhs_f32_slice: &[f32] = &rhs_f32;
    let (rhs_extra, rhs_chunks) = rhs_f32_slice.as_rchunks();

    let mut dot = [0.0; 4];
    let mut x2 = [0.0; 4];
    let mut y2 = [0.0; 4];
    for i in 0..lhs_extra.len() {
        let x = lhs_extra[i];
        let y = rhs_extra[i];
        dot[i] = x * y;
        x2[i] = x * x;
        y2[i] = y * y;
    }

    let mut dot = f32x4::from_array(dot);
    let mut x2 = f32x4::from_array(x2);
    let mut y2 = f32x4::from_array(y2);

    std::iter::zip(lhs_chunks, rhs_chunks).for_each(|(x, y)| {
        let x_vec = f32x4::from_array(*x);
        let y_vec = f32x4::from_array(*y);
        dot += x_vec * y_vec;
        x2 += x_vec * x_vec;
        y2 += y_vec * y_vec;
    });

    let dot = dot.reduce_sum();
    let x2 = x2.reduce_sum();
    let y2 = y2.reduce_sum();

    Scalar(dot / (x2 * y2).sqrt())
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
    if IS_VECTORIZARION_ENABLED {
        return distance_dot_vec(lhs, rhs);
    }
    distance_dot_scalar(lhs, rhs)
}

#[inline(always)]
fn distance_dot_scalar(lhs: &[Scalar], rhs: &[Scalar]) -> Scalar {
    let n = lhs.len();
    let mut xy = Scalar::Z;
    for i in 0..n {
        xy += lhs[i] * rhs[i];
    }
    xy
}

#[inline(always)]
fn distance_dot_vec(lhs: &[Scalar], rhs: &[Scalar]) -> Scalar {
    let lhs_f32: Vec<f32> = lhs.iter().map(|&item| f32::from(item)).collect();
    let lhs_f32_slice: &[f32] = &lhs_f32;
    let (lhs_extra, lhs_chunks) = lhs_f32_slice.as_rchunks();

    let rhs_f32: Vec<f32> = rhs.iter().map(|&item| f32::from(item)).collect();
    let rhs_f32_slice: &[f32] = &rhs_f32;
    let (rhs_extra, rhs_chunks) = rhs_f32_slice.as_rchunks();

    let mut sums = [0.0; 4];
    for ((x, y), d) in std::iter::zip(lhs_extra, rhs_extra).zip(&mut sums) {
        *d = x * y;
    }

    let mut sums = f32x4::from_array(sums);
    std::iter::zip(lhs_chunks, rhs_chunks).for_each(|(x, y)| {
        sums += f32x4::from_array(*x) * f32x4::from_array(*y);
    });

    Scalar(sums.reduce_sum())
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
    if IS_VECTORIZARION_ENABLED {
        return xy_x2_y2_vec(lhs, rhs);
    }
    xy_x2_y2_scalar(lhs, rhs)
}

#[inline(always)]
fn xy_x2_y2_scalar(lhs: &[Scalar], rhs: &[Scalar]) -> (Scalar, Scalar, Scalar) {
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
fn xy_x2_y2_vec(lhs: &[Scalar], rhs: &[Scalar]) -> (Scalar, Scalar, Scalar) {
    let lhs_f32: Vec<f32> = lhs.iter().map(|&item| f32::from(item)).collect();
    let lhs_f32_slice: &[f32] = &lhs_f32;
    let (lhs_extra, lhs_chunks) = lhs_f32_slice.as_rchunks();

    let rhs_f32: Vec<f32> = rhs.iter().map(|&item| f32::from(item)).collect();
    let rhs_f32_slice: &[f32] = &rhs_f32;
    let (rhs_extra, rhs_chunks) = rhs_f32_slice.as_rchunks();

    let mut dot = [0.0; 4];
    let mut x2 = [0.0; 4];
    let mut y2 = [0.0; 4];
    for i in 0..lhs_extra.len() {
        let x = lhs_extra[i];
        let y = rhs_extra[i];
        dot[i] = x * y;
        x2[i] = x * x;
        y2[i] = y * y;
    }

    let mut dot = f32x4::from_array(dot);
    let mut x2 = f32x4::from_array(x2);
    let mut y2 = f32x4::from_array(y2);

    std::iter::zip(lhs_chunks, rhs_chunks).for_each(|(x, y)| {
        let x_vec = f32x4::from_array(*x);
        let y_vec = f32x4::from_array(*y);
        dot += x_vec * y_vec;
        x2 += x_vec * x_vec;
        y2 += y_vec * y_vec;
    });

    let dot = dot.reduce_sum();
    let x2 = x2.reduce_sum();
    let y2 = y2.reduce_sum();

    (Scalar(dot), Scalar(x2), Scalar(y2))
}

#[inline(always)]
fn length(vector: &[Scalar]) -> Scalar {
    if IS_VECTORIZARION_ENABLED {
        return length_vec(vector);
    }
    length_scalar(vector)
}

#[inline(always)]
fn length_scalar(vector: &[Scalar]) -> Scalar {
    let n = vector.len();
    let mut dot = Scalar::Z;
    for i in 0..n {
        dot += vector[i] * vector[i];
    }
    dot.sqrt()
}

#[inline(always)]
fn length_vec(vector: &[Scalar]) -> Scalar {
    let vec_f32: Vec<f32> = vector.iter().map(|&item| f32::from(item)).collect();
    let vec_f32_slice: &[f32] = &vec_f32;
    let (extra, chunks) = vec_f32_slice.as_rchunks();

    let mut sums = [0.0; 4];
    for (x, d) in std::iter::zip(extra, &mut sums) {
        *d = x * x;
    }

    let mut sums = f32x4::from_array(sums);
    for i in 0..chunks.len() {
        let vec = f32x4::from_array(chunks[i]);
        sums += vec * vec;
    }

    Scalar(sums.reduce_sum().sqrt())
}

#[inline(always)]
fn l2_normalize(vector: &mut [Scalar]) {
    let n = vector.len();
    let l = length(vector);
    for i in 0..n {
        vector[i] /= l;
    }
}

#[cfg(test)]
mod distance_tests {
    use super::*;
    use rand::Rng;

    #[test]
    fn test_distance_dot_vec() {
        let mut rng = rand::thread_rng();
        if IS_VECTORIZARION_ENABLED {
            for _ in 0..100 {
                let array_length = rng.gen_range(1..=10);
                let mut x = Vec::new();
                let mut y = Vec::new();

                for _ in 0..array_length {
                    let e1 = Scalar::from(rng.gen::<f32>());
                    let e2 = Scalar::from(rng.gen::<f32>());
                    x.push(e1);
                    y.push(e2);
                }

                let x: &[Scalar] = &x;
                let y: &[Scalar] = &y;
                assert!((distance_dot_scalar(x, y) - distance_dot_vec(x, y)).0.abs() <= 1e-5);
            }
        }
    }

    #[test]
    fn test_distance_cosine_vec() {
        let mut rng = rand::thread_rng();
        if IS_VECTORIZARION_ENABLED {
            for _ in 0..100 {
                let array_length = rng.gen_range(1..=10);
                let mut x = Vec::new();
                let mut y = Vec::new();

                for _ in 0..array_length {
                    let e1 = Scalar::from(rng.gen::<f32>());
                    let e2 = Scalar::from(rng.gen::<f32>());
                    x.push(e1);
                    y.push(e2);
                }

                let x: &[Scalar] = &x;
                let y: &[Scalar] = &y;
                assert!(
                    (distance_cosine_scalar(x, y) - distance_cosine_vec(x, y))
                        .0
                        .abs()
                        <= 1e-5
                );
            }
        }
    }

    #[test]
    fn test_distance_squared_l2_vec() {
        let mut rng = rand::thread_rng();
        if IS_VECTORIZARION_ENABLED {
            for _ in 0..100 {
                let array_length = rng.gen_range(1..=10);
                let mut x = Vec::new();
                let mut y = Vec::new();

                for _ in 0..array_length {
                    let e1 = Scalar::from(rng.gen::<f32>());
                    let e2 = Scalar::from(rng.gen::<f32>());
                    x.push(e1);
                    y.push(e2);
                }

                let x: &[Scalar] = &x;
                let y: &[Scalar] = &y;
                assert!(
                    (distance_squared_l2_scalar(x, y) - distance_squared_l2_vec(x, y))
                        .0
                        .abs()
                        <= 1e-5
                );
            }
        }
    }

    #[test]
    fn test_length_vec() {
        let mut rng = rand::thread_rng();
        if IS_VECTORIZARION_ENABLED {
            for _ in 0..100 {
                let array_length = rng.gen_range(1..=10);
                let mut x = Vec::new();

                for _ in 0..array_length {
                    let e1 = Scalar::from(rng.gen::<f32>());
                    x.push(e1);
                }

                let x: &[Scalar] = &x;
                assert!((length_scalar(x) - length_vec(x)).0.abs() <= 1e-5);
            }
        }
    }
}
