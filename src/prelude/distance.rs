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
        distance_squared_l2(lhs, rhs).sqrt()
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
