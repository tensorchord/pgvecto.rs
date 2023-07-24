use crate::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Distance {
    L2,
    Cosine,
    Dot,
}

impl Distance {
    #[inline(always)]
    pub fn distance(self, lhs: &[Scalar], rhs: &[Scalar]) -> Scalar {
        match self {
            Distance::L2 => distance_squared_l2(lhs, rhs),
            Distance::Cosine => distance_squared_cosine(lhs, rhs) * (-1.0),
            Distance::Dot => distance_dot(lhs, rhs) * (-1.0),
        }
    }
    #[inline(always)]
    pub fn kmeans_normalize(self, vector: &mut [Scalar]) {
        match self {
            Distance::L2 => (),
            Distance::Cosine | Distance::Dot => l2_normalize(vector),
        }
    }
    #[inline(always)]
    pub fn kmeans_distance(self, lhs: &[Scalar], rhs: &[Scalar]) -> Scalar {
        match self {
            Distance::L2 => distance_squared_l2(lhs, rhs),
            Distance::Cosine | Distance::Dot => distance_dot(lhs, rhs).acos(),
        }
    }
}

#[inline(always)]
fn distance_squared_l2(lhs: &[Scalar], rhs: &[Scalar]) -> Scalar {
    if lhs.len() != rhs.len() {
        return Scalar::NAN;
    }
    let n = lhs.len();
    let mut result = Scalar::Z;
    for i in 0..n {
        let diff = lhs[i] - rhs[i];
        result += diff * diff;
    }
    result
}

#[inline(always)]
fn distance_squared_cosine(lhs: &[Scalar], rhs: &[Scalar]) -> Scalar {
    if lhs.len() != rhs.len() {
        return Scalar::NAN;
    }
    let n = lhs.len();
    let mut dot = Scalar::Z;
    let mut x2 = Scalar::Z;
    let mut y2 = Scalar::Z;
    for i in 0..n {
        dot += lhs[i] * rhs[i];
        x2 += lhs[i] * lhs[i];
        y2 += rhs[i] * rhs[i];
    }
    (dot * dot) / (x2 * y2)
}

#[inline(always)]
fn distance_dot(lhs: &[Scalar], rhs: &[Scalar]) -> Scalar {
    if lhs.len() != rhs.len() {
        return Scalar::NAN;
    }
    let n = lhs.len();
    let mut dot = Scalar::Z;
    for i in 0..n {
        dot += lhs[i] * rhs[i];
    }
    dot
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
