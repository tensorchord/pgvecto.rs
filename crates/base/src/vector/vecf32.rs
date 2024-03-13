use super::{VectorBorrowed, VectorKind, VectorOwned};
use crate::scalar::F32;
use num_traits::{Float, Zero};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[repr(transparent)]
pub struct Vecf32Owned(Vec<F32>);

impl Vecf32Owned {
    #[inline(always)]
    pub fn new(slice: Vec<F32>) -> Self {
        Self::new_checked(slice).unwrap()
    }
    #[inline(always)]
    pub fn new_checked(slice: Vec<F32>) -> Option<Self> {
        if !(1 <= slice.len() && slice.len() <= 65535) {
            return None;
        }
        Some(unsafe { Self::new_unchecked(slice) })
    }
    /// # Safety
    ///
    /// * `slice.len()` must not be zero.
    #[inline(always)]
    pub unsafe fn new_unchecked(slice: Vec<F32>) -> Self {
        Self(slice)
    }
    #[inline(always)]
    pub fn slice(&self) -> &[F32] {
        self.0.as_slice()
    }
    #[inline(always)]
    pub fn slice_mut(&mut self) -> &mut [F32] {
        self.0.as_mut_slice()
    }
}

impl VectorOwned for Vecf32Owned {
    type Scalar = F32;
    type Borrowed<'a> = Vecf32Borrowed<'a>;

    const VECTOR_KIND: VectorKind = VectorKind::Vecf32;

    fn dims(&self) -> u32 {
        self.0.len() as u32
    }

    fn for_borrow(&self) -> Vecf32Borrowed<'_> {
        Vecf32Borrowed(self.0.as_slice())
    }

    fn to_vec(&self) -> Vec<F32> {
        self.0.clone()
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct Vecf32Borrowed<'a>(&'a [F32]);

impl<'a> Vecf32Borrowed<'a> {
    #[inline(always)]
    pub fn new(slice: &'a [F32]) -> Self {
        Self::new_checked(slice).unwrap()
    }
    #[inline(always)]
    pub fn new_checked(slice: &'a [F32]) -> Option<Self> {
        if !(1 <= slice.len() && slice.len() <= 65535) {
            return None;
        }
        Some(unsafe { Self::new_unchecked(slice) })
    }
    /// # Safety
    ///
    /// * `slice.len()` must not be zero.
    #[inline(always)]
    pub unsafe fn new_unchecked(slice: &'a [F32]) -> Self {
        Self(slice)
    }
    #[inline(always)]
    pub fn slice(&self) -> &[F32] {
        self.0
    }
}

impl<'a> VectorBorrowed for Vecf32Borrowed<'a> {
    type Scalar = F32;
    type Owned = Vecf32Owned;

    fn dims(&self) -> u32 {
        self.0.len() as u32
    }

    fn for_own(&self) -> Vecf32Owned {
        Vecf32Owned(self.0.to_vec())
    }

    fn to_vec(&self) -> Vec<F32> {
        self.0.to_vec()
    }
}

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
pub fn cosine(lhs: &[F32], rhs: &[F32]) -> F32 {
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
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
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
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
pub fn sl2(lhs: &[F32], rhs: &[F32]) -> F32 {
    assert!(lhs.len() == rhs.len());
    let n = lhs.len();
    let mut d2 = F32::zero();
    for i in 0..n {
        let d = lhs[i] - rhs[i];
        d2 += d * d;
    }
    d2
}

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
pub fn length(vector: &[F32]) -> F32 {
    let n = vector.len();
    let mut dot = F32::zero();
    for i in 0..n {
        dot += vector[i] * vector[i];
    }
    dot.sqrt()
}

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
pub fn l2_normalize(vector: &mut [F32]) {
    let n = vector.len();
    let l = length(vector);
    for i in 0..n {
        vector[i] /= l;
    }
}

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
pub fn xy_x2_y2(lhs: &[F32], rhs: &[F32]) -> (F32, F32, F32) {
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
    (xy, x2, y2)
}

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
pub fn xy_x2_y2_delta(lhs: &[F32], rhs: &[F32], del: &[F32]) -> (F32, F32, F32) {
    assert!(lhs.len() == rhs.len());
    let n = lhs.len();
    let mut xy = F32::zero();
    let mut x2 = F32::zero();
    let mut y2 = F32::zero();
    for i in 0..n {
        xy += lhs[i] * (rhs[i] + del[i]);
        x2 += lhs[i] * lhs[i];
        y2 += (rhs[i] + del[i]) * (rhs[i] + del[i]);
    }
    (xy, x2, y2)
}

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
pub fn dot_delta(lhs: &[F32], rhs: &[F32], del: &[F32]) -> F32 {
    assert!(lhs.len() == rhs.len());
    let n: usize = lhs.len();
    let mut xy = F32::zero();
    for i in 0..n {
        xy += lhs[i] * (rhs[i] + del[i]);
    }
    xy
}

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
pub fn distance_squared_l2_delta(lhs: &[F32], rhs: &[F32], del: &[F32]) -> F32 {
    assert!(lhs.len() == rhs.len());
    let n = lhs.len();
    let mut d2 = F32::zero();
    for i in 0..n {
        let d = lhs[i] - (rhs[i] + del[i]);
        d2 += d * d;
    }
    d2
}
