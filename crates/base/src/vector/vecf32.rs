use super::{VectorBorrowed, VectorKind, VectorOwned};
use crate::scalar::F32;
use num_traits::{Float, Zero};
use serde::{Deserialize, Serialize};
use std::ops::RangeBounds;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[repr(transparent)]
pub struct Vecf32Owned(Vec<F32>);

impl Vecf32Owned {
    #[inline(always)]
    pub fn new(slice: Vec<F32>) -> Self {
        Self::new_checked(slice).expect("invalid data")
    }

    #[inline(always)]
    pub fn new_checked(slice: Vec<F32>) -> Option<Self> {
        if !(1..=65535).contains(&slice.len()) {
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

    #[inline(always)]
    fn as_borrowed(&self) -> Vecf32Borrowed<'_> {
        Vecf32Borrowed(self.0.as_slice())
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct Vecf32Borrowed<'a>(&'a [F32]);

impl<'a> Vecf32Borrowed<'a> {
    #[inline(always)]
    pub fn new(slice: &'a [F32]) -> Self {
        Self::new_checked(slice).expect("invalid data")
    }

    #[inline(always)]
    pub fn new_checked(slice: &'a [F32]) -> Option<Self> {
        if !(1..=65535).contains(&slice.len()) {
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
    pub fn slice(&self) -> &'a [F32] {
        self.0
    }
}

impl<'a> VectorBorrowed for Vecf32Borrowed<'a> {
    type Scalar = F32;
    type Owned = Vecf32Owned;

    #[inline(always)]
    fn dims(&self) -> u32 {
        self.0.len() as u32
    }

    #[inline(always)]
    fn own(&self) -> Vecf32Owned {
        Vecf32Owned(self.0.to_vec())
    }

    #[inline(always)]
    fn to_vec(&self) -> Vec<F32> {
        self.0.to_vec()
    }

    #[inline(always)]
    fn norm(&self) -> F32 {
        length(self.0)
    }

    #[inline(always)]
    fn operator_dot(self, rhs: Self) -> F32 {
        dot(self.slice(), rhs.slice()) * (-1.0)
    }

    #[inline(always)]
    fn operator_l2(self, rhs: Self) -> F32 {
        sl2(self.slice(), rhs.slice())
    }

    #[inline(always)]
    fn operator_cos(self, rhs: Self) -> F32 {
        F32(1.0) - dot(self.slice(), rhs.slice()) / (self.norm() * rhs.norm())
    }

    #[inline(always)]
    fn operator_hamming(self, _: Self) -> F32 {
        unimplemented!()
    }

    #[inline(always)]
    fn operator_jaccard(self, _: Self) -> F32 {
        unimplemented!()
    }

    #[inline(always)]
    fn function_normalize(&self) -> Vecf32Owned {
        let mut data = self.0.to_vec();
        l2_normalize(&mut data);
        Vecf32Owned(data)
    }

    fn operator_add(&self, rhs: Self) -> Self::Owned {
        assert_eq!(self.0.len(), rhs.0.len());
        let n = self.dims();
        let mut slice = vec![F32::zero(); n as usize];
        for i in 0..n {
            slice[i as usize] = self.0[i as usize] + rhs.0[i as usize];
        }
        Vecf32Owned::new(slice)
    }

    fn operator_minus(&self, rhs: Self) -> Self::Owned {
        assert_eq!(self.0.len(), rhs.0.len());
        let n = self.dims();
        let mut slice = vec![F32::zero(); n as usize];
        for i in 0..n {
            slice[i as usize] = self.0[i as usize] - rhs.0[i as usize];
        }
        Vecf32Owned::new(slice)
    }

    fn operator_mul(&self, rhs: Self) -> Self::Owned {
        assert_eq!(self.0.len(), rhs.0.len());
        let n = self.dims();
        let mut slice = vec![F32::zero(); n as usize];
        for i in 0..n {
            slice[i as usize] = self.0[i as usize] * rhs.0[i as usize];
        }
        Vecf32Owned::new(slice)
    }

    fn operator_and(&self, _: Self) -> Self::Owned {
        unimplemented!()
    }

    fn operator_or(&self, _: Self) -> Self::Owned {
        unimplemented!()
    }

    fn operator_xor(&self, _: Self) -> Self::Owned {
        unimplemented!()
    }

    #[inline(always)]
    fn subvector(&self, bounds: impl RangeBounds<u32>) -> Option<Self::Owned> {
        let start_bound = bounds.start_bound().map(|x| *x as usize);
        let end_bound = bounds.end_bound().map(|x| *x as usize);
        let slice = self.0.get((start_bound, end_bound))?;
        if slice.is_empty() {
            return None;
        }
        Self::Owned::new_checked(slice.to_vec())
    }
}

impl<'a> PartialEq for Vecf32Borrowed<'a> {
    fn eq(&self, other: &Self) -> bool {
        if self.0.len() != other.0.len() {
            return false;
        }
        self.0 == other.0
    }
}

impl<'a> PartialOrd for Vecf32Borrowed<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.0.len() != other.0.len() {
            return None;
        }
        Some(self.0.cmp(other.0))
    }
}

#[detect::multiversion(v4, v3, v2, neon, fallback)]
pub fn dot(lhs: &[F32], rhs: &[F32]) -> F32 {
    assert!(lhs.len() == rhs.len());
    let n = lhs.len();
    let mut xy = F32::zero();
    for i in 0..n {
        xy += lhs[i] * rhs[i];
    }
    xy
}

#[detect::multiversion(v4, v3, v2, neon, fallback)]
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

#[detect::multiversion(v4, v3, v2, neon, fallback)]
pub fn length(vector: &[F32]) -> F32 {
    let n = vector.len();
    let mut dot = F32::zero();
    for i in 0..n {
        dot += vector[i] * vector[i];
    }
    dot.sqrt()
}

#[detect::multiversion(v4, v3, v2, neon, fallback)]
pub fn l2_normalize(vector: &mut [F32]) {
    let n = vector.len();
    let l = length(vector);
    for i in 0..n {
        vector[i] /= l;
    }
}

#[detect::multiversion(v4, v3, v2, neon, fallback)]
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
