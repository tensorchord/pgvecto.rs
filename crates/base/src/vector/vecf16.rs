use super::{VectorBorrowed, VectorKind, VectorOwned};
use crate::scalar::{ScalarLike, F16, F32};
use num_traits::{Float, Zero};
use serde::{Deserialize, Serialize};
use std::ops::RangeBounds;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[repr(transparent)]
pub struct Vecf16Owned(Vec<F16>);

impl Vecf16Owned {
    #[inline(always)]
    pub fn new(slice: Vec<F16>) -> Self {
        Self::new_checked(slice).expect("invalid data")
    }

    #[inline(always)]
    pub fn new_checked(slice: Vec<F16>) -> Option<Self> {
        if !(1..=65535).contains(&slice.len()) {
            return None;
        }
        Some(unsafe { Self::new_unchecked(slice) })
    }

    /// # Safety
    ///
    /// * `slice.len()` must not be zero.
    #[inline(always)]
    pub unsafe fn new_unchecked(slice: Vec<F16>) -> Self {
        Self(slice)
    }

    #[inline(always)]
    pub fn slice(&self) -> &[F16] {
        self.0.as_slice()
    }

    #[inline(always)]
    pub fn slice_mut(&mut self) -> &mut [F16] {
        self.0.as_mut_slice()
    }
}

impl VectorOwned for Vecf16Owned {
    type Scalar = F16;
    type Borrowed<'a> = Vecf16Borrowed<'a>;

    const VECTOR_KIND: VectorKind = VectorKind::Vecf16;

    #[inline(always)]
    fn as_borrowed(&self) -> Vecf16Borrowed<'_> {
        Vecf16Borrowed(self.0.as_slice())
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct Vecf16Borrowed<'a>(&'a [F16]);

impl<'a> Vecf16Borrowed<'a> {
    #[inline(always)]
    pub fn new(slice: &'a [F16]) -> Self {
        Self::new_checked(slice).expect("invalid data")
    }

    #[inline(always)]
    pub fn new_checked(slice: &'a [F16]) -> Option<Self> {
        if !(1..=65535).contains(&slice.len()) {
            return None;
        }
        Some(unsafe { Self::new_unchecked(slice) })
    }

    /// # Safety
    ///
    /// * `slice.len()` must not be zero.
    #[inline(always)]
    pub unsafe fn new_unchecked(slice: &'a [F16]) -> Self {
        Self(slice)
    }

    #[inline(always)]
    pub fn slice(&self) -> &'a [F16] {
        self.0
    }
}

impl<'a> VectorBorrowed for Vecf16Borrowed<'a> {
    type Scalar = F16;
    type Owned = Vecf16Owned;

    #[inline(always)]
    fn dims(&self) -> u32 {
        self.0.len() as u32
    }

    #[inline(always)]
    fn own(&self) -> Vecf16Owned {
        Vecf16Owned(self.0.to_vec())
    }

    #[inline(always)]
    fn to_vec(&self) -> Vec<F16> {
        self.0.to_vec()
    }

    #[inline(always)]
    fn norm(&self) -> F32 {
        length(self.0).to_f()
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
    fn function_normalize(&self) -> Vecf16Owned {
        let mut data = self.0.to_vec();
        l2_normalize(&mut data);
        Vecf16Owned(data)
    }

    fn operator_add(&self, rhs: Self) -> Self::Owned {
        assert_eq!(self.0.len(), rhs.0.len());
        let n = self.dims();
        let mut slice = vec![F16::zero(); n as usize];
        for i in 0..n {
            slice[i as usize] = self.0[i as usize] + rhs.0[i as usize];
        }
        Vecf16Owned::new(slice)
    }

    fn operator_minus(&self, rhs: Self) -> Self::Owned {
        assert_eq!(self.0.len(), rhs.0.len());
        let n = self.dims();
        let mut slice = vec![F16::zero(); n as usize];
        for i in 0..n {
            slice[i as usize] = self.0[i as usize] - rhs.0[i as usize];
        }
        Vecf16Owned::new(slice)
    }

    fn operator_mul(&self, rhs: Self) -> Self::Owned {
        assert_eq!(self.0.len(), rhs.0.len());
        let n = self.dims();
        let mut slice = vec![F16::zero(); n as usize];
        for i in 0..n {
            slice[i as usize] = self.0[i as usize] * rhs.0[i as usize];
        }
        Vecf16Owned::new(slice)
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

impl<'a> PartialEq for Vecf16Borrowed<'a> {
    fn eq(&self, other: &Self) -> bool {
        if self.0.len() != other.0.len() {
            return false;
        }
        self.0 == other.0
    }
}

impl<'a> PartialOrd for Vecf16Borrowed<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.0.len() != other.0.len() {
            return None;
        }
        Some(self.0.cmp(other.0))
    }
}

#[inline]
#[cfg(target_arch = "x86_64")]
unsafe fn dot_v4_avx512fp16(lhs: &[F16], rhs: &[F16]) -> F32 {
    assert!(lhs.len() == rhs.len());
    let n = lhs.len();
    unsafe { ccc::v_f16_dot_avx512fp16(lhs.as_ptr().cast(), rhs.as_ptr().cast(), n).into() }
}

#[cfg(all(target_arch = "x86_64", test))]
#[test]
fn dot_v4_avx512fp16_test() {
    const EPSILON: F32 = F32(2.0);
    detect::init();
    if !detect::v4_avx512fp16::detect() {
        println!("test {} ... skipped (v4_avx512fp16)", module_path!());
        return;
    }
    for _ in 0..300 {
        let n = 4000;
        let lhs = (0..n).map(|_| F16(rand::random::<_>())).collect::<Vec<_>>();
        let rhs = (0..n).map(|_| F16(rand::random::<_>())).collect::<Vec<_>>();
        let specialized = unsafe { dot_v4_avx512fp16(&lhs, &rhs) };
        let fallback = unsafe { dot_fallback(&lhs, &rhs) };
        assert!(
            (specialized - fallback).abs() < EPSILON,
            "specialized = {specialized}, fallback = {fallback}."
        );
    }
}

#[inline]
#[cfg(target_arch = "x86_64")]
unsafe fn dot_v4(lhs: &[F16], rhs: &[F16]) -> F32 {
    assert!(lhs.len() == rhs.len());
    let n = lhs.len();
    unsafe { ccc::v_f16_dot_v4(lhs.as_ptr().cast(), rhs.as_ptr().cast(), n).into() }
}

#[cfg(all(target_arch = "x86_64", test))]
#[test]
fn dot_v4_test() {
    const EPSILON: F32 = F32(2.0);
    detect::init();
    if !detect::v4::detect() {
        println!("test {} ... skipped (v4)", module_path!());
        return;
    }
    for _ in 0..300 {
        let n = 4000;
        let lhs = (0..n).map(|_| F16(rand::random::<_>())).collect::<Vec<_>>();
        let rhs = (0..n).map(|_| F16(rand::random::<_>())).collect::<Vec<_>>();
        let specialized = unsafe { dot_v4(&lhs, &rhs) };
        let fallback = unsafe { dot_fallback(&lhs, &rhs) };
        assert!(
            (specialized - fallback).abs() < EPSILON,
            "specialized = {specialized}, fallback = {fallback}."
        );
    }
}

#[inline]
#[cfg(target_arch = "x86_64")]
unsafe fn dot_v3(lhs: &[F16], rhs: &[F16]) -> F32 {
    assert!(lhs.len() == rhs.len());
    let n = lhs.len();
    unsafe { ccc::v_f16_dot_v3(lhs.as_ptr().cast(), rhs.as_ptr().cast(), n).into() }
}

#[cfg(all(target_arch = "x86_64", test))]
#[test]
fn dot_v3_test() {
    const EPSILON: F32 = F32(2.0);
    detect::init();
    if !detect::v3::detect() {
        println!("test {} ... skipped (v3)", module_path!());
        return;
    }
    for _ in 0..300 {
        let n = 4000;
        let lhs = (0..n).map(|_| F16(rand::random::<_>())).collect::<Vec<_>>();
        let rhs = (0..n).map(|_| F16(rand::random::<_>())).collect::<Vec<_>>();
        let specialized = unsafe { dot_v3(&lhs, &rhs) };
        let fallback = unsafe { dot_fallback(&lhs, &rhs) };
        assert!(
            (specialized - fallback).abs() < EPSILON,
            "specialized = {specialized}, fallback = {fallback}."
        );
    }
}

#[detect::multiversion(v4_avx512fp16 = import, v4 = import, v3 = import, v2, neon, fallback = export)]
pub fn dot(lhs: &[F16], rhs: &[F16]) -> F32 {
    assert!(lhs.len() == rhs.len());
    let n = lhs.len();
    let mut xy = F32::zero();
    for i in 0..n {
        xy += lhs[i].to_f() * rhs[i].to_f();
    }
    xy
}

#[inline]
#[cfg(target_arch = "x86_64")]
unsafe fn sl2_v4_avx512fp16(lhs: &[F16], rhs: &[F16]) -> F32 {
    assert!(lhs.len() == rhs.len());
    let n = lhs.len();
    unsafe { ccc::v_f16_sl2_avx512fp16(lhs.as_ptr().cast(), rhs.as_ptr().cast(), n).into() }
}

#[cfg(all(target_arch = "x86_64", test))]
#[test]
fn sl2_v4_avx512fp16_test() {
    const EPSILON: F32 = F32(2.0);
    detect::init();
    if !detect::v4_avx512fp16::detect() {
        println!("test {} ... skipped (v4_avx512fp16)", module_path!());
        return;
    }
    for _ in 0..300 {
        let n = 4000;
        let lhs = (0..n).map(|_| F16(rand::random::<_>())).collect::<Vec<_>>();
        let rhs = (0..n).map(|_| F16(rand::random::<_>())).collect::<Vec<_>>();
        let specialized = unsafe { sl2_v4_avx512fp16(&lhs, &rhs) };
        let fallback = unsafe { sl2_fallback(&lhs, &rhs) };
        assert!(
            (specialized - fallback).abs() < EPSILON,
            "specialized = {specialized}, fallback = {fallback}."
        );
    }
}

#[inline]
#[cfg(target_arch = "x86_64")]
unsafe fn sl2_v4(lhs: &[F16], rhs: &[F16]) -> F32 {
    assert!(lhs.len() == rhs.len());
    let n = lhs.len();
    unsafe { ccc::v_f16_sl2_v4(lhs.as_ptr().cast(), rhs.as_ptr().cast(), n).into() }
}

#[cfg(all(target_arch = "x86_64", test))]
#[test]
fn sl2_v4_test() {
    const EPSILON: F32 = F32(2.0);
    detect::init();
    if !detect::v4::detect() {
        println!("test {} ... skipped (v4)", module_path!());
        return;
    }
    for _ in 0..300 {
        let n = 4000;
        let lhs = (0..n).map(|_| F16(rand::random::<_>())).collect::<Vec<_>>();
        let rhs = (0..n).map(|_| F16(rand::random::<_>())).collect::<Vec<_>>();
        let specialized = unsafe { sl2_v4(&lhs, &rhs) };
        let fallback = unsafe { sl2_fallback(&lhs, &rhs) };
        assert!(
            (specialized - fallback).abs() < EPSILON,
            "specialized = {specialized}, fallback = {fallback}."
        );
    }
}

#[inline]
#[cfg(target_arch = "x86_64")]
unsafe fn sl2_v3(lhs: &[F16], rhs: &[F16]) -> F32 {
    assert!(lhs.len() == rhs.len());
    let n = lhs.len();
    unsafe { ccc::v_f16_sl2_v3(lhs.as_ptr().cast(), rhs.as_ptr().cast(), n).into() }
}

#[cfg(all(target_arch = "x86_64", test))]
#[test]
fn sl2_v3_test() {
    const EPSILON: F32 = F32(2.0);
    detect::init();
    if !detect::v3::detect() {
        println!("test {} ... skipped (v3)", module_path!());
        return;
    }
    for _ in 0..300 {
        let n = 4000;
        let lhs = (0..n).map(|_| F16(rand::random::<_>())).collect::<Vec<_>>();
        let rhs = (0..n).map(|_| F16(rand::random::<_>())).collect::<Vec<_>>();
        let specialized = unsafe { sl2_v3(&lhs, &rhs) };
        let fallback = unsafe { sl2_fallback(&lhs, &rhs) };
        assert!(
            (specialized - fallback).abs() < EPSILON,
            "specialized = {specialized}, fallback = {fallback}."
        );
    }
}

#[detect::multiversion(v4_avx512fp16 = import, v4 = import, v3 = import, v2, neon, fallback = export)]
pub fn sl2(lhs: &[F16], rhs: &[F16]) -> F32 {
    assert!(lhs.len() == rhs.len());
    let n = lhs.len();
    let mut d2 = F32::zero();
    for i in 0..n {
        let d = lhs[i].to_f() - rhs[i].to_f();
        d2 += d * d;
    }
    d2
}

#[detect::multiversion(v4, v3, v2, neon, fallback)]
fn length(vector: &[F16]) -> F16 {
    let n = vector.len();
    let mut dot = F16::zero();
    for i in 0..n {
        dot += vector[i] * vector[i];
    }
    dot.sqrt()
}

#[detect::multiversion(v4, v3, v2, neon, fallback)]
pub fn l2_normalize(vector: &mut [F16]) {
    let n = vector.len();
    let l = length(vector);
    for i in 0..n {
        vector[i] /= l;
    }
}

#[detect::multiversion(v4, v3, v2, neon, fallback)]
pub fn xy_x2_y2(lhs: &[F16], rhs: &[F16]) -> (F32, F32, F32) {
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
