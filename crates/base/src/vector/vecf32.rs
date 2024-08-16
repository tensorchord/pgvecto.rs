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
    fn prefetch(&self) {
        unsafe {
            std::intrinsics::prefetch_read_data(self.0.as_ptr(), 3);
            std::intrinsics::prefetch_read_data(self.0.as_ptr().add(16), 3);
        }
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

#[cfg(target_arch = "x86_64")]
#[detect::target_cpu(enable = "v4")]
unsafe fn dot_v4(lhs: &[F32], rhs: &[F32]) -> F32 {
    assert!(lhs.len() == rhs.len());
    use std::arch::x86_64::*;
    unsafe {
        let mut n = lhs.len() as u32;
        let mut a = lhs.as_ptr();
        let mut b = rhs.as_ptr();
        let mut xy = _mm512_set1_ps(0.0);
        while n >= 16 {
            let x = _mm512_loadu_ps(a.cast());
            let y = _mm512_loadu_ps(b.cast());
            a = a.add(16);
            b = b.add(16);
            n -= 16;
            xy = _mm512_fmadd_ps(x, y, xy);
        }
        if n > 0 {
            let mask = _bzhi_u32(0xFFFF, n) as u16;
            let x = _mm512_maskz_loadu_ps(mask, a.cast());
            let y = _mm512_maskz_loadu_ps(mask, b.cast());
            xy = _mm512_fmadd_ps(x, y, xy);
        }
        F32(_mm512_reduce_add_ps(xy))
    }
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
        let n = 4010;
        let lhs = (0..n).map(|_| F32(rand::random::<_>())).collect::<Vec<_>>();
        let rhs = (0..n).map(|_| F32(rand::random::<_>())).collect::<Vec<_>>();
        for z in 3990..4010 {
            let lhs = &lhs[..z];
            let rhs = &rhs[..z];
            let specialized = unsafe { dot_v4(&lhs, &rhs) };
            let fallback = unsafe { dot_fallback(&lhs, &rhs) };
            assert!(
                (specialized - fallback).abs() < EPSILON,
                "specialized = {specialized}, fallback = {fallback}."
            );
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[detect::target_cpu(enable = "v3")]
unsafe fn dot_v3(lhs: &[F32], rhs: &[F32]) -> F32 {
    assert!(lhs.len() == rhs.len());
    use std::arch::x86_64::*;
    unsafe {
        let mut n = lhs.len() as u32;
        let mut a = lhs.as_ptr();
        let mut b = rhs.as_ptr();
        let mut xy = _mm256_set1_ps(0.0);
        while n >= 8 {
            let x = _mm256_loadu_ps(a.cast());
            let y = _mm256_loadu_ps(b.cast());
            a = a.add(8);
            b = b.add(8);
            n -= 8;
            xy = _mm256_fmadd_ps(x, y, xy);
        }
        #[inline]
        #[detect::target_cpu(enable = "v3")]
        unsafe fn _mm256_reduce_add_ps(mut x: __m256) -> f32 {
            unsafe {
                x = _mm256_add_ps(x, _mm256_permute2f128_ps(x, x, 1));
                x = _mm256_hadd_ps(x, x);
                x = _mm256_hadd_ps(x, x);
                _mm256_cvtss_f32(x)
            }
        }
        let mut xy = F32(_mm256_reduce_add_ps(xy));
        while n > 0 {
            let x = a.read();
            let y = b.read();
            a = a.add(1);
            b = b.add(1);
            n -= 1;
            xy += x * y;
        }
        xy
    }
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
        let n = 4010;
        let lhs = (0..n).map(|_| F32(rand::random::<_>())).collect::<Vec<_>>();
        let rhs = (0..n).map(|_| F32(rand::random::<_>())).collect::<Vec<_>>();
        for z in 3990..4010 {
            let lhs = &lhs[..z];
            let rhs = &rhs[..z];
            let specialized = unsafe { dot_v3(&lhs, &rhs) };
            let fallback = unsafe { dot_fallback(&lhs, &rhs) };
            assert!(
                (specialized - fallback).abs() < EPSILON,
                "specialized = {specialized}, fallback = {fallback}."
            );
        }
    }
}

#[detect::multiversion(v4 = import, v3 = import, v2, neon, fallback = export)]
pub fn dot(lhs: &[F32], rhs: &[F32]) -> F32 {
    assert!(lhs.len() == rhs.len());
    let n = lhs.len();
    let mut xy = F32::zero();
    for i in 0..n {
        xy += lhs[i] * rhs[i];
    }
    xy
}

#[cfg(target_arch = "x86_64")]
#[detect::target_cpu(enable = "v4")]
unsafe fn sl2_v4(lhs: &[F32], rhs: &[F32]) -> F32 {
    assert!(lhs.len() == rhs.len());
    use std::arch::x86_64::*;
    unsafe {
        let mut n = lhs.len() as u32;
        let mut a = lhs.as_ptr();
        let mut b = rhs.as_ptr();
        let mut dd = _mm512_set1_ps(0.0);
        while n >= 16 {
            let x = _mm512_loadu_ps(a.cast());
            let y = _mm512_loadu_ps(b.cast());
            a = a.add(16);
            b = b.add(16);
            n -= 16;
            let d = _mm512_sub_ps(x, y);
            dd = _mm512_fmadd_ps(d, d, dd);
        }
        if n > 0 {
            let mask = _bzhi_u32(0xFFFF, n) as u16;
            let x = _mm512_maskz_loadu_ps(mask, a.cast());
            let y = _mm512_maskz_loadu_ps(mask, b.cast());
            let d = _mm512_sub_ps(x, y);
            dd = _mm512_fmadd_ps(d, d, dd);
        }
        F32(_mm512_reduce_add_ps(dd))
    }
}

#[cfg(target_arch = "x86_64")]
#[detect::target_cpu(enable = "v3")]
unsafe fn sl2_v3(lhs: &[F32], rhs: &[F32]) -> F32 {
    assert!(lhs.len() == rhs.len());
    use std::arch::x86_64::*;
    unsafe {
        let mut n = lhs.len() as u32;
        let mut a = lhs.as_ptr();
        let mut b = rhs.as_ptr();
        let mut dd = _mm256_set1_ps(0.0);
        while n >= 8 {
            let x = _mm256_loadu_ps(a.cast());
            let y = _mm256_loadu_ps(b.cast());
            a = a.add(8);
            b = b.add(8);
            n -= 8;
            let d = _mm256_sub_ps(x, y);
            dd = _mm256_fmadd_ps(d, d, dd);
        }
        if n >= 4 {
            let x = _mm_loadu_ps(a.cast());
            let y = _mm_loadu_ps(b.cast());
            a = a.add(4);
            b = b.add(4);
            n -= 4;
            let d = _mm256_zextps128_ps256(_mm_sub_ps(x, y));
            dd = _mm256_fmadd_ps(d, d, dd);
        }
        #[inline]
        #[detect::target_cpu(enable = "v3")]
        unsafe fn _mm256_reduce_add_ps(mut x: __m256) -> f32 {
            unsafe {
                x = _mm256_add_ps(x, _mm256_permute2f128_ps(x, x, 1));
                x = _mm256_hadd_ps(x, x);
                x = _mm256_hadd_ps(x, x);
                _mm256_cvtss_f32(x)
            }
        }
        let mut rdd = F32(_mm256_reduce_add_ps(dd));
        if std::intrinsics::unlikely(n > 0) {
            while n > 0 {
                let x = a.read();
                let y = b.read();
                a = a.add(1);
                b = b.add(1);
                n -= 1;
                rdd += (x - y) * (x - y);
            }
        }
        rdd
    }
}

#[detect::target_cpu(enable = "v3")]
unsafe fn sqr_dist(mut d: *const f32, mut q: *const f32) -> f32 {
    #[repr(align(32))]
    struct TmpRes([f32; 8]);

    use std::arch::x86_64::*;

    unsafe {
        let mut r = TmpRes([0.0f32; 8]);

        let mut sum = _mm256_set1_ps(0.0);
        for _ in 0..6 {
            let v1 = _mm256_loadu_ps(d);
            let v2 = _mm256_loadu_ps(q);
            d = d.add(8);
            q = q.add(8);
            let diff = _mm256_sub_ps(v1, v2);
            sum = _mm256_add_ps(sum, _mm256_mul_ps(diff, diff));

            let v1 = _mm256_loadu_ps(d);
            let v2 = _mm256_loadu_ps(q);
            d = d.add(8);
            q = q.add(8);
            let diff = _mm256_sub_ps(v1, v2);
            sum = _mm256_add_ps(sum, _mm256_mul_ps(diff, diff));
        }
        _mm256_store_ps(r.0.as_mut_ptr(), sum);

        let mut ret = r.0[0] + r.0[1] + r.0[2] + r.0[3] + r.0[4] + r.0[5] + r.0[6] + r.0[7];

        for _ in 0..4 {
            let tmp = (*q) - (*d);
            ret += tmp * tmp;
            d = d.add(1);
            q = q.add(1);
        }
        ret
    }
}

#[inline(always)]
pub fn sl2(lhs: &[F32], rhs: &[F32]) -> F32 {
    assert!(lhs.len() == 100);
    assert!(rhs.len() == 100);
    unsafe { F32(sqr_dist(lhs.as_ptr().cast(), rhs.as_ptr().cast())) }
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
