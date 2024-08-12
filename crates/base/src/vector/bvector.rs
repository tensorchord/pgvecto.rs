use std::ops::{Bound, RangeBounds};

use crate::scalar::F32;
use crate::vector::{Vecf32Owned, VectorBorrowed, VectorKind, VectorOwned};
use num_traits::Float;
use serde::{Deserialize, Serialize};

pub const BVECTOR_WIDTH: u32 = u64::BITS;

// When using binary vector, please ensure that the padding bits are always zero.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BVectorOwned {
    dims: u32,
    data: Vec<u64>,
}

impl BVectorOwned {
    #[inline(always)]
    pub fn new(dims: u32, data: Vec<u64>) -> Self {
        Self::new_checked(dims, data).expect("invalid data")
    }

    #[inline(always)]
    pub fn new_checked(dims: u32, data: Vec<u64>) -> Option<Self> {
        if !(1..=65535).contains(&dims) {
            return None;
        }
        if data.len() != dims.div_ceil(BVECTOR_WIDTH) as usize {
            return None;
        }
        if dims % BVECTOR_WIDTH != 0 && data[data.len() - 1] >> (dims % BVECTOR_WIDTH) != 0 {
            return None;
        }
        unsafe { Some(Self::new_unchecked(dims, data)) }
    }

    /// # Safety
    ///
    /// * `dims` must be in `1..=65535`.
    /// * `data` must be of the correct length.
    /// * The padding bits must be zero.
    #[inline(always)]
    pub unsafe fn new_unchecked(dims: u32, data: Vec<u64>) -> Self {
        Self { dims, data }
    }
}

impl VectorOwned for BVectorOwned {
    type Scalar = F32;
    type Borrowed<'a> = BVectorBorrowed<'a>;

    const VECTOR_KIND: VectorKind = VectorKind::BVector;

    #[inline(always)]
    fn as_borrowed(&self) -> BVectorBorrowed<'_> {
        BVectorBorrowed {
            dims: self.dims,
            data: &self.data,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BVectorBorrowed<'a> {
    dims: u32,
    data: &'a [u64],
}

impl<'a> BVectorBorrowed<'a> {
    #[inline(always)]
    pub fn new(dims: u32, data: &'a [u64]) -> Self {
        Self::new_checked(dims, data).expect("invalid data")
    }

    #[inline(always)]
    pub fn new_checked(dims: u32, data: &'a [u64]) -> Option<Self> {
        if !(1..=65535).contains(&dims) {
            return None;
        }
        if data.len() != dims.div_ceil(BVECTOR_WIDTH) as usize {
            return None;
        }
        if dims % BVECTOR_WIDTH != 0 && data[data.len() - 1] >> (dims % BVECTOR_WIDTH) != 0 {
            return None;
        }
        unsafe { Some(Self::new_unchecked(dims, data)) }
    }

    /// # Safety
    ///
    /// * `dims` must be in `1..=65535`.
    /// * `data` must be of the correct length.
    /// * The padding bits must be zero.
    #[inline(always)]
    pub unsafe fn new_unchecked(dims: u32, data: &'a [u64]) -> Self {
        Self { dims, data }
    }

    #[inline(always)]
    pub fn data(&self) -> &'a [u64] {
        self.data
    }

    #[inline(always)]
    pub fn get(&self, index: u32) -> bool {
        assert!(index < self.dims);
        self.data[(index / BVECTOR_WIDTH) as usize] & (1 << (index % BVECTOR_WIDTH)) != 0
    }

    #[inline(always)]
    pub fn iter(self) -> impl Iterator<Item = bool> + 'a {
        let mut index = 0_u32;
        std::iter::from_fn(move || {
            if index < self.dims {
                let result = self.data[(index / BVECTOR_WIDTH) as usize]
                    & (1 << (index % BVECTOR_WIDTH))
                    != 0;
                index += 1;
                Some(result)
            } else {
                None
            }
        })
    }
}

impl<'a> VectorBorrowed for BVectorBorrowed<'a> {
    type Scalar = F32;
    type Owned = BVectorOwned;

    #[inline(always)]
    fn dims(&self) -> u32 {
        self.dims
    }

    fn own(&self) -> BVectorOwned {
        BVectorOwned {
            dims: self.dims,
            data: self.data.to_vec(),
        }
    }

    #[inline(always)]
    fn to_vec(&self) -> Vec<F32> {
        self.iter().map(|i| F32(i as u32 as f32)).collect()
    }

    #[inline(always)]
    fn norm(&self) -> F32 {
        length(*self)
    }

    #[inline(always)]
    fn operator_dot(self, rhs: Self) -> F32 {
        dot(self, rhs) * (-1.0)
    }

    #[inline(always)]
    fn operator_l2(self, _: Self) -> F32 {
        unimplemented!()
    }

    #[inline(always)]
    fn operator_cos(self, _: Self) -> F32 {
        unimplemented!()
    }

    #[inline(always)]
    fn operator_hamming(self, rhs: Self) -> F32 {
        hamming(self, rhs)
    }

    #[inline(always)]
    fn operator_jaccard(self, rhs: Self) -> F32 {
        F32(1.0) - jaccard(self, rhs)
    }

    #[inline(always)]
    fn function_normalize(&self) -> BVectorOwned {
        unimplemented!()
    }

    fn operator_add(&self, _: Self) -> Self::Owned {
        unimplemented!()
    }

    fn operator_minus(&self, _: Self) -> Self::Owned {
        unimplemented!()
    }

    fn operator_mul(&self, _: Self) -> Self::Owned {
        unimplemented!()
    }

    fn operator_and(&self, rhs: Self) -> Self::Owned {
        assert_eq!(self.dims, rhs.dims);
        assert_eq!(self.data.len(), rhs.data.len());
        let mut data = vec![0_u64; self.data.len()];
        for i in 0..data.len() {
            data[i] = self.data[i] & rhs.data[i];
        }
        BVectorOwned::new(self.dims, data)
    }

    fn operator_or(&self, rhs: Self) -> Self::Owned {
        assert_eq!(self.dims, rhs.dims);
        assert_eq!(self.data.len(), rhs.data.len());
        let mut data = vec![0_u64; self.data.len()];
        for i in 0..data.len() {
            data[i] = self.data[i] | rhs.data[i];
        }
        BVectorOwned::new(self.dims, data)
    }

    fn operator_xor(&self, rhs: Self) -> Self::Owned {
        assert_eq!(self.dims, rhs.dims);
        assert_eq!(self.data.len(), rhs.data.len());
        let mut data = vec![0_u64; self.data.len()];
        for i in 0..data.len() {
            data[i] = self.data[i] ^ rhs.data[i];
        }
        BVectorOwned::new(self.dims, data)
    }

    #[inline(always)]
    fn subvector(&self, bounds: impl RangeBounds<u32>) -> Option<Self::Owned> {
        let start = match bounds.start_bound().cloned() {
            Bound::Included(x) => x,
            Bound::Excluded(u32::MAX) => return None,
            Bound::Excluded(x) => x + 1,
            Bound::Unbounded => 0,
        };
        let end = match bounds.end_bound().cloned() {
            Bound::Included(u32::MAX) => return None,
            Bound::Included(x) => x + 1,
            Bound::Excluded(x) => x,
            Bound::Unbounded => self.dims,
        };
        if start >= end || end > self.dims {
            return None;
        }
        let dims = end - start;
        let mut data = vec![0_u64; dims.div_ceil(BVECTOR_WIDTH) as _];
        {
            let mut i = 0;
            let mut j = start;
            while j < end {
                if self.data[(j / BVECTOR_WIDTH) as usize] & (1 << (j % BVECTOR_WIDTH)) != 0 {
                    data[(i / BVECTOR_WIDTH) as usize] |= 1 << (i % BVECTOR_WIDTH);
                }
                i += 1;
                j += 1;
            }
        }
        Self::Owned::new_checked(dims, data)
    }
}

impl<'a> PartialEq for BVectorBorrowed<'a> {
    fn eq(&self, other: &Self) -> bool {
        if self.dims != other.dims {
            return false;
        }
        for (&l, &r) in self.data.iter().zip(other.data.iter()) {
            let l = l.reverse_bits();
            let r = r.reverse_bits();
            if l != r {
                return false;
            }
        }
        true
    }
}

impl<'a> PartialOrd for BVectorBorrowed<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        use std::cmp::Ordering;
        if self.dims != other.dims {
            return None;
        }
        for (&l, &r) in self.data.iter().zip(other.data.iter()) {
            let l = l.reverse_bits();
            let r = r.reverse_bits();
            match l.cmp(&r) {
                Ordering::Equal => (),
                x => return Some(x),
            }
        }
        Some(Ordering::Equal)
    }
}

#[inline]
#[cfg(target_arch = "x86_64")]
#[detect::target_cpu(enable = "v4_avx512vpopcntdq")]
unsafe fn dot_v4_avx512vpopcntdq(lhs: BVectorBorrowed<'_>, rhs: BVectorBorrowed<'_>) -> F32 {
    use std::arch::x86_64::*;
    let lhs = lhs.data();
    let rhs = rhs.data();
    assert!(lhs.len() == rhs.len());
    unsafe {
        const WIDTH: usize = 512 / 8 / size_of::<usize>();
        let mut xy = _mm512_setzero_si512();
        let mut a = lhs.as_ptr();
        let mut b = rhs.as_ptr();
        let mut n = lhs.len();
        while n >= WIDTH {
            let x = _mm512_loadu_si512(a.cast());
            let y = _mm512_loadu_si512(b.cast());
            a = a.add(WIDTH);
            b = b.add(WIDTH);
            n -= WIDTH;
            xy = _mm512_add_epi64(xy, _mm512_popcnt_epi64(_mm512_and_si512(x, y)));
        }
        if n > 0 {
            let mask = _bzhi_u32(0xFFFF, n as u32) as u8;
            let x = _mm512_maskz_loadu_epi64(mask, a.cast());
            let y = _mm512_maskz_loadu_epi64(mask, b.cast());
            xy = _mm512_add_epi64(xy, _mm512_popcnt_epi64(_mm512_and_si512(x, y)));
        }
        let rxy = _mm512_reduce_add_epi64(xy) as f32;
        F32(rxy)
    }
}

#[cfg(all(target_arch = "x86_64", test))]
#[test]
fn dot_v4_avx512vpopcntdq_test() {
    const EPSILON: F32 = F32(1e-5);
    detect::init();
    if !detect::v4_avx512vpopcntdq::detect() {
        println!("test {} ... skipped (v4_avx512vpopcntdq)", module_path!());
        return;
    }
    for _ in 0..300 {
        let lhs = random_bvector();
        let rhs = random_bvector();
        let specialized = unsafe { dot_v4_avx512vpopcntdq(lhs.as_borrowed(), rhs.as_borrowed()) };
        let fallback = unsafe { dot_fallback(lhs.as_borrowed(), rhs.as_borrowed()) };
        assert!(
            (specialized - fallback).abs() < EPSILON,
            "specialized = {specialized}, fallback = {fallback}."
        );
    }
}

#[detect::multiversion(v4_avx512vpopcntdq = import, v4, v3, v2, neon, fallback = export)]
pub fn dot(lhs: BVectorBorrowed<'_>, rhs: BVectorBorrowed<'_>) -> F32 {
    let lhs = lhs.data();
    let rhs = rhs.data();
    assert!(lhs.len() == rhs.len());
    let mut xy = 0;
    for i in 0..lhs.len() {
        xy += (lhs[i] & rhs[i]).count_ones();
    }
    F32(xy as f32)
}

#[inline]
#[cfg(target_arch = "x86_64")]
#[detect::target_cpu(enable = "v4_avx512vpopcntdq")]
unsafe fn hamming_v4_avx512vpopcntdq(lhs: BVectorBorrowed<'_>, rhs: BVectorBorrowed<'_>) -> F32 {
    use std::arch::x86_64::*;
    let lhs = lhs.data();
    let rhs = rhs.data();
    assert!(lhs.len() == rhs.len());
    unsafe {
        const WIDTH: usize = 512 / 8 / size_of::<usize>();
        let mut dd = _mm512_setzero_si512();
        let mut a = lhs.as_ptr();
        let mut b = rhs.as_ptr();
        let mut n = lhs.len();
        while n >= WIDTH {
            let x = _mm512_loadu_si512(a.cast());
            let y = _mm512_loadu_si512(b.cast());
            a = a.add(WIDTH);
            b = b.add(WIDTH);
            n -= WIDTH;
            dd = _mm512_add_epi64(dd, _mm512_popcnt_epi64(_mm512_xor_si512(x, y)));
        }
        if n > 0 {
            let mask = _bzhi_u32(0xFFFF, n as u32) as u8;
            let x = _mm512_maskz_loadu_epi64(mask, a.cast());
            let y = _mm512_maskz_loadu_epi64(mask, b.cast());
            dd = _mm512_add_epi64(dd, _mm512_popcnt_epi64(_mm512_xor_si512(x, y)));
        }
        let rdd = _mm512_reduce_add_epi64(dd) as f32;
        F32(rdd)
    }
}

#[cfg(all(target_arch = "x86_64", test))]
#[test]
fn hamming_v4_avx512vpopcntdq_test() {
    const EPSILON: F32 = F32(1e-5);
    detect::init();
    if !detect::v4_avx512vpopcntdq::detect() {
        println!("test {} ... skipped (v4_avx512vpopcntdq)", module_path!());
        return;
    }
    for _ in 0..300 {
        let lhs = random_bvector();
        let rhs = random_bvector();
        let specialized =
            unsafe { hamming_v4_avx512vpopcntdq(lhs.as_borrowed(), rhs.as_borrowed()) };
        let fallback = unsafe { hamming_fallback(lhs.as_borrowed(), rhs.as_borrowed()) };
        assert!(
            (specialized - fallback).abs() < EPSILON,
            "specialized = {specialized}, fallback = {fallback}."
        );
    }
}

#[detect::multiversion(v4_avx512vpopcntdq = import, v4, v3, v2, neon, fallback = export)]
pub fn hamming(lhs: BVectorBorrowed<'_>, rhs: BVectorBorrowed<'_>) -> F32 {
    let lhs = lhs.data();
    let rhs = rhs.data();
    assert!(lhs.len() == rhs.len());
    let mut dd = 0;
    for i in 0..lhs.len() {
        dd += (lhs[i] ^ rhs[i]).count_ones();
    }
    F32(dd as f32)
}

#[inline]
#[cfg(target_arch = "x86_64")]
#[detect::target_cpu(enable = "v4_avx512vpopcntdq")]
unsafe fn jaccard_v4_avx512vpopcntdq(lhs: BVectorBorrowed<'_>, rhs: BVectorBorrowed<'_>) -> F32 {
    use std::arch::x86_64::*;
    let lhs = lhs.data();
    let rhs = rhs.data();
    assert!(lhs.len() == rhs.len());
    unsafe {
        const WIDTH: usize = 512 / 8 / size_of::<usize>();
        let mut inter = _mm512_setzero_si512();
        let mut union = _mm512_setzero_si512();
        let mut a = lhs.as_ptr();
        let mut b = rhs.as_ptr();
        let mut n = lhs.len();
        while n >= WIDTH {
            let x = _mm512_loadu_si512(a.cast());
            let y = _mm512_loadu_si512(b.cast());
            a = a.add(WIDTH);
            b = b.add(WIDTH);
            n -= WIDTH;
            inter = _mm512_add_epi64(inter, _mm512_popcnt_epi64(_mm512_and_si512(x, y)));
            union = _mm512_add_epi64(union, _mm512_popcnt_epi64(_mm512_or_si512(x, y)));
        }
        if n > 0 {
            let mask = _bzhi_u32(0xFFFF, n as u32) as u8;
            let x = _mm512_maskz_loadu_epi64(mask, a.cast());
            let y = _mm512_maskz_loadu_epi64(mask, b.cast());
            inter = _mm512_add_epi64(inter, _mm512_popcnt_epi64(_mm512_and_si512(x, y)));
            union = _mm512_add_epi64(union, _mm512_popcnt_epi64(_mm512_or_si512(x, y)));
        }
        let rinter = _mm512_reduce_add_epi64(inter) as f32;
        let runion = _mm512_reduce_add_epi64(union) as f32;
        F32(rinter / runion)
    }
}

#[cfg(all(target_arch = "x86_64", test))]
#[test]
fn jaccard_v4_avx512vpopcntdq_test() {
    const EPSILON: F32 = F32(1e-5);
    detect::init();
    if !detect::v4_avx512vpopcntdq::detect() {
        println!("test {} ... skipped (v4_avx512vpopcntdq)", module_path!());
        return;
    }
    for _ in 0..300 {
        let lhs = random_bvector();
        let rhs = random_bvector();
        let specialized =
            unsafe { jaccard_v4_avx512vpopcntdq(lhs.as_borrowed(), rhs.as_borrowed()) };
        let fallback = unsafe { jaccard_fallback(lhs.as_borrowed(), rhs.as_borrowed()) };
        assert!(
            (specialized - fallback).abs() < EPSILON,
            "specialized = {specialized}, fallback = {fallback}."
        );
    }
}

#[detect::multiversion(v4_avx512vpopcntdq = import, v4, v3, v2, neon, fallback = export)]
pub fn jaccard(lhs: BVectorBorrowed<'_>, rhs: BVectorBorrowed<'_>) -> F32 {
    let lhs = lhs.data();
    let rhs = rhs.data();
    assert!(lhs.len() == rhs.len());
    let mut inter = 0;
    let mut union = 0;
    for i in 0..lhs.len() {
        inter += (lhs[i] & rhs[i]).count_ones();
        union += (lhs[i] | rhs[i]).count_ones();
    }
    F32(inter as f32 / union as f32)
}

#[inline]
#[cfg(target_arch = "x86_64")]
#[detect::target_cpu(enable = "v4_avx512vpopcntdq")]
unsafe fn length_v4_avx512vpopcntdq(vector: BVectorBorrowed<'_>) -> F32 {
    use std::arch::x86_64::*;
    let lhs = vector.data();
    unsafe {
        const WIDTH: usize = 512 / 8 / size_of::<usize>();
        let mut cnt = _mm512_setzero_si512();
        let mut a = lhs.as_ptr();
        let mut n = lhs.len();
        while n >= WIDTH {
            let x = _mm512_loadu_si512(a.cast());
            a = a.add(WIDTH);
            n -= WIDTH;
            cnt = _mm512_add_epi64(cnt, _mm512_popcnt_epi64(x));
        }
        if n > 0 {
            let mask = _bzhi_u32(0xFFFF, n as u32) as u8;
            let x = _mm512_maskz_loadu_epi64(mask, a.cast());
            cnt = _mm512_add_epi64(cnt, _mm512_popcnt_epi64(x));
        }
        let rcnt = _mm512_reduce_add_epi64(cnt) as f32;
        F32(rcnt.sqrt())
    }
}

#[detect::multiversion(v4_avx512vpopcntdq = import, v4, v3, v2, neon, fallback = export)]
pub fn length(vector: BVectorBorrowed<'_>) -> F32 {
    let vector = vector.data();
    let mut l = 0;
    for i in 0..vector.len() {
        l += vector[i].count_ones();
    }
    F32(l as f32).sqrt()
}

#[detect::multiversion(v4, v3, v2, neon, fallback)]
pub fn l2_normalize<'a>(vector: BVectorBorrowed<'a>) -> Vecf32Owned {
    let l = length(vector);
    Vecf32Owned::new(vector.iter().map(|i| F32(i as u32 as f32) / l).collect())
}

#[cfg(all(target_arch = "x86_64", test))]
fn random_bvector() -> BVectorOwned {
    let mut x = vec![0; 126];
    x.fill_with(rand::random);
    x[125] &= 1;
    BVectorOwned::new(8001, x)
}
