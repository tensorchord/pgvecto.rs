use crate::scalar::F32;
use crate::vector::{Vecf32Owned, VectorBorrowed, VectorKind, VectorOwned};
use num_traits::Float;
use serde::{Deserialize, Serialize};

pub const BVEC_WIDTH: usize = usize::BITS as usize;

// When using binary vector, please ensure that the padding bits are always zero.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BVecf32Owned {
    dims: u16,
    data: Vec<usize>,
}

impl BVecf32Owned {
    pub fn new_zeroed(dims: u16) -> Self {
        assert!((1..=65535).contains(&dims));
        let size = (dims as usize).div_ceil(BVEC_WIDTH);
        Self {
            dims,
            data: vec![0; size],
        }
    }

    pub fn set(&mut self, index: usize, value: bool) {
        assert!(index < self.dims as usize);
        if value {
            self.data[index / BVEC_WIDTH] |= 1 << (index % BVEC_WIDTH);
        } else {
            self.data[index / BVEC_WIDTH] &= !(1 << (index % BVEC_WIDTH));
        }
    }

    /// # Safety
    ///
    /// The caller must ensure that it won't modify the padding bits
    #[inline(always)]
    pub unsafe fn data_mut(&mut self) -> &mut [usize] {
        &mut self.data
    }
}

impl VectorOwned for BVecf32Owned {
    type Scalar = F32;
    type Borrowed<'a> = BVecf32Borrowed<'a>;

    const VECTOR_KIND: VectorKind = VectorKind::BVecf32;

    #[inline(always)]
    fn dims(&self) -> u32 {
        self.dims as u32
    }

    fn for_borrow(&self) -> BVecf32Borrowed<'_> {
        BVecf32Borrowed {
            dims: self.dims,
            data: &self.data,
        }
    }

    fn to_vec(&self) -> Vec<F32> {
        self.for_borrow().to_vec()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BVecf32Borrowed<'a> {
    dims: u16,
    data: &'a [usize],
}

impl<'a> BVecf32Borrowed<'a> {
    #[inline(always)]
    pub fn new(dims: u16, data: &'a [usize]) -> Self {
        Self::new_checked(dims, data).unwrap()
    }
    #[inline(always)]
    pub fn new_checked(dims: u16, data: &'a [usize]) -> Option<Self> {
        if dims == 0 {
            return None;
        }
        if data.len() != (dims as usize).div_ceil(BVEC_WIDTH) {
            return None;
        }
        if data[data.len() - 1] >> (dims % BVEC_WIDTH as u16) != 0 {
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
    pub unsafe fn new_unchecked(dims: u16, data: &'a [usize]) -> Self {
        Self { dims, data }
    }

    pub fn iter(self) -> impl Iterator<Item = bool> + 'a {
        let mut index = 0;
        std::iter::from_fn(move || {
            if index < self.dims as usize {
                let result = self.data[index / BVEC_WIDTH] & (1 << (index % BVEC_WIDTH)) != 0;
                index += 1;
                Some(result)
            } else {
                None
            }
        })
    }

    pub fn get(&self, index: usize) -> bool {
        assert!(index < self.dims as usize);
        self.data[index / BVEC_WIDTH] & (1 << (index % BVEC_WIDTH)) != 0
    }

    #[inline(always)]
    pub fn data(&self) -> &'a [usize] {
        self.data
    }
}

impl<'a> VectorBorrowed for BVecf32Borrowed<'a> {
    type Scalar = F32;
    type Owned = BVecf32Owned;

    #[inline(always)]
    fn dims(&self) -> u32 {
        self.dims as u32
    }

    fn for_own(&self) -> BVecf32Owned {
        BVecf32Owned {
            dims: self.dims,
            data: self.data.to_vec(),
        }
    }

    fn to_vec(&self) -> Vec<F32> {
        self.iter().map(|i| F32(i as u32 as f32)).collect()
    }
}

impl<'a> Ord for BVecf32Borrowed<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        assert_eq!(self.dims, other.dims);
        for (&l, &r) in self.data.iter().zip(other.data.iter()) {
            let l = l.reverse_bits();
            let r = r.reverse_bits();
            match l.cmp(&r) {
                std::cmp::Ordering::Equal => {}
                x => return x,
            }
        }
        std::cmp::Ordering::Equal
    }
}

impl<'a> PartialOrd for BVecf32Borrowed<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[inline(always)]
pub fn cosine<'a>(lhs: BVecf32Borrowed<'a>, rhs: BVecf32Borrowed<'a>) -> F32 {
    let lhs = lhs.data();
    let rhs = rhs.data();
    assert!(lhs.len() == rhs.len());

    #[inline(always)]
    #[multiversion::multiversion(targets(
        "x86_64/x86-64-v4",
        "x86_64/x86-64-v3",
        "x86_64/x86-64-v2",
        "aarch64+neon"
    ))]
    fn cosine(lhs: &[usize], rhs: &[usize]) -> F32 {
        let mut xy = 0;
        let mut xx = 0;
        let mut yy = 0;
        for i in 0..lhs.len() {
            xy += (lhs[i] & rhs[i]).count_ones();
            xx += lhs[i].count_ones();
            yy += rhs[i].count_ones();
        }
        let rxy = xy as f32;
        let rxx = xx as f32;
        let ryy = yy as f32;
        F32(rxy / (rxx * ryy).sqrt())
    }

    #[inline]
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx512vpopcntdq,avx512bw,avx512f,bmi2")]
    unsafe fn cosine_avx512vpopcntdq(lhs: &[usize], rhs: &[usize]) -> F32 {
        use std::arch::x86_64::*;
        #[inline]
        #[target_feature(enable = "avx512vpopcntdq,avx512bw,avx512f,bmi2")]
        pub unsafe fn _mm512_maskz_loadu_epi64(k: __mmask8, mem_addr: *const i8) -> __m512i {
            let mut dst: __m512i;
            unsafe {
                std::arch::asm!(
                    "vmovdqu64 {dst}{{{k}}} {{z}}, [{p}]",
                    p = in(reg) mem_addr,
                    k = in(kreg) k,
                    dst = out(zmm_reg) dst,
                    options(pure, readonly, nostack)
                );
            }
            dst
        }
        assert_eq!(lhs.len(), rhs.len());
        unsafe {
            const WIDTH: usize = 512 / 8 / std::mem::size_of::<usize>();
            let mut xy = _mm512_setzero_si512();
            let mut xx = _mm512_setzero_si512();
            let mut yy = _mm512_setzero_si512();
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
                xx = _mm512_add_epi64(xx, _mm512_popcnt_epi64(x));
                yy = _mm512_add_epi64(yy, _mm512_popcnt_epi64(y));
            }
            if n > 0 {
                let mask = _bzhi_u32(0xFFFF, n as u32) as u8;
                let x = _mm512_maskz_loadu_epi64(mask, a.cast());
                let y = _mm512_maskz_loadu_epi64(mask, b.cast());
                xy = _mm512_add_epi64(xy, _mm512_popcnt_epi64(_mm512_and_si512(x, y)));
                xx = _mm512_add_epi64(xx, _mm512_popcnt_epi64(x));
                yy = _mm512_add_epi64(yy, _mm512_popcnt_epi64(y));
            }
            let rxy = _mm512_reduce_add_epi64(xy) as f32;
            let rxx = _mm512_reduce_add_epi64(xx) as f32;
            let ryy = _mm512_reduce_add_epi64(yy) as f32;
            F32(rxy / (rxx * ryy).sqrt())
        }
    }

    #[cfg(target_arch = "x86_64")]
    if detect::x86_64::detect_avx512vpopcntdq() {
        unsafe {
            return cosine_avx512vpopcntdq(lhs, rhs);
        }
    }
    cosine(lhs, rhs)
}

#[inline(always)]
pub fn dot<'a>(lhs: BVecf32Borrowed<'a>, rhs: BVecf32Borrowed<'a>) -> F32 {
    let lhs = lhs.data();
    let rhs = rhs.data();
    assert!(lhs.len() == rhs.len());

    #[inline(always)]
    #[multiversion::multiversion(targets(
        "x86_64/x86-64-v4",
        "x86_64/x86-64-v3",
        "x86_64/x86-64-v2",
        "aarch64+neon"
    ))]
    fn dot(lhs: &[usize], rhs: &[usize]) -> F32 {
        let mut xy = 0;
        for i in 0..lhs.len() {
            xy += (lhs[i] & rhs[i]).count_ones();
        }
        F32(xy as f32)
    }

    #[inline]
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx512vpopcntdq,avx512bw,avx512f,bmi2")]
    unsafe fn dot_avx512vpopcntdq(lhs: &[usize], rhs: &[usize]) -> F32 {
        use std::arch::x86_64::*;
        #[inline]
        #[target_feature(enable = "avx512vpopcntdq,avx512bw,avx512f,bmi2")]
        pub unsafe fn _mm512_maskz_loadu_epi64(k: __mmask8, mem_addr: *const i8) -> __m512i {
            let mut dst: __m512i;
            unsafe {
                std::arch::asm!(
                    "vmovdqu64 {dst}{{{k}}} {{z}}, [{p}]",
                    p = in(reg) mem_addr,
                    k = in(kreg) k,
                    dst = out(zmm_reg) dst,
                    options(pure, readonly, nostack)
                );
            }
            dst
        }
        assert_eq!(lhs.len(), rhs.len());
        unsafe {
            const WIDTH: usize = 512 / 8 / std::mem::size_of::<usize>();
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

    #[cfg(target_arch = "x86_64")]
    if detect::x86_64::detect_avx512vpopcntdq() {
        unsafe {
            return dot_avx512vpopcntdq(lhs, rhs);
        }
    }
    dot(lhs, rhs)
}

#[inline(always)]
pub fn sl2<'a>(lhs: BVecf32Borrowed<'a>, rhs: BVecf32Borrowed<'a>) -> F32 {
    let lhs = lhs.data();
    let rhs = rhs.data();
    assert!(lhs.len() == rhs.len());

    #[inline(always)]
    #[multiversion::multiversion(targets(
        "x86_64/x86-64-v4",
        "x86_64/x86-64-v3",
        "x86_64/x86-64-v2",
        "aarch64+neon"
    ))]
    fn sl2(lhs: &[usize], rhs: &[usize]) -> F32 {
        let mut dd = 0;
        for i in 0..lhs.len() {
            dd += (lhs[i] ^ rhs[i]).count_ones();
        }
        F32(dd as f32)
    }

    #[inline]
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx512vpopcntdq,avx512bw,avx512f,bmi2")]
    unsafe fn sl2_avx512vpopcntdq(lhs: &[usize], rhs: &[usize]) -> F32 {
        use std::arch::x86_64::*;
        #[inline]
        #[target_feature(enable = "avx512vpopcntdq,avx512bw,avx512f,bmi2")]
        pub unsafe fn _mm512_maskz_loadu_epi64(k: __mmask8, mem_addr: *const i8) -> __m512i {
            let mut dst: __m512i;
            unsafe {
                std::arch::asm!(
                    "vmovdqu64 {dst}{{{k}}} {{z}}, [{p}]",
                    p = in(reg) mem_addr,
                    k = in(kreg) k,
                    dst = out(zmm_reg) dst,
                    options(pure, readonly, nostack)
                );
            }
            dst
        }
        assert_eq!(lhs.len(), rhs.len());
        unsafe {
            const WIDTH: usize = 512 / 8 / std::mem::size_of::<usize>();
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

    #[cfg(target_arch = "x86_64")]
    if detect::x86_64::detect_avx512vpopcntdq() {
        unsafe {
            return sl2_avx512vpopcntdq(lhs, rhs);
        }
    }
    sl2(lhs, rhs)
}

#[inline(always)]
pub fn jaccard<'a>(lhs: BVecf32Borrowed<'a>, rhs: BVecf32Borrowed<'a>) -> F32 {
    let lhs = lhs.data();
    let rhs = rhs.data();
    assert!(lhs.len() == rhs.len());

    #[inline(always)]
    #[multiversion::multiversion(targets(
        "x86_64/x86-64-v4",
        "x86_64/x86-64-v3",
        "x86_64/x86-64-v2",
        "aarch64+neon"
    ))]
    fn jaccard(lhs: &[usize], rhs: &[usize]) -> F32 {
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
    #[target_feature(enable = "avx512vpopcntdq,avx512bw,avx512f,bmi2")]
    unsafe fn jaccard_avx512vpopcntdq(lhs: &[usize], rhs: &[usize]) -> F32 {
        use std::arch::x86_64::*;
        #[inline]
        #[target_feature(enable = "avx512vpopcntdq,avx512bw,avx512f,bmi2")]
        pub unsafe fn _mm512_maskz_loadu_epi64(k: __mmask8, mem_addr: *const i8) -> __m512i {
            let mut dst: __m512i;
            unsafe {
                std::arch::asm!(
                    "vmovdqu64 {dst}{{{k}}} {{z}}, [{p}]",
                    p = in(reg) mem_addr,
                    k = in(kreg) k,
                    dst = out(zmm_reg) dst,
                    options(pure, readonly, nostack)
                );
            }
            dst
        }
        assert_eq!(lhs.len(), rhs.len());
        unsafe {
            const WIDTH: usize = 512 / 8 / std::mem::size_of::<usize>();
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

    #[cfg(target_arch = "x86_64")]
    if detect::x86_64::detect_avx512vpopcntdq() {
        unsafe {
            return jaccard_avx512vpopcntdq(lhs, rhs);
        }
    }
    jaccard(lhs, rhs)
}

#[inline(always)]
pub fn length(vector: BVecf32Borrowed<'_>) -> F32 {
    let vector = vector.data();

    #[inline(always)]
    #[multiversion::multiversion(targets(
        "x86_64/x86-64-v4",
        "x86_64/x86-64-v3",
        "x86_64/x86-64-v2",
        "aarch64+neon"
    ))]
    pub fn length(vector: &[usize]) -> F32 {
        let mut l = 0;
        for i in 0..vector.len() {
            l += vector[i].count_ones();
        }
        F32(l as f32).sqrt()
    }

    #[inline]
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx512vpopcntdq,avx512bw,avx512f,bmi2")]
    unsafe fn length_avx512vpopcntdq(lhs: &[usize]) -> F32 {
        use std::arch::x86_64::*;
        #[inline]
        #[target_feature(enable = "avx512vpopcntdq,avx512bw,avx512f,bmi2")]
        pub unsafe fn _mm512_maskz_loadu_epi64(k: __mmask8, mem_addr: *const i8) -> __m512i {
            let mut dst: __m512i;
            unsafe {
                std::arch::asm!(
                    "vmovdqu64 {dst}{{{k}}} {{z}}, [{p}]",
                    p = in(reg) mem_addr,
                    k = in(kreg) k,
                    dst = out(zmm_reg) dst,
                    options(pure, readonly, nostack)
                );
            }
            dst
        }
        unsafe {
            const WIDTH: usize = 512 / 8 / std::mem::size_of::<usize>();
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

    #[cfg(target_arch = "x86_64")]
    if detect::x86_64::detect_avx512vpopcntdq() {
        unsafe {
            return length_avx512vpopcntdq(vector);
        }
    }
    length(vector)
}

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
pub fn l2_normalize<'a>(vector: BVecf32Borrowed<'a>) -> Vecf32Owned {
    let l = length(vector);
    Vecf32Owned::new(vector.iter().map(|i| F32(i as u32 as f32) / l).collect())
}
