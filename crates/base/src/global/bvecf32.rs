use crate::scalar::*;
use crate::vector::*;
use num_traits::Float;

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

    #[inline(always)]
    #[cfg(target_arch = "x86_64")]
    unsafe fn cosine_avx(lhs: &[usize], rhs: &[usize]) -> F32 {
        unsafe {
            use std::arch::x86_64::*;
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
            return cosine_avx(lhs, rhs);
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

    #[inline(always)]
    #[cfg(target_arch = "x86_64")]
    unsafe fn dot_avx(lhs: &[usize], rhs: &[usize]) -> F32 {
        unsafe {
            use std::arch::x86_64::*;
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
            return dot_avx(lhs, rhs);
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

    #[inline(always)]
    #[cfg(target_arch = "x86_64")]
    unsafe fn sl2_avx(lhs: &[usize], rhs: &[usize]) -> F32 {
        unsafe {
            use std::arch::x86_64::*;
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
            return sl2_avx(lhs, rhs);
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

    #[inline(always)]
    #[cfg(target_arch = "x86_64")]
    unsafe fn jaccard_avx(lhs: &[usize], rhs: &[usize]) -> F32 {
        unsafe {
            use std::arch::x86_64::*;
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
            return jaccard_avx(lhs, rhs);
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

    #[inline(always)]
    #[cfg(target_arch = "x86_64")]
    unsafe fn length_avx(lhs: &[usize]) -> F32 {
        unsafe {
            use std::arch::x86_64::*;
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
                let mask = _bzhi_u32(0xFFFF, n as u32).try_into().unwrap();
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
            return length_avx(vector);
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
