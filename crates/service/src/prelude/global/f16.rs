use crate::prelude::*;
use crate::utils::detect;

pub fn cosine(lhs: &[F16], rhs: &[F16]) -> F32 {
    #[inline(always)]
    #[multiversion::multiversion(targets(
        "x86_64/x86-64-v4",
        "x86_64/x86-64-v3",
        "x86_64/x86-64-v2",
        "aarch64+neon"
    ))]
    pub fn cosine(lhs: &[F16], rhs: &[F16]) -> F32 {
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
        xy / (x2 * y2).sqrt()
    }
    #[cfg(target_arch = "x86_64")]
    if self::detect::detect_avx512fp16() {
        assert!(lhs.len() == rhs.len());
        let n = lhs.len();
        unsafe {
            return c::v_f16_cosine_avx512fp16(lhs.as_ptr().cast(), rhs.as_ptr().cast(), n).into();
        }
    }
    #[cfg(target_arch = "x86_64")]
    if self::detect::detect_v3() {
        assert!(lhs.len() == rhs.len());
        let n = lhs.len();
        unsafe {
            return c::v_f16_cosine_v3(lhs.as_ptr().cast(), rhs.as_ptr().cast(), n).into();
        }
    }
    cosine(lhs, rhs)
}

pub fn dot(lhs: &[F16], rhs: &[F16]) -> F32 {
    #[inline(always)]
    #[multiversion::multiversion(targets(
        "x86_64/x86-64-v4",
        "x86_64/x86-64-v3",
        "x86_64/x86-64-v2",
        "aarch64+neon"
    ))]
    pub fn dot(lhs: &[F16], rhs: &[F16]) -> F32 {
        assert!(lhs.len() == rhs.len());
        let n = lhs.len();
        let mut xy = F32::zero();
        for i in 0..n {
            xy += lhs[i].to_f() * rhs[i].to_f();
        }
        xy
    }
    #[cfg(target_arch = "x86_64")]
    if self::detect::detect_avx512fp16() {
        assert!(lhs.len() == rhs.len());
        let n = lhs.len();
        unsafe {
            return c::v_f16_dot_avx512fp16(lhs.as_ptr().cast(), rhs.as_ptr().cast(), n).into();
        }
    }
    #[cfg(target_arch = "x86_64")]
    if self::detect::detect_v3() {
        assert!(lhs.len() == rhs.len());
        let n = lhs.len();
        unsafe {
            return c::v_f16_dot_v3(lhs.as_ptr().cast(), rhs.as_ptr().cast(), n).into();
        }
    }
    cosine(lhs, rhs)
}

pub fn sl2(lhs: &[F16], rhs: &[F16]) -> F32 {
    #[inline(always)]
    #[multiversion::multiversion(targets(
        "x86_64/x86-64-v4",
        "x86_64/x86-64-v3",
        "x86_64/x86-64-v2",
        "aarch64+neon"
    ))]
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
    #[cfg(target_arch = "x86_64")]
    if self::detect::detect_avx512fp16() {
        assert!(lhs.len() == rhs.len());
        let n = lhs.len();
        unsafe {
            return c::v_f16_sl2_avx512fp16(lhs.as_ptr().cast(), rhs.as_ptr().cast(), n).into();
        }
    }
    #[cfg(target_arch = "x86_64")]
    if self::detect::detect_v3() {
        assert!(lhs.len() == rhs.len());
        let n = lhs.len();
        unsafe {
            return c::v_f16_sl2_v3(lhs.as_ptr().cast(), rhs.as_ptr().cast(), n).into();
        }
    }
    sl2(lhs, rhs)
}
