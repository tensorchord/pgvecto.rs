use crate::prelude::*;

pub fn cosine(lhs: &[F16], rhs: &[F16]) -> F32 {
    #[inline(always)]
    #[multiversion::multiversion(targets = "simd")]
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
    if super::avx512fp16::detect() {
        assert!(lhs.len() == rhs.len());
        let n = lhs.len();
        unsafe {
            return c::v_f16_cosine_axv512(lhs.as_ptr().cast(), rhs.as_ptr().cast(), n).into();
        }
    }
    if super::avx2::detect() {
        assert!(lhs.len() == rhs.len());
        let n = lhs.len();
        unsafe {
            return c::v_f16_cosine_axv2(lhs.as_ptr().cast(), rhs.as_ptr().cast(), n).into();
        }
    }
    cosine(lhs, rhs)
}

pub fn dot(lhs: &[F16], rhs: &[F16]) -> F32 {
    #[inline(always)]
    #[multiversion::multiversion(targets = "simd")]
    pub fn dot(lhs: &[F16], rhs: &[F16]) -> F32 {
        assert!(lhs.len() == rhs.len());
        let n = lhs.len();
        let mut xy = F32::zero();
        for i in 0..n {
            xy += lhs[i].to_f() * rhs[i].to_f();
        }
        xy
    }
    if super::avx512fp16::detect() {
        assert!(lhs.len() == rhs.len());
        let n = lhs.len();
        unsafe {
            return c::v_f16_dot_axv512(lhs.as_ptr().cast(), rhs.as_ptr().cast(), n).into();
        }
    }
    if super::avx2::detect() {
        assert!(lhs.len() == rhs.len());
        let n = lhs.len();
        unsafe {
            return c::v_f16_dot_axv2(lhs.as_ptr().cast(), rhs.as_ptr().cast(), n).into();
        }
    }
    cosine(lhs, rhs)
}

pub fn sl2(lhs: &[F16], rhs: &[F16]) -> F32 {
    #[inline(always)]
    #[multiversion::multiversion(targets = "simd")]
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
    if super::avx512fp16::detect() {
        assert!(lhs.len() == rhs.len());
        let n = lhs.len();
        unsafe {
            return c::v_f16_sl2_axv512(lhs.as_ptr().cast(), rhs.as_ptr().cast(), n).into();
        }
    }
    if super::avx2::detect() {
        assert!(lhs.len() == rhs.len());
        let n = lhs.len();
        unsafe {
            return c::v_f16_sl2_axv2(lhs.as_ptr().cast(), rhs.as_ptr().cast(), n).into();
        }
    }
    sl2(lhs, rhs)
}
