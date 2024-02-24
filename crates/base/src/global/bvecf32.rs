use crate::scalar::*;
use crate::vector::*;
use num_traits::{Float, Zero};

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
    pub fn cosine(lhs: &[usize], rhs: &[usize]) -> F32 {
        let mut xy = F32::zero();
        let mut x2 = F32::zero();
        let mut y2 = F32::zero();
        for i in 0..lhs.len() {
            xy += (lhs[i] & rhs[i]).count_ones() as f32;
            x2 += lhs[i].count_ones() as f32;
            y2 += rhs[i].count_ones() as f32;
        }
        xy / (x2 * y2).sqrt()
    }
    #[cfg(target_arch = "x86_64")]
    if detect::x86_64::detect_avx512vpopcntdq() {
        unsafe {
            return c::v_binary_cosine_avx512vpopcntdq(lhs.as_ptr(), rhs.as_ptr(), lhs.len())
                .into();
        }
    }
    #[cfg(target_arch = "x86_64")]
    if detect::x86_64::detect_v4() {
        unsafe {
            return c::v_binary_cosine_v4(lhs.as_ptr(), rhs.as_ptr(), lhs.len()).into();
        }
    }
    #[cfg(target_arch = "x86_64")]
    if detect::x86_64::detect_v3() {
        unsafe {
            return c::v_binary_cosine_v3(lhs.as_ptr(), rhs.as_ptr(), lhs.len()).into();
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
    pub fn dot(lhs: &[usize], rhs: &[usize]) -> F32 {
        let mut xy = F32::zero();
        for i in 0..lhs.len() {
            xy += (lhs[i] & rhs[i]).count_ones() as f32;
        }
        xy
    }

    #[cfg(target_arch = "x86_64")]
    if detect::x86_64::detect_avx512vpopcntdq() {
        unsafe {
            return c::v_binary_dot_avx512vpopcntdq(lhs.as_ptr(), rhs.as_ptr(), lhs.len()).into();
        }
    }
    #[cfg(target_arch = "x86_64")]
    if detect::x86_64::detect_v4() {
        unsafe {
            return c::v_binary_dot_v4(lhs.as_ptr(), rhs.as_ptr(), lhs.len()).into();
        }
    }
    #[cfg(target_arch = "x86_64")]
    if detect::x86_64::detect_v3() {
        unsafe {
            return c::v_binary_dot_v3(lhs.as_ptr(), rhs.as_ptr(), lhs.len()).into();
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
    pub fn sl2(lhs: &[usize], rhs: &[usize]) -> F32 {
        let mut xy = F32::zero();
        let mut x2 = F32::zero();
        let mut y2 = F32::zero();
        for i in 0..lhs.len() {
            xy += (lhs[i] ^ rhs[i]).count_ones() as f32;
            x2 += lhs[i].count_ones() as f32;
            y2 += rhs[i].count_ones() as f32;
        }
        F32(1.0) - xy / (x2 * y2).sqrt()
    }

    #[cfg(target_arch = "x86_64")]
    if detect::x86_64::detect_avx512vpopcntdq() {
        unsafe {
            return c::v_binary_sl2_avx512vpopcntdq(lhs.as_ptr(), rhs.as_ptr(), lhs.len()).into();
        }
    }
    #[cfg(target_arch = "x86_64")]
    if detect::x86_64::detect_v4() {
        unsafe {
            return c::v_binary_sl2_v4(lhs.as_ptr(), rhs.as_ptr(), lhs.len()).into();
        }
    }
    #[cfg(target_arch = "x86_64")]
    if detect::x86_64::detect_v3() {
        unsafe {
            return c::v_binary_sl2_v3(lhs.as_ptr(), rhs.as_ptr(), lhs.len()).into();
        }
    }
    sl2(lhs, rhs)
}

#[inline(always)]
pub fn length<'a>(vector: BVecf32Borrowed<'a>) -> F32 {
    let vector = vector.data();

    #[inline(always)]
    #[multiversion::multiversion(targets(
        "x86_64/x86-64-v4",
        "x86_64/x86-64-v3",
        "x86_64/x86-64-v2",
        "aarch64+neon"
    ))]
    pub fn length(vector: &[usize]) -> F32 {
        let mut l = F32::zero();
        for i in 0..vector.len() {
            l += vector[i].count_ones() as f32;
        }
        l.sqrt()
    }
    #[cfg(target_arch = "x86_64")]
    if detect::x86_64::detect_avx512vpopcntdq() {
        unsafe {
            return c::v_binary_cnt_avx512vpopcntdq(vector.as_ptr(), vector.len())
                .sqrt()
                .into();
        }
    }
    #[cfg(target_arch = "x86_64")]
    if detect::x86_64::detect_v4() {
        unsafe {
            return c::v_binary_cnt_v4(vector.as_ptr(), vector.len())
                .sqrt()
                .into();
        }
    }
    #[cfg(target_arch = "x86_64")]
    if detect::x86_64::detect_v3() {
        unsafe {
            return c::v_binary_cnt_v3(vector.as_ptr(), vector.len())
                .sqrt()
                .into();
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
