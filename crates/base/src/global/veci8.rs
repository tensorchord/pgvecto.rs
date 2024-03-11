use crate::scalar::{F32, I8};

use super::Veci8Borrowed;

pub fn dot(x: &[I8], y: &[I8]) -> F32 {
    #[cfg(target_arch = "x86_64")]
    {
        if detect::x86_64::test_avx512vnni() {
            return unsafe { dot_i8_avx512vnni(x, y) };
        }
    }
    dot_i8_fallback(x, y)
}

#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
fn dot_i8_fallback(x: &[I8], y: &[I8]) -> F32 {
    // i8 * i8 fall in range of i16. Since our length is less than (2^16 - 1), the result won't overflow.
    let mut sum = 0;
    assert_eq!(x.len(), y.len());
    let length = x.len();
    // according to https://godbolt.org/z/ff48vW4es, this loop will be autovectorized
    for i in 0..length {
        sum += (x[i].0 as i16 * y[i].0 as i16) as i32;
    }
    F32(sum as f32)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512vnni,avx512bw,avx512f,bmi2")]
unsafe fn dot_i8_avx512vnni(x: &[I8], y: &[I8]) -> F32 {
    use std::arch::x86_64::*;
    #[inline]
    #[target_feature(enable = "avx512vnni,avx512bw,avx512f,bmi2")]
    pub unsafe fn _mm512_maskz_loadu_epi8(k: __mmask64, mem_addr: *const i8) -> __m512i {
        let mut dst: __m512i;
        unsafe {
            std::arch::asm!(
                "vmovdqu8 {dst}{{{k}}} {{z}}, [{p}]",
                p = in(reg) mem_addr,
                k = in(kreg) k,
                dst = out(zmm_reg) dst,
                options(pure, readonly, nostack)
            );
        }
        dst
    }
    assert_eq!(x.len(), y.len());
    let mut sum = 0;
    let mut i = x.len();
    let mut p_x = x.as_ptr() as *const i8;
    let mut p_y = y.as_ptr() as *const i8;
    let mut vec_x;
    let mut vec_y;
    unsafe {
        let mut result = _mm512_setzero_si512();
        let zero = _mm512_setzero_si512();
        while i > 0 {
            if i < 64 {
                let mask = _bzhi_u64(0xFFFF_FFFF_FFFF_FFFF, i as u32);
                vec_x = _mm512_maskz_loadu_epi8(mask, p_x);
                vec_y = _mm512_maskz_loadu_epi8(mask, p_y);
                i = 0;
            } else {
                vec_x = _mm512_loadu_epi8(p_x);
                vec_y = _mm512_loadu_epi8(p_y);
                i -= 64;
                p_x = p_x.add(64);
                p_y = p_y.add(64);
            }
            // There are only _mm512_dpbusd_epi32 support, dpbusd will zeroextend a[i] and signextend b[i] first, so we need to convert a[i] positive and change corresponding b[i] to get right result.
            // And because we use -b[i] here, the range of quantization should be [-127, 127] instead of [-128, 127] to avoid overflow.
            let neg_mask = _mm512_movepi8_mask(vec_x);
            vec_x = _mm512_mask_abs_epi8(vec_x, neg_mask, vec_x);
            // Get -b[i] here, use saturating sub to avoid overflow. There are some precision loss here.
            vec_y = _mm512_mask_subs_epi8(vec_y, neg_mask, zero, vec_y);
            result = _mm512_dpbusd_epi32(result, vec_x, vec_y);
        }
        sum += _mm512_reduce_add_epi32(result);
    }
    F32(sum as f32)
}

pub fn dot_distance(x: &Veci8Borrowed<'_>, y: &Veci8Borrowed<'_>) -> F32 {
    // (alpha_x * x[i] + offset_x) * (alpha_y * y[i] + offset_y)
    // = alpha_x * alpha_y * x[i] * y[i] + alpha_x * offset_y * x[i] + alpha_y * offset_x * y[i] + offset_x * offset_y
    // Sum(dot(origin_x[i] , origin_y[i])) = alpha_x * alpha_y * Sum(dot(x[i], y[i])) + offset_y * Sum(alpha_x * x[i]) + offset_x * Sum(alpha_y * y[i]) + offset_x * offset_y * dims
    let dot_xy = dot(x.data(), y.data());
    x.alpha() * y.alpha() * dot_xy
        + x.offset() * y.sum()
        + y.offset() * x.sum()
        + x.offset() * y.offset() * F32(x.dims() as f32)
}

pub fn l2_distance(x: &Veci8Borrowed<'_>, y: &Veci8Borrowed<'_>) -> F32 {
    // Sum(l2(origin_x[i] - origin_y[i])) = sum(x[i] ^ 2 - 2 * x[i] * y[i] + y[i] ^ 2)
    // = dot(x, x) - 2 * dot(x, y) + dot(y, y)
    x.l2_norm() * x.l2_norm() - F32(2.0) * dot_distance(x, y) + y.l2_norm() * y.l2_norm()
}

pub fn cosine_distance(x: &Veci8Borrowed<'_>, y: &Veci8Borrowed<'_>) -> F32 {
    // dot(x, y) / (l2(x) * l2(y))
    let dot_xy = dot_distance(x, y);
    let l2_x = x.l2_norm();
    let l2_y = y.l2_norm();
    dot_xy / (l2_x * l2_y)
}

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
pub fn l2_2<'a>(lhs: Veci8Borrowed<'a>, rhs: &[F32]) -> F32 {
    let data = lhs.data();
    assert_eq!(data.len(), rhs.len());
    data.iter()
        .zip(rhs.iter())
        .map(|(&x, &y)| {
            (x.to_f32() * lhs.alpha() + lhs.offset() - y)
                * (x.to_f32() * lhs.alpha() + lhs.offset() - y)
        })
        .sum::<F32>()
}

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
pub fn dot_2<'a>(lhs: Veci8Borrowed<'a>, rhs: &[F32]) -> F32 {
    let data = lhs.data();
    assert_eq!(data.len(), rhs.len());
    data.iter()
        .zip(rhs.iter())
        .map(|(&x, &y)| (x.to_f32() * lhs.alpha() + lhs.offset()) * y)
        .sum::<F32>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        global::{Veci8Owned, VectorOwned},
        vector::i8_quantization,
    };

    fn new_random_vec_f32(size: usize) -> Vec<F32> {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        (0..size)
            .map(|_| F32(rng.gen_range(-100000.0..100000.0)))
            .collect()
    }

    fn vec_to_owned(vec: Vec<F32>) -> Veci8Owned {
        let (v, alpha, offset) = i8_quantization(&vec);
        Veci8Owned::new(v.len() as u32, v, alpha, offset)
    }

    #[test]
    fn test_dot_i8() {
        let x = vec![F32(1.0), F32(2.0), F32(3.0)];
        let y = vec![F32(3.0), F32(2.0), F32(1.0)];
        let x_owned = vec_to_owned(x);
        let ref_x = x_owned.for_borrow();
        let y_owned = vec_to_owned(y);
        let ref_y = y_owned.for_borrow();
        let result = dot_distance(&ref_x, &ref_y);
        assert!((result.0 - 10.0).abs() < 0.1);
    }

    #[test]
    fn test_cos_i8() {
        let x = vec![F32(1.0), F32(2.0), F32(3.0)];
        let y = vec![F32(3.0), F32(2.0), F32(1.0)];
        let x_owned = vec_to_owned(x);
        let ref_x = x_owned.for_borrow();
        let y_owned = vec_to_owned(y);
        let ref_y = y_owned.for_borrow();
        let result = cosine_distance(&ref_x, &ref_y);
        assert!((result.0 - (10.0 / 14.0)).abs() < 0.1);
        // test cos_i8 using random generated data, check the precision
        let x = new_random_vec_f32(1000);
        let y = new_random_vec_f32(1000);
        let xy = x.iter().zip(y.iter()).map(|(&x, &y)| x * y).sum::<F32>().0;
        let l2_x = x.iter().map(|&x| x * x).sum::<F32>().0.sqrt();
        let l2_y = y.iter().map(|&y| y * y).sum::<F32>().0.sqrt();
        let result_expected = xy / (l2_x * l2_y);
        let x_owned = vec_to_owned(x);
        let ref_x = x_owned.for_borrow();
        let y_owned = vec_to_owned(y);
        let ref_y = y_owned.for_borrow();
        let result = cosine_distance(&ref_x, &ref_y);
        assert!((result.0 - result_expected).abs() / result_expected < 0.25);
    }

    #[test]
    fn test_l2_i8() {
        let x = vec![F32(1.0), F32(2.0), F32(3.0)];
        let y = vec![F32(3.0), F32(2.0), F32(1.0)];
        let x_owned = vec_to_owned(x);
        let ref_x = x_owned.for_borrow();
        let y_owned = vec_to_owned(y);
        let ref_y = y_owned.for_borrow();
        let result = l2_distance(&ref_x, &ref_y);
        assert!((result.0 - 8.0).abs() < 0.1);
        // test l2_i8 using random generated data, check the precision
        let x = new_random_vec_f32(1000);
        let y = new_random_vec_f32(1000);
        let result_expected = x
            .iter()
            .zip(y.iter())
            .map(|(&x, &y)| (x - y) * (x - y))
            .sum::<F32>()
            .0;
        let x_owned = vec_to_owned(x);
        let ref_x = x_owned.for_borrow();
        let y_owned = vec_to_owned(y);
        let ref_y = y_owned.for_borrow();
        let result = l2_distance(&ref_x, &ref_y);
        assert!((result.0 - result_expected).abs() / result_expected < 0.05);
    }
}
