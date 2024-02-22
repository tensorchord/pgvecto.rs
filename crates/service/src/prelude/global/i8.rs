use crate::prelude::*;

pub fn dot(x: &[I8], y: &[I8]) -> F32 {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
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

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx512f,avx512bw,avx512vnni,bmi2")]
unsafe fn dot_i8_avx512vnni(x: &[I8], y: &[I8]) -> F32 {
    use std::arch::x86_64::*;

    assert_eq!(x.len(), y.len());
    let mut sum = 0;
    let mut i = x.len();
    let mut p_x = x.as_ptr() as *const i8;
    let mut p_y = y.as_ptr() as *const i8;
    let mut vec_x;
    let mut vec_y;
    unsafe {
        let result = _mm512_setzero_si512();
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
            _mm512_dpbusd_epi32(result, vec_x, vec_y);
        }
        sum += _mm512_reduce_add_epi32(result);
    }
    F32(sum as f32)
}

pub fn dot_distance(x: &VecI8Ref<'_>, y: &VecI8Ref<'_>) -> F32 {
    // (alpha_x * x[i] + offset_x) * (alpha_y * y[i] + offset_y)
    // = alpha_x * alpha_y * x[i] * y[i] + alpha_x * offset_y * x[i] + alpha_y * offset_x * y[i] + offset_x * offset_y
    // Sum(dot(origin_x[i] , origin_y[i])) = alpha_x * alpha_y * Sum(dot(x[i], y[i])) + offset_y * Sum(alpha_x * x[i]) + offset_x * Sum(alpha_y * y[i]) + offset_x * offset_y * dims
    let dot_xy = dot(x.data, y.data);
    x.alpha * y.alpha * dot_xy
        + x.offset * y.sum
        + y.offset * x.sum
        + x.offset * y.offset * F32(x.dims as f32)
}

pub fn l2_distance(x: &VecI8Ref<'_>, y: &VecI8Ref<'_>) -> F32 {
    // Sum(l2(origin_x[i] - origin_y[i])) = sum(x[i] ^ 2 - 2 * x[i] * y[i] + y[i] ^ 2)
    // = dot(x, x) - 2 * dot(x, y) + dot(y, y)
    dot_distance(x, x) - F32(2.0) * dot_distance(x, y) + dot_distance(y, y)
}

pub fn cosine_distance(x: &VecI8Ref<'_>, y: &VecI8Ref<'_>) -> F32 {
    // dot(x, y) / (l2(x) * l2(y))
    let dot_xy = dot_distance(x, y);
    let l2_x = x.l2_norm;
    let l2_y = y.l2_norm;
    dot_xy / (l2_x * l2_y)
}

pub fn quantization(vector: Vec<F32>) -> (Vec<I8>, F32, F32) {
    let min = vector.iter().copied().fold(F32::infinity(), Float::min);
    let max = vector.iter().copied().fold(F32::neg_infinity(), Float::max);
    let alpha = (max - min) / 255.0;
    let offset = (max + min) / 2.0;
    let result = vector
        .iter()
        .map(|&x| ((x - offset) / alpha).into())
        .collect();
    (result, alpha, offset)
}

pub fn dequantization(vector: &[I8], alpha: F32, offset: F32) -> Vec<F32> {
    vector
        .iter()
        .map(|&x| (x.to_f() * alpha + offset))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quantization_roundtrip() {
        let vector = vec![F32(0.0), F32(1.0), F32(2.0), F32(3.0), F32(4.0)];
        let (result, alpha, offset) = quantization(vector);
        assert_eq!(result, vec![I8(-127), I8(-63), I8(0), I8(63), I8(127)]);
        assert_eq!(alpha, F32(4.0 / 255.0));
        assert_eq!(offset, F32(2.0));
        let vector = dequantization(result.as_slice(), alpha, offset);
        for (i, x) in vector.iter().enumerate() {
            assert!((x.0 - (i as f32)).abs() < 0.05);
        }
    }

    #[test]
    fn test_dot_i8() {
        let x = vec![F32(1.0), F32(2.0), F32(3.0)];
        let y = vec![F32(3.0), F32(2.0), F32(1.0)];
        let (v_x, alpha_x, offset_x) = quantization(x);
        let ref_x = VecI8Ref::new(v_x.len() as u16, &v_x, alpha_x, offset_x);
        let (v_y, alpha_y, offset_y) = quantization(y);
        let ref_y = VecI8Ref::new(v_y.len() as u16, &v_y, alpha_y, offset_y);
        let result = dot_distance(&ref_x, &ref_y);
        assert!((result.0 - 10.0).abs() < 0.1);
    }

    #[test]
    fn test_cos_i8() {
        let x = vec![F32(1.0), F32(2.0), F32(3.0)];
        let y = vec![F32(3.0), F32(2.0), F32(1.0)];
        let (v_x, alpha_x, offset_x) = quantization(x);
        let ref_x = VecI8Ref::new(v_x.len() as u16, &v_x, alpha_x, offset_x);
        let (v_y, alpha_y, offset_y) = quantization(y);
        let ref_y = VecI8Ref::new(v_y.len() as u16, &v_y, alpha_y, offset_y);
        let result = cosine_distance(&ref_x, &ref_y);
        assert!((result.0 - (10.0 / 14.0)).abs() < 0.1);
    }

    #[test]
    fn test_l2_i8() {
        let x = vec![F32(1.0), F32(2.0), F32(3.0)];
        let y = vec![F32(3.0), F32(2.0), F32(1.0)];
        let (v_x, alpha_x, offset_x) = quantization(x);
        let ref_x = VecI8Ref::new(v_x.len() as u16, &v_x, alpha_x, offset_x);
        let (v_y, alpha_y, offset_y) = quantization(y);
        let ref_y = VecI8Ref::new(v_y.len() as u16, &v_y, alpha_y, offset_y);
        let result = l2_distance(&ref_x, &ref_y);
        assert!((result.0 - 8.0).abs() < 0.1);
    }
}
