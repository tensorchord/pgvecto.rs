use base::scalar::F32;
use common::aligned_array::AlignedArray;
use detect::multiversion;
use num_traits::Float;

pub fn quantize_255(lut: &[F32]) -> (F32, F32, Vec<u8>) {
    let min = lut.iter().copied().fold(F32::infinity(), std::cmp::min);
    let max = lut.iter().copied().fold(F32::neg_infinity(), std::cmp::max);
    let k = std::cmp::max(max - min, F32(0.0)) / F32(255.0);
    let b = min;
    (k, b, lut.iter().map(|&y| ((y - b) / k).0 as u8).collect())
}

#[detect::target_cpu(enable = "v3")]
pub unsafe fn quantize_15(lhs: &[F32], rhs: &[F32]) -> (F32, F32, Vec<u8>, u32) {
    assert_eq!(lhs.len(), rhs.len());
    let n = lhs.len();
    let mut min = 1.0e20;
    let mut max = -1.0e20;
    unsafe {
        use std::arch::x86_64::*;
        let mut alpha = _mm256_set1_ps(1.0e20);
        let mut beta = _mm256_set1_ps(-1.0e20);
        for i in 0..n / 8 {
            let x = _mm256_loadu_ps(lhs.as_ptr().cast::<f32>().add(8 * i));
            let y = _mm256_loadu_ps(rhs.as_ptr().cast::<f32>().add(8 * i));
            let z = _mm256_sub_ps(x, y);
            alpha = _mm256_min_ps(alpha, z);
            beta = _mm256_max_ps(beta, z);
        }
        let mut al = AlignedArray::<f32, 8>([0.0; 8]);
        _mm256_store_ps(al.0.as_mut_ptr(), alpha);
        let mut ba = AlignedArray::<f32, 8>([0.0; 8]);
        _mm256_store_ps(ba.0.as_mut_ptr(), beta);
        min = al.0[0]
            .min(al.0[1])
            .min(al.0[2])
            .min(al.0[3])
            .min(al.0[4])
            .min(al.0[5])
            .min(al.0[6])
            .min(al.0[7]);
        max = ba.0[0]
            .max(ba.0[1])
            .max(ba.0[2])
            .max(ba.0[3])
            .max(ba.0[4])
            .max(ba.0[5])
            .max(ba.0[6])
            .max(ba.0[7]);
    }
    let k = (max - min) / 15.0;
    let b = min;
    let k_inv = 1.0 / k;
    let mut result = Vec::<u8>::with_capacity(n);
    let mut qvector_sum = 0_u32;
    /*
    for i in 0..n {
        let y = lhs[i] - rhs[i];
        let val = ((y - b) * k_inv).0 as u8;
        result.push(val);
        qvector_sum += val as u32;
    }
    */
    unsafe {
        use std::arch::x86_64::*;
        let b = _mm256_set1_ps(b);
        let k_inv = _mm256_set1_ps(k_inv);
        let mut sum = _mm256_set1_epi32(0);
        let cons = _mm256_setr_epi8(
            0, 4, 8, 12, -1, -1, -1, -1, //
            -1, -1, -1, -1, -1, -1, -1, -1, //
            0, 4, 8, 12, -1, -1, -1, -1, //
            -1, -1, -1, -1, -1, -1, -1, -1, //
        );
        for i in 0..n / 8 {
            let x = _mm256_loadu_ps(lhs.as_ptr().cast::<f32>().add(8 * i));
            let y = _mm256_loadu_ps(rhs.as_ptr().cast::<f32>().add(8 * i));
            let z = _mm256_sub_ps(x, y);
            let val = _mm256_mul_ps(_mm256_sub_ps(z, b), k_inv);
            let val = _mm256_cvtps_epi32(val);
            let temp = _mm256_shuffle_epi8(val, cons);
            let temp_a = _mm256_extract_epi32::<0>(temp) as u64;
            let temp_b = _mm256_extract_epi32::<4>(temp) as u64;
            result
                .as_mut_ptr()
                .add(8 * i)
                .cast::<i64>()
                .write_unaligned((temp_a | (temp_b << 32)) as i64);
            sum = _mm256_add_epi32(sum, val);
        }
        {
            let mut s = AlignedArray::<i32, 8>([0; 8]);
            _mm256_store_epi32(s.0.as_mut_ptr(), sum);
            qvector_sum =
                (s.0[0] + s.0[1] + s.0[2] + s.0[3] + s.0[4] + s.0[5] + s.0[6] + s.0[7]) as _;
        }
    }
    unsafe {
        result.set_len(n);
    }
    (F32(k), F32(b), result, qvector_sum)
}

pub fn dequantize(sum_1: u32, k: F32, b: F32, sum_x: u16) -> F32 {
    F32(sum_1 as f32) * b + F32(sum_x as f32) * k
}
