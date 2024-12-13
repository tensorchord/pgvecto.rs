use crate::simd::*;

mod mul_add_round {
    #[inline]
    #[cfg(target_arch = "x86_64")]
    #[crate::simd::target_cpu(enable = "v4")]
    fn mul_add_round_v4(this: &[f32], k: f32, b: f32) -> Vec<u8> {
        let n = this.len();
        let mut r = Vec::<u8>::with_capacity(n);
        unsafe {
            use std::arch::x86_64::*;
            let lk = _mm512_set1_ps(k);
            let lb = _mm512_set1_ps(b);
            let mut n = n;
            let mut a = this.as_ptr();
            let mut r = r.as_mut_ptr();
            while n >= 16 {
                let x = _mm512_loadu_ps(a);
                let v =
                    _mm512_fmadd_round_ps(x, lk, lb, _MM_FROUND_TO_NEAREST_INT | _MM_FROUND_NO_EXC);
                let v = _mm512_cvtps_epi32(v);
                let vfl = _mm512_cvtepi32_epi8(v);
                _mm_storeu_si128(r.cast(), vfl);
                n -= 16;
                a = a.add(16);
                r = r.add(16);
            }
            if n > 0 {
                let mask = _bzhi_u32(0xffff, n as u32) as u16;
                let x = _mm512_maskz_loadu_ps(mask, a);
                let v =
                    _mm512_fmadd_round_ps(x, lk, lb, _MM_FROUND_TO_NEAREST_INT | _MM_FROUND_NO_EXC);
                let v = _mm512_cvtps_epi32(v);
                let vfl = _mm512_cvtepi32_epi8(v);
                _mm_mask_storeu_epi8(r.cast(), mask, vfl);
            }
        }
        unsafe {
            r.set_len(n);
        }
        r
    }

    #[cfg(all(target_arch = "x86_64", test, not(miri)))]
    #[test]
    fn mul_add_round_v4_test() {
        if !crate::simd::is_cpu_detected!("v4") {
            println!("test {} ... skipped (v4)", module_path!());
            return;
        }
        for _ in 0..if cfg!(not(miri)) { 256 } else { 1 } {
            let n = 4010;
            let x = (0..n).map(|_| rand::random::<_>()).collect::<Vec<_>>();
            for z in 3990..4010 {
                let x = &x[..z];
                let k = 20.0;
                let b = 20.0;
                let specialized = unsafe { mul_add_round_v4(x, k, b) };
                let fallback = mul_add_round_fallback(x, k, b);
                assert_eq!(specialized, fallback);
            }
        }
    }

    #[inline]
    #[cfg(target_arch = "x86_64")]
    #[crate::simd::target_cpu(enable = "v3")]
    fn mul_add_round_v3(this: &[f32], k: f32, b: f32) -> Vec<u8> {
        let n = this.len();
        let mut r = Vec::<u8>::with_capacity(n);
        unsafe {
            use std::arch::x86_64::*;
            let cons = _mm256_setr_epi8(
                0, 4, 8, 12, -1, -1, -1, -1, // 0..8
                -1, -1, -1, -1, -1, -1, -1, -1, // 8..15
                0, 4, 8, 12, -1, -1, -1, -1, // 16..24
                -1, -1, -1, -1, -1, -1, -1, -1, // 24..32
            );
            let lk = _mm256_set1_ps(k);
            let lb = _mm256_set1_ps(b);
            let mut n = n;
            let mut a = this.as_ptr();
            let mut r = r.as_mut_ptr();
            while n >= 8 {
                let x = _mm256_loadu_ps(a);
                let v = _mm256_fmadd_ps(x, lk, lb);
                let v = _mm256_cvtps_epi32(_mm256_round_ps(v, 0x00));
                let vs = _mm256_shuffle_epi8(v, cons);
                let vlo = _mm256_extract_epi32::<0>(vs) as u32;
                let vhi = _mm256_extract_epi32::<4>(vs) as u32;
                let vfl = vlo as u64 | ((vhi as u64) << 32);
                r.cast::<u64>().write_unaligned(vfl);
                n -= 8;
                a = a.add(8);
                r = r.add(8);
            }
            // this hint is used to disable loop unrolling
            while std::hint::black_box(n) > 0 {
                let x = a.read();
                let v = x.mul_add(k, b).round_ties_even() as u8;
                r.write(v);
                n -= 1;
                a = a.add(1);
                r = r.add(1);
            }
        }
        unsafe {
            r.set_len(n);
        }
        r
    }

    #[cfg(all(target_arch = "x86_64", test))]
    #[test]
    fn mul_add_round_v3_test() {
        if !crate::simd::is_cpu_detected!("v3") {
            println!("test {} ... skipped (v3)", module_path!());
            return;
        }
        for _ in 0..if cfg!(not(miri)) { 256 } else { 1 } {
            let n = 4010;
            let x = (0..n).map(|_| rand::random::<_>()).collect::<Vec<_>>();
            for z in 3990..4010 {
                let x = &x[..z];
                let k = 20.0;
                let b = 20.0;
                let specialized = unsafe { mul_add_round_v3(x, k, b) };
                let fallback = mul_add_round_fallback(x, k, b);
                assert_eq!(specialized, fallback);
            }
        }
    }

    #[inline]
    #[cfg(target_arch = "x86_64")]
    #[crate::simd::target_cpu(enable = "v2")]
    #[target_feature(enable = "fma")]
    fn mul_add_round_v2_fma(this: &[f32], k: f32, b: f32) -> Vec<u8> {
        let n = this.len();
        let mut r = Vec::<u8>::with_capacity(n);
        unsafe {
            use std::arch::x86_64::*;
            let cons = _mm_setr_epi8(
                0, 4, 8, 12, -1, -1, -1, -1, // 0..8
                -1, -1, -1, -1, -1, -1, -1, -1, // 8..15
            );
            let lk = _mm_set1_ps(k);
            let lb = _mm_set1_ps(b);
            let mut n = n;
            let mut a = this.as_ptr();
            let mut r = r.as_mut_ptr();
            while n >= 4 {
                let x = _mm_loadu_ps(a);
                let v = _mm_fmadd_ps(x, lk, lb);
                let v = _mm_cvtps_epi32(_mm_round_ps(v, 0x00));
                let vs = _mm_shuffle_epi8(v, cons);
                let vfl = _mm_extract_epi32::<0>(vs) as u32;
                r.cast::<u32>().write_unaligned(vfl);
                n -= 4;
                a = a.add(4);
                r = r.add(4);
            }
            // this hint is used to disable loop unrolling
            while std::hint::black_box(n) > 0 {
                let x = a.read();
                let v = x.mul_add(k, b).round_ties_even() as u8;
                r.write(v);
                n -= 1;
                a = a.add(1);
                r = r.add(1);
            }
        }
        unsafe {
            r.set_len(n);
        }
        r
    }

    #[cfg(all(target_arch = "x86_64", test))]
    #[test]
    fn mul_add_round_v2_fma_test() {
        if !crate::simd::is_cpu_detected!("v2") || !crate::simd::is_feature_detected!("fma") {
            println!("test {} ... skipped (v2:fma)", module_path!());
            return;
        }
        for _ in 0..if cfg!(not(miri)) { 256 } else { 1 } {
            let n = 4010;
            let x = (0..n).map(|_| rand::random::<_>()).collect::<Vec<_>>();
            for z in 3990..4010 {
                let x = &x[..z];
                let k = 20.0;
                let b = 20.0;
                let specialized = unsafe { mul_add_round_v2_fma(x, k, b) };
                let fallback = mul_add_round_fallback(x, k, b);
                assert_eq!(specialized, fallback);
            }
        }
    }

    #[inline]
    #[cfg(target_arch = "aarch64")]
    #[crate::simd::target_cpu(enable = "v8.3a")]
    fn mul_add_round_v8_3a(this: &[f32], k: f32, b: f32) -> Vec<u8> {
        let n = this.len();
        let mut r = Vec::<u8>::with_capacity(n);
        unsafe {
            use std::arch::aarch64::*;
            let cons = vld1q_u8(
                [
                    0, 4, 8, 12, 0xff, 0xff, 0xff, 0xff, // 0..8
                    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // 8..15
                ]
                .as_ptr(),
            );
            let lk = vdupq_n_f32(k);
            let lb = vdupq_n_f32(b);
            let mut n = n;
            let mut a = this.as_ptr();
            let mut r = r.as_mut_ptr();
            while n >= 4 {
                let x = vld1q_f32(a);
                let v = vfmaq_f32(lb, x, lk);
                let v = vcvtnq_u32_f32(v);
                let vs = vqtbl1q_u8(vreinterpretq_u8_u32(v), cons);
                let vfl = vgetq_lane_u32::<0>(vreinterpretq_u32_u8(vs));
                r.cast::<u32>().write_unaligned(vfl);
                n -= 4;
                a = a.add(4);
                r = r.add(4);
            }
            // this hint is used to disable loop unrolling
            while std::hint::black_box(n) > 0 {
                let x = a.read();
                let v = x.mul_add(k, b).round_ties_even() as u8;
                r.write(v);
                n -= 1;
                a = a.add(1);
                r = r.add(1);
            }
        }
        unsafe {
            r.set_len(n);
        }
        r
    }

    #[cfg(all(target_arch = "aarch64", test, not(miri)))]
    #[test]
    fn mul_add_round_v8_3a_test() {
        if !crate::simd::is_cpu_detected!("v8.3a") {
            println!("test {} ... skipped (v8.3a)", module_path!());
            return;
        }
        for _ in 0..if cfg!(not(miri)) { 256 } else { 1 } {
            let n = 4010;
            let x = (0..n).map(|_| rand::random::<_>()).collect::<Vec<_>>();
            for z in 3990..4010 {
                let x = &x[..z];
                let k = 20.0;
                let b = 20.0;
                let specialized = unsafe { mul_add_round_v8_3a(x, k, b) };
                let fallback = mul_add_round_fallback(x, k, b);
                assert_eq!(specialized, fallback);
            }
        }
    }

    #[crate::simd::multiversion(@"v4", @"v3", @"v2:fma", @"v8.3a")]
    pub fn mul_add_round(this: &[f32], k: f32, b: f32) -> Vec<u8> {
        let n = this.len();
        let mut r = Vec::<u8>::with_capacity(n);
        for i in 0..n {
            let x = this[i];
            let v = x.mul_add(k, b).round_ties_even() as u8;
            unsafe {
                r.as_mut_ptr().add(i).write(v);
            }
        }
        unsafe {
            r.set_len(n);
        }
        r
    }
}

#[inline(always)]
pub fn quantize(lut: &[f32], n: f32) -> (f32, f32, Vec<u8>) {
    use crate::simd::ScalarLike;
    let (min, max) = f32::reduce_min_max_of_x(lut);
    let k = 0.0f32.max((max - min) / n);
    let b = min;
    (k, b, mul_add_round::mul_add_round(lut, 1.0 / k, -b / k))
}
