use base::scalar::*;

mod mul_add {
    #[cfg(target_arch = "x86_64")]
    #[detect::target_cpu(enable = "v4")]
    unsafe fn mul_add_v4(this: &[f32], k: f32, b: f32) -> Vec<u8> {
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

    #[cfg(all(target_arch = "x86_64", test))]
    #[test]
    fn mul_add_v4_test() {
        detect::init();
        if !detect::v4::detect() {
            println!("test {} ... skipped (v4)", module_path!());
            return;
        }
        for _ in 0..300 {
            let n = 4010;
            let x = (0..n).map(|_| rand::random::<_>()).collect::<Vec<_>>();
            for z in 3990..4010 {
                let x = &x[..z];
                let k = 20.0;
                let b = 20.0;
                let specialized = unsafe { mul_add_v4(x, k, b) };
                let fallback = unsafe { mul_add_fallback(x, k, b) };
                assert_eq!(specialized, fallback);
            }
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[detect::target_cpu(enable = "v3")]
    unsafe fn mul_add_v3(this: &[f32], k: f32, b: f32) -> Vec<u8> {
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
    fn mul_add_v3_test() {
        detect::init();
        if !detect::v3::detect() {
            println!("test {} ... skipped (v3)", module_path!());
            return;
        }
        for _ in 0..300 {
            let n = 4010;
            let x = (0..n).map(|_| rand::random::<_>()).collect::<Vec<_>>();
            for z in 3990..4010 {
                let x = &x[..z];
                let k = 20.0;
                let b = 20.0;
                let specialized = unsafe { mul_add_v3(x, k, b) };
                let fallback = unsafe { mul_add_fallback(x, k, b) };
                assert_eq!(specialized, fallback);
            }
        }
    }

    #[detect::multiversion(v4 = import, v3 = import, v2, neon, fallback = export)]
    pub fn mul_add(this: &[f32], k: f32, b: f32) -> Vec<u8> {
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
pub fn quantize<const N: u8>(lut: &[f32]) -> (f32, f32, Vec<u8>) {
    let (min, max) = f32::reduce_min_max_of_x(lut);
    let k = 0.0f32.max((max - min) / (N as f32));
    let b = min;
    (k, b, mul_add::mul_add(lut, 1.0 / k, -b / k))
}

#[inline(always)]
pub fn dequantize(sum_1: u32, k: f32, b: f32, sum_x: u16) -> f32 {
    (sum_1 as f32) * b + (sum_x as f32) * k
}
