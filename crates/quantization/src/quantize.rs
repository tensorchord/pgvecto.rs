use base::scalar::*;

mod mul_add_round {
    #[cfg(target_arch = "x86_64")]
    #[detect::target_cpu(enable = "v4")]
    unsafe fn mul_add_round_v4(this: &[f32], k: f32, b: f32) -> Vec<u8> {
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
    fn mul_add_round_v4_test() {
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
                let specialized = unsafe { mul_add_round_v4(x, k, b) };
                let fallback = unsafe { mul_add_round_fallback(x, k, b) };
                assert_eq!(specialized, fallback);
            }
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[detect::target_cpu(enable = "v3")]
    unsafe fn mul_add_round_v3(this: &[f32], k: f32, b: f32) -> Vec<u8> {
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
                let specialized = unsafe { mul_add_round_v3(x, k, b) };
                let fallback = unsafe { mul_add_round_fallback(x, k, b) };
                assert_eq!(specialized, fallback);
            }
        }
    }

    #[detect::multiversion(v4 = import, v3 = import, v2, neon, fallback = export)]
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

mod reduce_sum_of_x_as_u16 {
    #[cfg(target_arch = "x86_64")]
    #[detect::target_cpu(enable = "v4")]
    unsafe fn reduce_sum_of_x_as_u16_v4(this: &[u8]) -> u16 {
        use base::scalar::emulate::emulate_mm512_reduce_add_epi16;
        unsafe {
            use std::arch::x86_64::*;
            let us = _mm512_set1_epi16(255);
            let mut n = this.len();
            let mut a = this.as_ptr();
            let mut sum = _mm512_setzero_si512();
            while n >= 32 {
                let x = _mm256_loadu_si256(a.cast());
                a = a.add(32);
                n -= 32;
                sum = _mm512_add_epi16(_mm512_and_si512(us, _mm512_cvtepi8_epi16(x)), sum);
            }
            if n > 0 {
                let mask = _bzhi_u32(0xffffffff, n as u32);
                let x = _mm256_maskz_loadu_epi8(mask, a.cast());
                sum = _mm512_add_epi16(_mm512_and_si512(us, _mm512_cvtepi8_epi16(x)), sum);
            }
            emulate_mm512_reduce_add_epi16(sum) as u16
        }
    }

    #[cfg(all(target_arch = "x86_64", test))]
    #[test]
    fn reduce_sum_of_x_as_u16_v4_test() {
        use rand::Rng;
        detect::init();
        if !detect::v4::detect() {
            println!("test {} ... skipped (v4)", module_path!());
            return;
        }
        let mut rng = rand::thread_rng();
        for _ in 0..256 {
            let n = 4016;
            let this = (0..n).map(|_| rng.gen_range(0..16)).collect::<Vec<_>>();
            for z in 3984..4016 {
                let this = &this[..z];
                let specialized = unsafe { reduce_sum_of_x_as_u16_v4(this) };
                let fallback = unsafe { reduce_sum_of_x_as_u16_fallback(this) };
                assert_eq!(specialized, fallback);
            }
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[detect::target_cpu(enable = "v3")]
    unsafe fn reduce_sum_of_x_as_u16_v3(this: &[u8]) -> u16 {
        use base::scalar::emulate::emulate_mm256_reduce_add_epi16;
        unsafe {
            use std::arch::x86_64::*;
            let us = _mm256_set1_epi16(255);
            let mut n = this.len();
            let mut a = this.as_ptr();
            let mut sum = _mm256_setzero_si256();
            while n >= 16 {
                let x = _mm_loadu_si128(a.cast());
                a = a.add(16);
                n -= 16;
                sum = _mm256_add_epi16(_mm256_and_si256(us, _mm256_cvtepi8_epi16(x)), sum);
            }
            let mut sum = emulate_mm256_reduce_add_epi16(sum) as u16;
            // this hint is used to disable loop unrolling
            while std::hint::black_box(n) > 0 {
                let x = a.read();
                a = a.add(1);
                n -= 1;
                sum += x as u16;
            }
            sum
        }
    }

    #[cfg(all(target_arch = "x86_64", test))]
    #[test]
    fn reduce_sum_of_x_as_u16_v3_test() {
        use rand::Rng;
        detect::init();
        if !detect::v3::detect() {
            println!("test {} ... skipped (v3)", module_path!());
            return;
        }
        let mut rng = rand::thread_rng();
        for _ in 0..256 {
            let n = 4016;
            let this = (0..n).map(|_| rng.gen_range(0..16)).collect::<Vec<_>>();
            for z in 3984..4016 {
                let this = &this[..z];
                let specialized = unsafe { reduce_sum_of_x_as_u16_v3(this) };
                let fallback = unsafe { reduce_sum_of_x_as_u16_fallback(this) };
                assert_eq!(specialized, fallback);
            }
        }
    }

    #[detect::multiversion(v4 = import, v3 = import, v2, neon, fallback = export)]
    pub fn reduce_sum_of_x_as_u16(this: &[u8]) -> u16 {
        let n = this.len();
        let mut sum = 0;
        for i in 0..n {
            sum += this[i] as u16;
        }
        sum
    }
}

mod reduce_sum_of_x_as_u32 {
    #[cfg(target_arch = "x86_64")]
    #[detect::target_cpu(enable = "v4")]
    unsafe fn reduce_sum_of_x_as_u32_v4(this: &[u8]) -> u32 {
        unsafe {
            use std::arch::x86_64::*;
            let us = _mm512_set1_epi32(255);
            let mut n = this.len();
            let mut a = this.as_ptr();
            let mut sum = _mm512_setzero_si512();
            while n >= 16 {
                let x = _mm_loadu_epi8(a.cast());
                a = a.add(16);
                n -= 16;
                sum = _mm512_add_epi32(_mm512_and_si512(us, _mm512_cvtepi8_epi32(x)), sum);
            }
            if n > 0 {
                let mask = _bzhi_u32(0xffff, n as u32) as u16;
                let x = _mm_maskz_loadu_epi8(mask, a.cast());
                sum = _mm512_add_epi32(_mm512_and_si512(us, _mm512_cvtepi8_epi32(x)), sum);
            }
            _mm512_reduce_add_epi32(sum) as u32
        }
    }

    #[cfg(all(target_arch = "x86_64", test))]
    #[test]
    fn reduce_sum_of_x_as_u32_v4_test() {
        use rand::Rng;
        detect::init();
        if !detect::v4::detect() {
            println!("test {} ... skipped (v4)", module_path!());
            return;
        }
        let mut rng = rand::thread_rng();
        for _ in 0..256 {
            let n = 4016;
            let this = (0..n).map(|_| rng.gen_range(0..16)).collect::<Vec<_>>();
            for z in 3984..4016 {
                let this = &this[..z];
                let specialized = unsafe { reduce_sum_of_x_as_u32_v4(this) };
                let fallback = unsafe { reduce_sum_of_x_as_u32_fallback(this) };
                assert_eq!(specialized, fallback);
            }
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[detect::target_cpu(enable = "v3")]
    unsafe fn reduce_sum_of_x_as_u32_v3(this: &[u8]) -> u32 {
        use base::scalar::emulate::emulate_mm256_reduce_add_epi32;
        unsafe {
            use std::arch::x86_64::*;
            let us = _mm256_set1_epi32(255);
            let mut n = this.len();
            let mut a = this.as_ptr();
            let mut sum = _mm256_setzero_si256();
            while n >= 8 {
                let x = _mm_loadl_epi64(a.cast());
                a = a.add(8);
                n -= 8;
                sum = _mm256_add_epi32(_mm256_and_si256(us, _mm256_cvtepi8_epi32(x)), sum);
            }
            let mut sum = emulate_mm256_reduce_add_epi32(sum) as u32;
            // this hint is used to disable loop unrolling
            while std::hint::black_box(n) > 0 {
                let x = a.read();
                a = a.add(1);
                n -= 1;
                sum += x as u32;
            }
            sum
        }
    }

    #[cfg(all(target_arch = "x86_64", test))]
    #[test]
    fn reduce_sum_of_x_as_u16_v3_test() {
        use rand::Rng;
        detect::init();
        if !detect::v3::detect() {
            println!("test {} ... skipped (v3)", module_path!());
            return;
        }
        let mut rng = rand::thread_rng();
        for _ in 0..256 {
            let n = 4016;
            let this = (0..n).map(|_| rng.gen_range(0..16)).collect::<Vec<_>>();
            for z in 3984..4016 {
                let this = &this[..z];
                let specialized = unsafe { reduce_sum_of_x_as_u32_v3(this) };
                let fallback = unsafe { reduce_sum_of_x_as_u32_fallback(this) };
                assert_eq!(specialized, fallback);
            }
        }
    }

    #[detect::multiversion(v4 = import, v3 = import, v2, neon, fallback = export)]
    pub fn reduce_sum_of_x_as_u32(this: &[u8]) -> u32 {
        let n = this.len();
        let mut sum = 0;
        for i in 0..n {
            sum += this[i] as u32;
        }
        sum
    }
}

#[inline(always)]
pub fn quantize<const N: u8>(lut: &[f32]) -> (f32, f32, Vec<u8>) {
    let (min, max) = f32::reduce_min_max_of_x(lut);
    let k = 0.0f32.max((max - min) / (N as f32));
    let b = min;
    (k, b, mul_add_round::mul_add_round(lut, 1.0 / k, -b / k))
}

#[inline(always)]
pub fn dequantize(sum_1: u32, k: f32, b: f32, sum_x: u16) -> f32 {
    (sum_1 as f32) * b + (sum_x as f32) * k
}

#[inline(always)]
pub fn reduce_sum_of_x_as_u16(vector: &[u8]) -> u16 {
    reduce_sum_of_x_as_u16::reduce_sum_of_x_as_u16(vector)
}

#[inline(always)]
pub fn reduce_sum_of_x_as_u32(vector: &[u8]) -> u32 {
    reduce_sum_of_x_as_u32::reduce_sum_of_x_as_u32(vector)
}
