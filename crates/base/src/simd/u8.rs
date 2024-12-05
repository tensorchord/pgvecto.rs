use crate::simd::*;

#[detect::multiversion(v4, v3, v2, neon, fallback)]
pub fn reduce_sum_of_xy(s: &[u8], t: &[u8]) -> u32 {
    assert_eq!(s.len(), t.len());
    let n = s.len();
    let mut result = 0;
    for i in 0..n {
        result += (s[i] as u32) * (t[i] as u32);
    }
    result
}

mod reduce_sum_of_x_as_u16 {
    #[cfg(target_arch = "x86_64")]
    #[detect::target_cpu(enable = "v4")]
    unsafe fn reduce_sum_of_x_as_u16_v4(this: &[u8]) -> u16 {
        use crate::simd::emulate::emulate_mm512_reduce_add_epi16;
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
        use crate::simd::emulate::emulate_mm256_reduce_add_epi16;
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

#[inline(always)]
pub fn reduce_sum_of_x_as_u16(vector: &[u8]) -> u16 {
    reduce_sum_of_x_as_u16::reduce_sum_of_x_as_u16(vector)
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
        use crate::simd::emulate::emulate_mm256_reduce_add_epi32;
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
pub fn reduce_sum_of_x_as_u32(vector: &[u8]) -> u32 {
    reduce_sum_of_x_as_u32::reduce_sum_of_x_as_u32(vector)
}
