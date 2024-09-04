use crate::scalar::{f32, ScalarLike};
use half::f16;

impl ScalarLike for f16 {
    #[inline(always)]
    fn zero() -> Self {
        f16::ZERO
    }

    #[inline(always)]
    fn infinity() -> Self {
        f16::INFINITY
    }

    #[inline(always)]
    fn mask(self, m: bool) -> Self {
        f16::from_bits(self.to_bits() & (m as u16).wrapping_neg())
    }

    #[inline(always)]
    fn scalar_neg(this: Self) -> Self {
        -this
    }

    #[inline(always)]
    fn scalar_add(lhs: Self, rhs: Self) -> Self {
        lhs + rhs
    }

    #[inline(always)]
    fn scalar_sub(lhs: Self, rhs: Self) -> Self {
        lhs - rhs
    }

    #[inline(always)]
    fn scalar_mul(lhs: Self, rhs: Self) -> Self {
        lhs * rhs
    }

    #[inline(always)]
    fn from_f32(x: f32) -> Self {
        f16::from_f32(x)
    }

    #[inline(always)]
    fn to_f32(self) -> f32 {
        f16::to_f32(self)
    }

    // FIXME: add manually-implemented SIMD version
    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn reduce_or_of_is_zero(this: &[f16]) -> bool {
        for &x in this {
            if x == f16::ZERO {
                return true;
            }
        }
        false
    }

    // FIXME: add manually-implemented SIMD version
    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn reduce_sum_of_x(this: &[f16]) -> f32 {
        let n = this.len();
        let mut x = 0.0f32;
        for i in 0..n {
            x += this[i].to_f32();
        }
        x
    }

    // FIXME: add manually-implemented SIMD version
    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn reduce_sum_of_abs_x(this: &[f16]) -> f32 {
        let n = this.len();
        let mut x = 0.0f32;
        for i in 0..n {
            x += this[i].to_f32().abs();
        }
        x
    }

    // FIXME: add manually-implemented SIMD version
    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn reduce_sum_of_x2(this: &[f16]) -> f32 {
        let n = this.len();
        let mut x2 = 0.0f32;
        for i in 0..n {
            x2 += this[i].to_f32() * this[i].to_f32();
        }
        x2
    }

    // FIXME: add manually-implemented SIMD version
    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn reduce_min_max_of_x(this: &[f16]) -> (f32, f32) {
        let mut min = f32::INFINITY;
        let mut max = f32::NEG_INFINITY;
        let n = this.len();
        for i in 0..n {
            min = min.min(this[i].to_f32());
            max = max.max(this[i].to_f32());
        }
        (min, max)
    }

    #[inline(always)]
    fn reduce_sum_of_xy(lhs: &[Self], rhs: &[Self]) -> f32 {
        reduce_sum_of_xy::reduce_sum_of_xy(lhs, rhs)
    }

    #[inline(always)]
    fn reduce_sum_of_d2(lhs: &[f16], rhs: &[f16]) -> f32 {
        reduce_sum_of_d2::reduce_sum_of_d2(lhs, rhs)
    }

    #[inline(always)]
    fn reduce_sum_of_sparse_xy(lidx: &[u32], lval: &[f16], ridx: &[u32], rval: &[f16]) -> f32 {
        reduce_sum_of_sparse_xy::reduce_sum_of_sparse_xy(lidx, lval, ridx, rval)
    }

    #[inline(always)]
    fn reduce_sum_of_sparse_d2(lidx: &[u32], lval: &[f16], ridx: &[u32], rval: &[f16]) -> f32 {
        reduce_sum_of_sparse_d2::reduce_sum_of_sparse_d2(lidx, lval, ridx, rval)
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn vector_add(lhs: &[f16], rhs: &[f16]) -> Vec<f16> {
        assert_eq!(lhs.len(), rhs.len());
        let n = lhs.len();
        let mut r = Vec::<f16>::with_capacity(n);
        for i in 0..n {
            unsafe {
                r.as_mut_ptr().add(i).write(lhs[i] + rhs[i]);
            }
        }
        unsafe {
            r.set_len(n);
        }
        r
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn vector_add_inplace(lhs: &mut [f16], rhs: &[f16]) {
        assert_eq!(lhs.len(), rhs.len());
        let n = lhs.len();
        for i in 0..n {
            lhs[i] += rhs[i];
        }
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn vector_sub(lhs: &[f16], rhs: &[f16]) -> Vec<f16> {
        assert_eq!(lhs.len(), rhs.len());
        let n = lhs.len();
        let mut r = Vec::<f16>::with_capacity(n);
        for i in 0..n {
            unsafe {
                r.as_mut_ptr().add(i).write(lhs[i] - rhs[i]);
            }
        }
        unsafe {
            r.set_len(n);
        }
        r
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn vector_mul(lhs: &[f16], rhs: &[f16]) -> Vec<f16> {
        assert_eq!(lhs.len(), rhs.len());
        let n = lhs.len();
        let mut r = Vec::<f16>::with_capacity(n);
        for i in 0..n {
            unsafe {
                r.as_mut_ptr().add(i).write(lhs[i] * rhs[i]);
            }
        }
        unsafe {
            r.set_len(n);
        }
        r
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn vector_mul_scalar(lhs: &[f16], rhs: f32) -> Vec<f16> {
        let rhs = f16::from_f32(rhs);
        let n = lhs.len();
        let mut r = Vec::<f16>::with_capacity(n);
        for i in 0..n {
            unsafe {
                r.as_mut_ptr().add(i).write(lhs[i] * rhs);
            }
        }
        unsafe {
            r.set_len(n);
        }
        r
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn vector_mul_scalar_inplace(lhs: &mut [f16], rhs: f32) {
        let rhs = f16::from_f32(rhs);
        let n = lhs.len();
        for i in 0..n {
            lhs[i] *= rhs;
        }
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn vector_from_f32(this: &[f32]) -> Vec<f16> {
        let n = this.len();
        let mut r = Vec::<f16>::with_capacity(n);
        for i in 0..n {
            unsafe {
                r.as_mut_ptr().add(i).write(f16::from_f32(this[i]));
            }
        }
        unsafe {
            r.set_len(n);
        }
        r
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn vector_to_f32(this: &[f16]) -> Vec<f32> {
        let n = this.len();
        let mut r = Vec::<f32>::with_capacity(n);
        for i in 0..n {
            unsafe {
                r.as_mut_ptr().add(i).write(this[i].to_f32());
            }
        }
        unsafe {
            r.set_len(n);
        }
        r
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn kmeans_helper(this: &mut [f16], x: f32, y: f32) {
        let x = f16::from_f32(x);
        let y = f16::from_f32(y);
        let n = this.len();
        for i in 0..n {
            if i % 2 == 0 {
                this[i] *= x;
            } else {
                this[i] *= y;
            }
        }
    }
}

mod reduce_sum_of_xy {
    use half::f16;

    #[cfg(target_arch = "x86_64")]
    #[detect::target_cpu(enable = "v4_avx512fp16")]
    pub unsafe fn reduce_sum_of_xy_v4_avx512fp16(lhs: &[f16], rhs: &[f16]) -> f32 {
        assert!(lhs.len() == rhs.len());
        unsafe {
            use std::arch::x86_64::*;
            let mut n = lhs.len();
            let mut a = lhs.as_ptr();
            let mut b = rhs.as_ptr();
            let mut xy = _mm512_setzero_ph();
            while n >= 32 {
                let x = _mm512_loadu_ph(a.cast());
                let y = _mm512_loadu_ph(b.cast());
                a = a.add(32);
                b = b.add(32);
                n -= 32;
                xy = _mm512_fmadd_ph(x, y, xy);
            }
            if n > 0 {
                let mask = _bzhi_u32(0xffffffff, n as u32);
                let x = _mm512_castsi512_ph(_mm512_maskz_loadu_epi16(mask, a.cast()));
                let y = _mm512_castsi512_ph(_mm512_maskz_loadu_epi16(mask, b.cast()));
                xy = _mm512_fmadd_ph(x, y, xy);
            }
            _mm512_reduce_add_ph(xy) as f32
        }
    }

    #[cfg(all(target_arch = "x86_64", test))]
    #[test]
    fn reduce_sum_of_xy_v4_avx512fp16_test() {
        use rand::Rng;
        const EPSILON: f32 = 2.0;
        detect::init();
        if !detect::v4_avx512fp16::detect() {
            println!("test {} ... skipped (v4_avx512fp16)", module_path!());
            return;
        }
        let mut rng = rand::thread_rng();
        for _ in 0..256 {
            let n = 4016;
            let lhs = (0..n)
                .map(|_| f16::from_f32(rng.gen_range(-1.0..=1.0)))
                .collect::<Vec<_>>();
            let rhs = (0..n)
                .map(|_| f16::from_f32(rng.gen_range(-1.0..=1.0)))
                .collect::<Vec<_>>();
            for z in 3984..4016 {
                let lhs = &lhs[..z];
                let rhs = &rhs[..z];
                let specialized = unsafe { reduce_sum_of_xy_v4_avx512fp16(lhs, rhs) };
                let fallback = unsafe { reduce_sum_of_xy_fallback(lhs, rhs) };
                assert!(
                    (specialized - fallback).abs() < EPSILON,
                    "specialized = {specialized}, fallback = {fallback}."
                );
            }
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[detect::target_cpu(enable = "v4")]
    pub unsafe fn reduce_sum_of_xy_v4(lhs: &[f16], rhs: &[f16]) -> f32 {
        assert!(lhs.len() == rhs.len());
        unsafe {
            use std::arch::x86_64::*;
            let mut n = lhs.len();
            let mut a = lhs.as_ptr();
            let mut b = rhs.as_ptr();
            let mut xy = _mm512_setzero_ps();
            while n >= 16 {
                let x = _mm512_cvtph_ps(_mm256_loadu_epi16(a.cast()));
                let y = _mm512_cvtph_ps(_mm256_loadu_epi16(b.cast()));
                a = a.add(16);
                b = b.add(16);
                n -= 16;
                xy = _mm512_fmadd_ps(x, y, xy);
            }
            if n > 0 {
                let mask = _bzhi_u32(0xffff, n as u32) as u16;
                let x = _mm512_cvtph_ps(_mm256_maskz_loadu_epi16(mask, a.cast()));
                let y = _mm512_cvtph_ps(_mm256_maskz_loadu_epi16(mask, b.cast()));
                xy = _mm512_fmadd_ps(x, y, xy);
            }
            _mm512_reduce_add_ps(xy)
        }
    }

    #[cfg(all(target_arch = "x86_64", test))]
    #[test]
    fn reduce_sum_of_xy_v4_test() {
        use rand::Rng;
        const EPSILON: f32 = 2.0;
        detect::init();
        if !detect::v4::detect() {
            println!("test {} ... skipped (v4)", module_path!());
            return;
        }
        let mut rng = rand::thread_rng();
        for _ in 0..256 {
            let n = 4016;
            let lhs = (0..n)
                .map(|_| f16::from_f32(rng.gen_range(-1.0..=1.0)))
                .collect::<Vec<_>>();
            let rhs = (0..n)
                .map(|_| f16::from_f32(rng.gen_range(-1.0..=1.0)))
                .collect::<Vec<_>>();
            let specialized = unsafe { reduce_sum_of_xy_v4(&lhs, &rhs) };
            let fallback = unsafe { reduce_sum_of_xy_fallback(&lhs, &rhs) };
            assert!(
                (specialized - fallback).abs() < EPSILON,
                "specialized = {specialized}, fallback = {fallback}."
            );
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[detect::target_cpu(enable = "v3")]
    pub unsafe fn reduce_sum_of_xy_v3(lhs: &[f16], rhs: &[f16]) -> f32 {
        use crate::scalar::emulate::emulate_mm256_reduce_add_ps;
        assert!(lhs.len() == rhs.len());
        unsafe {
            use std::arch::x86_64::*;
            let mut n = lhs.len();
            let mut a = lhs.as_ptr();
            let mut b = rhs.as_ptr();
            let mut xy = _mm256_setzero_ps();
            while n >= 8 {
                let x = _mm256_cvtph_ps(_mm_loadu_si128(a.cast()));
                let y = _mm256_cvtph_ps(_mm_loadu_si128(b.cast()));
                a = a.add(8);
                b = b.add(8);
                n -= 8;
                xy = _mm256_fmadd_ps(x, y, xy);
            }
            let mut xy = emulate_mm256_reduce_add_ps(xy);
            while n > 0 {
                let x = a.read().to_f32();
                let y = b.read().to_f32();
                a = a.add(1);
                b = b.add(1);
                n -= 1;
                xy = x.mul_add(y, xy);
            }
            xy
        }
    }

    #[cfg(all(target_arch = "x86_64", test))]
    #[test]
    fn reduce_sum_of_xy_v3_test() {
        use rand::Rng;
        const EPSILON: f32 = 2.0;
        detect::init();
        if !detect::v3::detect() {
            println!("test {} ... skipped (v3)", module_path!());
            return;
        }
        let mut rng = rand::thread_rng();
        for _ in 0..256 {
            let n = 4016;
            let lhs = (0..n)
                .map(|_| f16::from_f32(rng.gen_range(-1.0..=1.0)))
                .collect::<Vec<_>>();
            let rhs = (0..n)
                .map(|_| f16::from_f32(rng.gen_range(-1.0..=1.0)))
                .collect::<Vec<_>>();
            for z in 3984..4016 {
                let lhs = &lhs[..z];
                let rhs = &rhs[..z];
                let specialized = unsafe { reduce_sum_of_xy_v3(lhs, rhs) };
                let fallback = unsafe { reduce_sum_of_xy_fallback(lhs, rhs) };
                assert!(
                    (specialized - fallback).abs() < EPSILON,
                    "specialized = {specialized}, fallback = {fallback}."
                );
            }
        }
    }

    #[detect::multiversion(v4_avx512fp16 = import, v4 = import, v3 = import, v2, neon, fallback = export)]
    pub fn reduce_sum_of_xy(lhs: &[f16], rhs: &[f16]) -> f32 {
        assert!(lhs.len() == rhs.len());
        let n = lhs.len();
        let mut xy = 0.0f32;
        for i in 0..n {
            xy += lhs[i].to_f32() * rhs[i].to_f32();
        }
        xy
    }
}

mod reduce_sum_of_d2 {
    use half::f16;

    #[cfg(target_arch = "x86_64")]
    #[detect::target_cpu(enable = "v4_avx512fp16")]
    pub unsafe fn reduce_sum_of_d2_v4_avx512fp16(lhs: &[f16], rhs: &[f16]) -> f32 {
        assert!(lhs.len() == rhs.len());
        unsafe {
            use std::arch::x86_64::*;
            let mut n = lhs.len() as u32;
            let mut a = lhs.as_ptr();
            let mut b = rhs.as_ptr();
            let mut d2 = _mm512_setzero_ph();
            while n >= 32 {
                let x = _mm512_loadu_ph(a.cast());
                let y = _mm512_loadu_ph(b.cast());
                a = a.add(32);
                b = b.add(32);
                n -= 32;
                let d = _mm512_sub_ph(x, y);
                d2 = _mm512_fmadd_ph(d, d, d2);
            }
            if n > 0 {
                let mask = _bzhi_u32(0xffffffff, n);
                let x = _mm512_castsi512_ph(_mm512_maskz_loadu_epi16(mask, a.cast()));
                let y = _mm512_castsi512_ph(_mm512_maskz_loadu_epi16(mask, b.cast()));
                let d = _mm512_sub_ph(x, y);
                d2 = _mm512_fmadd_ph(d, d, d2);
            }
            _mm512_reduce_add_ph(d2) as f32
        }
    }

    #[cfg(all(target_arch = "x86_64", test))]
    #[test]
    fn reduce_sum_of_d2_v4_avx512fp16_test() {
        use rand::Rng;
        const EPSILON: f32 = 6.0;
        detect::init();
        if !detect::v4_avx512fp16::detect() {
            println!("test {} ... skipped (v4_avx512fp16)", module_path!());
            return;
        }
        let mut rng = rand::thread_rng();
        for _ in 0..256 {
            let n = 4016;
            let lhs = (0..n)
                .map(|_| f16::from_f32(rng.gen_range(-1.0..=1.0)))
                .collect::<Vec<_>>();
            let rhs = (0..n)
                .map(|_| f16::from_f32(rng.gen_range(-1.0..=1.0)))
                .collect::<Vec<_>>();
            for z in 3984..4016 {
                let lhs = &lhs[..z];
                let rhs = &rhs[..z];
                let specialized = unsafe { reduce_sum_of_d2_v4_avx512fp16(lhs, rhs) };
                let fallback = unsafe { reduce_sum_of_d2_fallback(lhs, rhs) };
                assert!(
                    (specialized - fallback).abs() < EPSILON,
                    "specialized = {specialized}, fallback = {fallback}."
                );
            }
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[detect::target_cpu(enable = "v4")]
    pub unsafe fn reduce_sum_of_d2_v4(lhs: &[f16], rhs: &[f16]) -> f32 {
        assert!(lhs.len() == rhs.len());
        unsafe {
            use std::arch::x86_64::*;
            let mut n = lhs.len() as u32;
            let mut a = lhs.as_ptr();
            let mut b = rhs.as_ptr();
            let mut d2 = _mm512_setzero_ps();
            while n >= 16 {
                let x = _mm512_cvtph_ps(_mm256_loadu_epi16(a.cast()));
                let y = _mm512_cvtph_ps(_mm256_loadu_epi16(b.cast()));
                a = a.add(16);
                b = b.add(16);
                n -= 16;
                let d = _mm512_sub_ps(x, y);
                d2 = _mm512_fmadd_ps(d, d, d2);
            }
            if n > 0 {
                let mask = _bzhi_u32(0xffff, n) as u16;
                let x = _mm512_cvtph_ps(_mm256_maskz_loadu_epi16(mask, a.cast()));
                let y = _mm512_cvtph_ps(_mm256_maskz_loadu_epi16(mask, b.cast()));
                let d = _mm512_sub_ps(x, y);
                d2 = _mm512_fmadd_ps(d, d, d2);
            }
            _mm512_reduce_add_ps(d2)
        }
    }

    #[cfg(all(target_arch = "x86_64", test))]
    #[test]
    fn reduce_sum_of_d2_v4_test() {
        use rand::Rng;
        const EPSILON: f32 = 2.0;
        detect::init();
        if !detect::v4::detect() {
            println!("test {} ... skipped (v4)", module_path!());
            return;
        }
        let mut rng = rand::thread_rng();
        for _ in 0..256 {
            let n = 4016;
            let lhs = (0..n)
                .map(|_| f16::from_f32(rng.gen_range(-1.0..=1.0)))
                .collect::<Vec<_>>();
            let rhs = (0..n)
                .map(|_| f16::from_f32(rng.gen_range(-1.0..=1.0)))
                .collect::<Vec<_>>();
            for z in 3984..4016 {
                let lhs = &lhs[..z];
                let rhs = &rhs[..z];
                let specialized = unsafe { reduce_sum_of_d2_v4(lhs, rhs) };
                let fallback = unsafe { reduce_sum_of_d2_fallback(lhs, rhs) };
                assert!(
                    (specialized - fallback).abs() < EPSILON,
                    "specialized = {specialized}, fallback = {fallback}."
                );
            }
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[detect::target_cpu(enable = "v3")]
    pub unsafe fn reduce_sum_of_d2_v3(lhs: &[f16], rhs: &[f16]) -> f32 {
        use crate::scalar::emulate::emulate_mm256_reduce_add_ps;
        assert!(lhs.len() == rhs.len());
        unsafe {
            use std::arch::x86_64::*;
            let mut n = lhs.len() as u32;
            let mut a = lhs.as_ptr();
            let mut b = rhs.as_ptr();
            let mut d2 = _mm256_setzero_ps();
            while n >= 8 {
                let x = _mm256_cvtph_ps(_mm_loadu_si128(a.cast()));
                let y = _mm256_cvtph_ps(_mm_loadu_si128(b.cast()));
                a = a.add(8);
                b = b.add(8);
                n -= 8;
                let d = _mm256_sub_ps(x, y);
                d2 = _mm256_fmadd_ps(d, d, d2);
            }
            let mut d2 = emulate_mm256_reduce_add_ps(d2);
            while n > 0 {
                let x = a.read().to_f32();
                let y = b.read().to_f32();
                a = a.add(1);
                b = b.add(1);
                n -= 1;
                let d = x - y;
                d2 = d.mul_add(d, d2);
            }
            d2
        }
    }

    #[cfg(all(target_arch = "x86_64", test))]
    #[test]
    fn reduce_sum_of_d2_v3_test() {
        use rand::Rng;
        const EPSILON: f32 = 2.0;
        detect::init();
        if !detect::v3::detect() {
            println!("test {} ... skipped (v3)", module_path!());
            return;
        }
        let mut rng = rand::thread_rng();
        for _ in 0..256 {
            let n = 4016;
            let lhs = (0..n)
                .map(|_| f16::from_f32(rng.gen_range(-1.0..=1.0)))
                .collect::<Vec<_>>();
            let rhs = (0..n)
                .map(|_| f16::from_f32(rng.gen_range(-1.0..=1.0)))
                .collect::<Vec<_>>();
            for z in 3984..4016 {
                let lhs = &lhs[..z];
                let rhs = &rhs[..z];
                let specialized = unsafe { reduce_sum_of_d2_v3(lhs, rhs) };
                let fallback = unsafe { reduce_sum_of_d2_fallback(lhs, rhs) };
                assert!(
                    (specialized - fallback).abs() < EPSILON,
                    "specialized = {specialized}, fallback = {fallback}."
                );
            }
        }
    }

    #[detect::multiversion(v4_avx512fp16 = import, v4 = import, v3 = import, v2, neon, fallback = export)]
    pub fn reduce_sum_of_d2(lhs: &[f16], rhs: &[f16]) -> f32 {
        assert!(lhs.len() == rhs.len());
        let n = lhs.len();
        let mut d2 = 0.0;
        for i in 0..n {
            let d = lhs[i].to_f32() - rhs[i].to_f32();
            d2 += d * d;
        }
        d2
    }
}

mod reduce_sum_of_sparse_xy {
    // There is no manually-implemented SIMD version.
    // Add it if `svecf16` is supported.

    use half::f16;

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    pub fn reduce_sum_of_sparse_xy(lidx: &[u32], lval: &[f16], ridx: &[u32], rval: &[f16]) -> f32 {
        use std::cmp::Ordering;
        assert_eq!(lidx.len(), lval.len());
        assert_eq!(ridx.len(), rval.len());
        let (mut lp, ln) = (0, lidx.len());
        let (mut rp, rn) = (0, ridx.len());
        let mut xy = 0.0f32;
        while lp < ln && rp < rn {
            match Ord::cmp(&lidx[lp], &ridx[rp]) {
                Ordering::Equal => {
                    xy += lval[lp].to_f32() * rval[rp].to_f32();
                    lp += 1;
                    rp += 1;
                }
                Ordering::Less => {
                    lp += 1;
                }
                Ordering::Greater => {
                    rp += 1;
                }
            }
        }
        xy
    }
}

mod reduce_sum_of_sparse_d2 {
    // There is no manually-implemented SIMD version.
    // Add it if `svecf16` is supported.

    use half::f16;

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    pub fn reduce_sum_of_sparse_d2(lidx: &[u32], lval: &[f16], ridx: &[u32], rval: &[f16]) -> f32 {
        use std::cmp::Ordering;
        assert_eq!(lidx.len(), lval.len());
        assert_eq!(ridx.len(), rval.len());
        let (mut lp, ln) = (0, lidx.len());
        let (mut rp, rn) = (0, ridx.len());
        let mut d2 = 0.0f32;
        while lp < ln && rp < rn {
            match Ord::cmp(&lidx[lp], &ridx[rp]) {
                Ordering::Equal => {
                    let d = lval[lp].to_f32() - rval[rp].to_f32();
                    d2 += d * d;
                    lp += 1;
                    rp += 1;
                }
                Ordering::Less => {
                    d2 += lval[lp].to_f32() * lval[lp].to_f32();
                    lp += 1;
                }
                Ordering::Greater => {
                    d2 += rval[rp].to_f32() * rval[rp].to_f32();
                    rp += 1;
                }
            }
        }
        for i in lp..ln {
            d2 += lval[i].to_f32() * lval[i].to_f32();
        }
        for i in rp..rn {
            d2 += rval[i].to_f32() * rval[i].to_f32();
        }
        d2
    }
}
