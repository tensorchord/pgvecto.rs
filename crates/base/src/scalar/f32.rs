use crate::scalar::ScalarLike;

impl ScalarLike for f32 {
    #[inline(always)]
    fn zero() -> Self {
        0.0f32
    }

    #[inline(always)]
    fn infinity() -> Self {
        f32::INFINITY
    }

    #[inline(always)]
    fn mask(self, m: bool) -> Self {
        f32::from_bits(self.to_bits() & (m as u32).wrapping_neg())
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
    fn scalar_is_sign_positive(self) -> bool {
        self.is_sign_positive()
    }

    #[inline(always)]
    fn scalar_is_sign_negative(self) -> bool {
        self.is_sign_negative()
    }

    #[inline(always)]
    fn from_f32(x: f32) -> Self {
        x
    }

    #[inline(always)]
    fn to_f32(self) -> f32 {
        self
    }

    // FIXME: add manually-implemented SIMD version
    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn reduce_or_of_is_zero(this: &[f32]) -> bool {
        for &x in this {
            if x == 0.0f32 {
                return true;
            }
        }
        false
    }

    #[inline(always)]
    fn reduce_sum_of_x(this: &[f32]) -> f32 {
        reduce_sum_of_x::reduce_sum_of_x(this)
    }

    #[inline(always)]
    fn reduce_sum_of_abs_x(this: &[f32]) -> f32 {
        reduce_sum_of_abs_x::reduce_sum_of_abs_x(this)
    }

    #[inline(always)]
    fn reduce_sum_of_x2(this: &[f32]) -> f32 {
        reduce_sum_of_x2::reduce_sum_of_x2(this)
    }

    #[inline(always)]
    fn reduce_min_max_of_x(this: &[f32]) -> (f32, f32) {
        reduce_min_max_of_x::reduce_min_max_of_x(this)
    }

    #[inline(always)]
    fn reduce_sum_of_xy(lhs: &[Self], rhs: &[Self]) -> f32 {
        reduce_sum_of_xy::reduce_sum_of_xy(lhs, rhs)
    }

    #[inline(always)]
    fn reduce_sum_of_d2(lhs: &[Self], rhs: &[Self]) -> f32 {
        reduce_sum_of_d2::reduce_sum_of_d2(lhs, rhs)
    }

    #[inline(always)]
    fn reduce_sum_of_sparse_xy(lidx: &[u32], lval: &[f32], ridx: &[u32], rval: &[f32]) -> f32 {
        reduce_sum_of_sparse_xy::reduce_sum_of_sparse_xy(lidx, lval, ridx, rval)
    }

    #[inline(always)]
    fn reduce_sum_of_sparse_d2(lidx: &[u32], lval: &[f32], ridx: &[u32], rval: &[f32]) -> f32 {
        reduce_sum_of_sparse_d2::reduce_sum_of_sparse_d2(lidx, lval, ridx, rval)
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn vector_add(lhs: &[f32], rhs: &[f32]) -> Vec<f32> {
        assert_eq!(lhs.len(), rhs.len());
        let n = lhs.len();
        let mut r = Vec::<f32>::with_capacity(n);
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
    fn vector_add_inplace(lhs: &mut [f32], rhs: &[f32]) {
        assert_eq!(lhs.len(), rhs.len());
        let n = lhs.len();
        for i in 0..n {
            lhs[i] += rhs[i];
        }
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn vector_sub(lhs: &[f32], rhs: &[f32]) -> Vec<f32> {
        assert_eq!(lhs.len(), rhs.len());
        let n = lhs.len();
        let mut r = Vec::<f32>::with_capacity(n);
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
    fn vector_mul(lhs: &[f32], rhs: &[f32]) -> Vec<f32> {
        assert_eq!(lhs.len(), rhs.len());
        let n = lhs.len();
        let mut r = Vec::<f32>::with_capacity(n);
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
    fn vector_mul_scalar(lhs: &[f32], rhs: f32) -> Vec<f32> {
        let n = lhs.len();
        let mut r = Vec::<f32>::with_capacity(n);
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
    fn vector_mul_scalar_inplace(lhs: &mut [f32], rhs: f32) {
        let n = lhs.len();
        for i in 0..n {
            lhs[i] *= rhs;
        }
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn vector_abs_inplace(this: &mut [f32]) {
        let n = this.len();
        for i in 0..n {
            this[i] = this[i].abs();
        }
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn vector_from_f32(this: &[f32]) -> Vec<f32> {
        this.to_vec()
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn vector_to_f32(this: &[f32]) -> Vec<f32> {
        this.to_vec()
    }

    fn vector_to_f32_borrowed(this: &[Self]) -> impl AsRef<[f32]> {
        this
    }

    #[detect::multiversion(v4, v3, v2, neon, fallback)]
    fn kmeans_helper(this: &mut [f32], x: f32, y: f32) {
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

mod reduce_sum_of_x {
    #[cfg(target_arch = "x86_64")]
    #[detect::target_cpu(enable = "v4")]
    unsafe fn reduce_sum_of_x_v4(this: &[f32]) -> f32 {
        unsafe {
            use std::arch::x86_64::*;
            let mut n = this.len();
            let mut a = this.as_ptr();
            let mut sum = _mm512_setzero_ps();
            while n >= 16 {
                let x = _mm512_loadu_ps(a);
                a = a.add(16);
                n -= 16;
                sum = _mm512_add_ps(x, sum);
            }
            if n > 0 {
                let mask = _bzhi_u32(0xffff, n as u32) as u16;
                let x = _mm512_maskz_loadu_ps(mask, a);
                sum = _mm512_add_ps(x, sum);
            }
            _mm512_reduce_add_ps(sum)
        }
    }

    #[cfg(all(target_arch = "x86_64", test))]
    #[test]
    fn reduce_sum_of_x_v4_test() {
        use rand::Rng;
        const EPSILON: f32 = 0.008;
        detect::init();
        if !detect::v4::detect() {
            println!("test {} ... skipped (v4)", module_path!());
            return;
        }
        let mut rng = rand::thread_rng();
        for _ in 0..256 {
            let n = 4016;
            let this = (0..n)
                .map(|_| rng.gen_range(-1.0..=1.0))
                .collect::<Vec<_>>();
            for z in 3984..4016 {
                let this = &this[..z];
                let specialized = unsafe { reduce_sum_of_x_v4(&this) };
                let fallback = unsafe { reduce_sum_of_x_fallback(&this) };
                assert!(
                    (specialized - fallback).abs() < EPSILON,
                    "specialized = {specialized}, fallback = {fallback}."
                );
            }
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[detect::target_cpu(enable = "v3")]
    unsafe fn reduce_sum_of_x_v3(this: &[f32]) -> f32 {
        use crate::scalar::emulate::emulate_mm256_reduce_add_ps;
        unsafe {
            use std::arch::x86_64::*;
            let mut n = this.len();
            let mut a = this.as_ptr();
            let mut sum = _mm256_setzero_ps();
            while n >= 8 {
                let x = _mm256_loadu_ps(a);
                a = a.add(8);
                n -= 8;
                sum = _mm256_add_ps(x, sum);
            }
            if n >= 4 {
                let x = _mm256_zextps128_ps256(_mm_loadu_ps(a));
                a = a.add(4);
                n -= 4;
                sum = _mm256_add_ps(x, sum);
            }
            let mut sum = emulate_mm256_reduce_add_ps(sum);
            // this hint is used to disable loop unrolling
            while std::hint::black_box(n) > 0 {
                let x = a.read();
                a = a.add(1);
                n -= 1;
                sum += x;
            }
            sum
        }
    }

    #[cfg(all(target_arch = "x86_64", test))]
    #[test]
    fn reduce_sum_of_x_v3_test() {
        use rand::Rng;
        const EPSILON: f32 = 0.008;
        detect::init();
        if !detect::v3::detect() {
            println!("test {} ... skipped (v3)", module_path!());
            return;
        }
        let mut rng = rand::thread_rng();
        for _ in 0..256 {
            let n = 4016;
            let this = (0..n)
                .map(|_| rng.gen_range(-1.0..=1.0))
                .collect::<Vec<_>>();
            for z in 3984..4016 {
                let this = &this[..z];
                let specialized = unsafe { reduce_sum_of_x_v3(this) };
                let fallback = unsafe { reduce_sum_of_x_fallback(this) };
                assert!(
                    (specialized - fallback).abs() < EPSILON,
                    "specialized = {specialized}, fallback = {fallback}."
                );
            }
        }
    }

    #[detect::multiversion(v4 = import, v3 = import, v2, neon, fallback = export)]
    pub fn reduce_sum_of_x(this: &[f32]) -> f32 {
        let n = this.len();
        let mut sum = 0.0f32;
        for i in 0..n {
            sum += this[i];
        }
        sum
    }
}

mod reduce_sum_of_abs_x {
    #[cfg(target_arch = "x86_64")]
    #[detect::target_cpu(enable = "v4")]
    unsafe fn reduce_sum_of_abs_x_v4(this: &[f32]) -> f32 {
        unsafe {
            use std::arch::x86_64::*;
            let mut n = this.len();
            let mut a = this.as_ptr();
            let mut sum = _mm512_setzero_ps();
            while n >= 16 {
                let x = _mm512_loadu_ps(a);
                let abs_x = _mm512_abs_ps(x);
                a = a.add(16);
                n -= 16;
                sum = _mm512_add_ps(abs_x, sum);
            }
            if n > 0 {
                let mask = _bzhi_u32(0xffff, n as u32) as u16;
                let x = _mm512_maskz_loadu_ps(mask, a);
                let abs_x = _mm512_abs_ps(x);
                sum = _mm512_add_ps(abs_x, sum);
            }
            _mm512_reduce_add_ps(sum)
        }
    }

    #[cfg(all(target_arch = "x86_64", test))]
    #[test]
    fn reduce_sum_of_abs_x_v4_test() {
        use rand::Rng;
        const EPSILON: f32 = 0.008;
        detect::init();
        if !detect::v4::detect() {
            println!("test {} ... skipped (v4)", module_path!());
            return;
        }
        let mut rng = rand::thread_rng();
        for _ in 0..256 {
            let n = 4016;
            let this = (0..n)
                .map(|_| rng.gen_range(-1.0..=1.0))
                .collect::<Vec<_>>();
            for z in 3984..4016 {
                let this = &this[..z];
                let specialized = unsafe { reduce_sum_of_abs_x_v4(&this) };
                let fallback = unsafe { reduce_sum_of_abs_x_fallback(&this) };
                assert!(
                    (specialized - fallback).abs() < EPSILON,
                    "specialized = {specialized}, fallback = {fallback}."
                );
            }
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[detect::target_cpu(enable = "v3")]
    unsafe fn reduce_sum_of_abs_x_v3(this: &[f32]) -> f32 {
        use crate::scalar::emulate::emulate_mm256_reduce_add_ps;
        unsafe {
            use std::arch::x86_64::*;
            let abs = _mm256_castsi256_ps(_mm256_srli_epi32(_mm256_set1_epi32(-1), 1));
            let mut n = this.len();
            let mut a = this.as_ptr();
            let mut sum = _mm256_setzero_ps();
            while n >= 8 {
                let x = _mm256_loadu_ps(a);
                let abs_x = _mm256_and_ps(abs, x);
                a = a.add(8);
                n -= 8;
                sum = _mm256_add_ps(abs_x, sum);
            }
            if n >= 4 {
                let x = _mm256_zextps128_ps256(_mm_loadu_ps(a));
                let abs_x = _mm256_and_ps(abs, x);
                a = a.add(4);
                n -= 4;
                sum = _mm256_add_ps(abs_x, sum);
            }
            let mut sum = emulate_mm256_reduce_add_ps(sum);
            // this hint is used to disable loop unrolling
            while std::hint::black_box(n) > 0 {
                let x = a.read();
                let abs_x = x.abs();
                a = a.add(1);
                n -= 1;
                sum += abs_x;
            }
            sum
        }
    }

    #[cfg(all(target_arch = "x86_64", test))]
    #[test]
    fn reduce_sum_of_abs_x_v3_test() {
        use rand::Rng;
        const EPSILON: f32 = 0.008;
        detect::init();
        if !detect::v3::detect() {
            println!("test {} ... skipped (v3)", module_path!());
            return;
        }
        let mut rng = rand::thread_rng();
        for _ in 0..256 {
            let n = 4016;
            let this = (0..n)
                .map(|_| rng.gen_range(-1.0..=1.0))
                .collect::<Vec<_>>();
            for z in 3984..4016 {
                let this = &this[..z];
                let specialized = unsafe { reduce_sum_of_abs_x_v3(this) };
                let fallback = unsafe { reduce_sum_of_abs_x_fallback(this) };
                assert!(
                    (specialized - fallback).abs() < EPSILON,
                    "specialized = {specialized}, fallback = {fallback}."
                );
            }
        }
    }

    #[detect::multiversion(v4 = import, v3 = import, v2, neon, fallback = export)]
    pub fn reduce_sum_of_abs_x(this: &[f32]) -> f32 {
        let n = this.len();
        let mut sum = 0.0f32;
        for i in 0..n {
            sum += this[i].abs();
        }
        sum
    }
}

mod reduce_sum_of_x2 {
    #[cfg(target_arch = "x86_64")]
    #[detect::target_cpu(enable = "v4")]
    unsafe fn reduce_sum_of_x2_v4(this: &[f32]) -> f32 {
        unsafe {
            use std::arch::x86_64::*;
            let mut n = this.len();
            let mut a = this.as_ptr();
            let mut x2 = _mm512_setzero_ps();
            while n >= 16 {
                let x = _mm512_loadu_ps(a);
                a = a.add(16);
                n -= 16;
                x2 = _mm512_fmadd_ps(x, x, x2);
            }
            if n > 0 {
                let mask = _bzhi_u32(0xffff, n as u32) as u16;
                let x = _mm512_maskz_loadu_ps(mask, a);
                x2 = _mm512_fmadd_ps(x, x, x2);
            }
            _mm512_reduce_add_ps(x2)
        }
    }

    #[cfg(all(target_arch = "x86_64", test))]
    #[test]
    fn reduce_sum_of_x2_v4_test() {
        use rand::Rng;
        const EPSILON: f32 = 0.006;
        detect::init();
        if !detect::v4::detect() {
            println!("test {} ... skipped (v4)", module_path!());
            return;
        }
        let mut rng = rand::thread_rng();
        for _ in 0..256 {
            let n = 4016;
            let this = (0..n)
                .map(|_| rng.gen_range(-1.0..=1.0))
                .collect::<Vec<_>>();
            for z in 3984..4016 {
                let this = &this[..z];
                let specialized = unsafe { reduce_sum_of_x2_v4(&this) };
                let fallback = unsafe { reduce_sum_of_x2_fallback(&this) };
                assert!(
                    (specialized - fallback).abs() < EPSILON,
                    "specialized = {specialized}, fallback = {fallback}."
                );
            }
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[detect::target_cpu(enable = "v3")]
    unsafe fn reduce_sum_of_x2_v3(this: &[f32]) -> f32 {
        use crate::scalar::emulate::emulate_mm256_reduce_add_ps;
        unsafe {
            use std::arch::x86_64::*;
            let mut n = this.len();
            let mut a = this.as_ptr();
            let mut x2 = _mm256_setzero_ps();
            while n >= 8 {
                let x = _mm256_loadu_ps(a);
                a = a.add(8);
                n -= 8;
                x2 = _mm256_fmadd_ps(x, x, x2);
            }
            if n >= 4 {
                let x = _mm256_zextps128_ps256(_mm_loadu_ps(a));
                a = a.add(4);
                n -= 4;
                x2 = _mm256_fmadd_ps(x, x, x2);
            }
            let mut x2 = emulate_mm256_reduce_add_ps(x2);
            // this hint is used to disable loop unrolling
            while std::hint::black_box(n) > 0 {
                let x = a.read();
                a = a.add(1);
                n -= 1;
                x2 += x * x;
            }
            x2
        }
    }

    #[cfg(all(target_arch = "x86_64", test))]
    #[test]
    fn reduce_sum_of_x2_v3_test() {
        use rand::Rng;
        const EPSILON: f32 = 0.006;
        detect::init();
        if !detect::v3::detect() {
            println!("test {} ... skipped (v3)", module_path!());
            return;
        }
        let mut rng = rand::thread_rng();
        for _ in 0..256 {
            let n = 4016;
            let this = (0..n)
                .map(|_| rng.gen_range(-1.0..=1.0))
                .collect::<Vec<_>>();
            for z in 3984..4016 {
                let this = &this[..z];
                let specialized = unsafe { reduce_sum_of_x2_v3(this) };
                let fallback = unsafe { reduce_sum_of_x2_fallback(this) };
                assert!(
                    (specialized - fallback).abs() < EPSILON,
                    "specialized = {specialized}, fallback = {fallback}."
                );
            }
        }
    }

    #[detect::multiversion(v4 = import, v3 = import, v2, neon, fallback = export)]
    pub fn reduce_sum_of_x2(this: &[f32]) -> f32 {
        let n = this.len();
        let mut x2 = 0.0f32;
        for i in 0..n {
            x2 += this[i] * this[i];
        }
        x2
    }
}

mod reduce_min_max_of_x {
    // Semanctics of `f32::min` is different from `_mm256_min_ps`,
    // which may lead to issues...

    #[cfg(target_arch = "x86_64")]
    #[detect::target_cpu(enable = "v4")]
    unsafe fn reduce_min_max_of_x_v4(this: &[f32]) -> (f32, f32) {
        unsafe {
            use std::arch::x86_64::*;
            let mut n = this.len();
            let mut a = this.as_ptr();
            let mut min = _mm512_set1_ps(f32::INFINITY);
            let mut max = _mm512_set1_ps(f32::NEG_INFINITY);
            while n >= 16 {
                let x = _mm512_loadu_ps(a);
                a = a.add(16);
                n -= 16;
                min = _mm512_min_ps(x, min);
                max = _mm512_max_ps(x, max);
            }
            if n > 0 {
                let mask = _bzhi_u32(0xffff, n as u32) as u16;
                let x = _mm512_maskz_loadu_ps(mask, a);
                min = _mm512_mask_min_ps(min, mask, x, min);
                max = _mm512_mask_max_ps(max, mask, x, max);
            }
            let min = _mm512_reduce_min_ps(min);
            let max = _mm512_reduce_max_ps(max);
            (min, max)
        }
    }

    #[cfg(all(target_arch = "x86_64", test))]
    #[test]
    fn reduce_min_max_of_x_v4_test() {
        use rand::Rng;
        detect::init();
        if !detect::v4::detect() {
            println!("test {} ... skipped (v4)", module_path!());
            return;
        }
        let mut rng = rand::thread_rng();
        for _ in 0..256 {
            let n = 200;
            let x = (0..n)
                .map(|_| rng.gen_range(-1.0..=1.0))
                .collect::<Vec<_>>();
            for z in 50..200 {
                let x = &x[..z];
                let specialized = unsafe { reduce_min_max_of_x_v4(x) };
                let fallback = unsafe { reduce_min_max_of_x_fallback(x) };
                assert_eq!(specialized.0, fallback.0);
                assert_eq!(specialized.1, fallback.1);
            }
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[detect::target_cpu(enable = "v3")]
    unsafe fn reduce_min_max_of_x_v3(this: &[f32]) -> (f32, f32) {
        use crate::scalar::emulate::emulate_mm256_reduce_max_ps;
        use crate::scalar::emulate::emulate_mm256_reduce_min_ps;
        unsafe {
            use std::arch::x86_64::*;
            let mut n = this.len();
            let mut a = this.as_ptr();
            let mut min = _mm256_set1_ps(f32::INFINITY);
            let mut max = _mm256_set1_ps(f32::NEG_INFINITY);
            while n >= 8 {
                let x = _mm256_loadu_ps(a);
                a = a.add(8);
                n -= 8;
                min = _mm256_min_ps(x, min);
                max = _mm256_max_ps(x, max);
            }
            let mut min = emulate_mm256_reduce_min_ps(min);
            let mut max = emulate_mm256_reduce_max_ps(max);
            // this hint is used to disable loop unrolling
            while std::hint::black_box(n) > 0 {
                let x = a.read();
                a = a.add(1);
                n -= 1;
                min = x.min(min);
                max = x.max(max);
            }
            (min, max)
        }
    }

    #[cfg(all(target_arch = "x86_64", test))]
    #[test]
    fn reduce_min_max_of_x_v3_test() {
        use rand::Rng;
        detect::init();
        if !detect::v3::detect() {
            println!("test {} ... skipped (v3)", module_path!());
            return;
        }
        let mut rng = rand::thread_rng();
        for _ in 0..256 {
            let n = 200;
            let x = (0..n)
                .map(|_| rng.gen_range(-1.0..=1.0))
                .collect::<Vec<_>>();
            for z in 50..200 {
                let x = &x[..z];
                let specialized = unsafe { reduce_min_max_of_x_v3(x) };
                let fallback = unsafe { reduce_min_max_of_x_fallback(x) };
                assert_eq!(specialized.0, fallback.0,);
                assert_eq!(specialized.1, fallback.1,);
            }
        }
    }

    #[detect::multiversion(v4 = import, v3 = import, v2, neon, fallback = export)]
    pub fn reduce_min_max_of_x(this: &[f32]) -> (f32, f32) {
        let mut min = f32::INFINITY;
        let mut max = f32::NEG_INFINITY;
        let n = this.len();
        for i in 0..n {
            min = min.min(this[i]);
            max = max.max(this[i]);
        }
        (min, max)
    }
}

mod reduce_sum_of_xy {
    #[cfg(target_arch = "x86_64")]
    #[detect::target_cpu(enable = "v4")]
    unsafe fn reduce_sum_of_xy_v4(lhs: &[f32], rhs: &[f32]) -> f32 {
        assert!(lhs.len() == rhs.len());
        unsafe {
            use std::arch::x86_64::*;
            let mut n = lhs.len();
            let mut a = lhs.as_ptr();
            let mut b = rhs.as_ptr();
            let mut xy = _mm512_setzero_ps();
            while n >= 16 {
                let x = _mm512_loadu_ps(a);
                let y = _mm512_loadu_ps(b);
                a = a.add(16);
                b = b.add(16);
                n -= 16;
                xy = _mm512_fmadd_ps(x, y, xy);
            }
            if n > 0 {
                let mask = _bzhi_u32(0xffff, n as u32) as u16;
                let x = _mm512_maskz_loadu_ps(mask, a);
                let y = _mm512_maskz_loadu_ps(mask, b);
                xy = _mm512_fmadd_ps(x, y, xy);
            }
            _mm512_reduce_add_ps(xy)
        }
    }

    #[cfg(all(target_arch = "x86_64", test))]
    #[test]
    fn reduce_sum_of_xy_v4_test() {
        use rand::Rng;
        const EPSILON: f32 = 0.004;
        detect::init();
        if !detect::v4::detect() {
            println!("test {} ... skipped (v4)", module_path!());
            return;
        }
        let mut rng = rand::thread_rng();
        for _ in 0..256 {
            let n = 4016;
            let lhs = (0..n)
                .map(|_| rng.gen_range(-1.0..=1.0))
                .collect::<Vec<_>>();
            let rhs = (0..n)
                .map(|_| rng.gen_range(-1.0..=1.0))
                .collect::<Vec<_>>();
            for z in 3984..4016 {
                let lhs = &lhs[..z];
                let rhs = &rhs[..z];
                let specialized = unsafe { reduce_sum_of_xy_v4(lhs, rhs) };
                let fallback = unsafe { reduce_sum_of_xy_fallback(lhs, rhs) };
                assert!(
                    (specialized - fallback).abs() < EPSILON,
                    "specialized = {specialized}, fallback = {fallback}."
                );
            }
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[detect::target_cpu(enable = "v3")]
    unsafe fn reduce_sum_of_xy_v3(lhs: &[f32], rhs: &[f32]) -> f32 {
        use crate::scalar::emulate::emulate_mm256_reduce_add_ps;
        assert!(lhs.len() == rhs.len());
        unsafe {
            use std::arch::x86_64::*;
            let mut n = lhs.len();
            let mut a = lhs.as_ptr();
            let mut b = rhs.as_ptr();
            let mut xy = _mm256_setzero_ps();
            while n >= 8 {
                let x = _mm256_loadu_ps(a);
                let y = _mm256_loadu_ps(b);
                a = a.add(8);
                b = b.add(8);
                n -= 8;
                xy = _mm256_fmadd_ps(x, y, xy);
            }
            if n >= 4 {
                let x = _mm256_zextps128_ps256(_mm_loadu_ps(a));
                let y = _mm256_zextps128_ps256(_mm_loadu_ps(b));
                a = a.add(4);
                b = b.add(4);
                n -= 4;
                xy = _mm256_fmadd_ps(x, y, xy);
            }
            let mut xy = emulate_mm256_reduce_add_ps(xy);
            // this hint is used to disable loop unrolling
            while std::hint::black_box(n) > 0 {
                let x = a.read();
                let y = b.read();
                a = a.add(1);
                b = b.add(1);
                n -= 1;
                xy += x * y;
            }
            xy
        }
    }

    #[cfg(all(target_arch = "x86_64", test))]
    #[test]
    fn reduce_sum_of_xy_v3_test() {
        use rand::Rng;
        const EPSILON: f32 = 0.004;
        detect::init();
        if !detect::v3::detect() {
            println!("test {} ... skipped (v3)", module_path!());
            return;
        }
        let mut rng = rand::thread_rng();
        for _ in 0..256 {
            let n = 4016;
            let lhs = (0..n)
                .map(|_| rng.gen_range(-1.0..=1.0))
                .collect::<Vec<_>>();
            let rhs = (0..n)
                .map(|_| rng.gen_range(-1.0..=1.0))
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

    #[detect::multiversion(v4 = import, v3 = import, v2, neon, fallback = export)]
    pub fn reduce_sum_of_xy(lhs: &[f32], rhs: &[f32]) -> f32 {
        assert!(lhs.len() == rhs.len());
        let n = lhs.len();
        let mut xy = 0.0f32;
        for i in 0..n {
            xy += lhs[i] * rhs[i];
        }
        xy
    }
}

mod reduce_sum_of_d2 {
    #[cfg(target_arch = "x86_64")]
    #[detect::target_cpu(enable = "v4")]
    unsafe fn reduce_sum_of_d2_v4(lhs: &[f32], rhs: &[f32]) -> f32 {
        assert!(lhs.len() == rhs.len());
        unsafe {
            use std::arch::x86_64::*;
            let mut n = lhs.len() as u32;
            let mut a = lhs.as_ptr();
            let mut b = rhs.as_ptr();
            let mut d2 = _mm512_setzero_ps();
            while n >= 16 {
                let x = _mm512_loadu_ps(a);
                let y = _mm512_loadu_ps(b);
                a = a.add(16);
                b = b.add(16);
                n -= 16;
                let d = _mm512_sub_ps(x, y);
                d2 = _mm512_fmadd_ps(d, d, d2);
            }
            if n > 0 {
                let mask = _bzhi_u32(0xffff, n) as u16;
                let x = _mm512_maskz_loadu_ps(mask, a);
                let y = _mm512_maskz_loadu_ps(mask, b);
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
        const EPSILON: f32 = 0.02;
        detect::init();
        if !detect::v4::detect() {
            println!("test {} ... skipped (v4)", module_path!());
            return;
        }
        let mut rng = rand::thread_rng();
        for _ in 0..256 {
            let n = 4016;
            let lhs = (0..n)
                .map(|_| rng.gen_range(-1.0..=1.0))
                .collect::<Vec<_>>();
            let rhs = (0..n)
                .map(|_| rng.gen_range(-1.0..=1.0))
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
    unsafe fn reduce_sum_of_d2_v3(lhs: &[f32], rhs: &[f32]) -> f32 {
        use crate::scalar::emulate::emulate_mm256_reduce_add_ps;
        assert!(lhs.len() == rhs.len());
        unsafe {
            use std::arch::x86_64::*;
            let mut n = lhs.len();
            let mut a = lhs.as_ptr();
            let mut b = rhs.as_ptr();
            let mut d2 = _mm256_setzero_ps();
            while n >= 8 {
                let x = _mm256_loadu_ps(a);
                let y = _mm256_loadu_ps(b);
                a = a.add(8);
                b = b.add(8);
                n -= 8;
                let d = _mm256_sub_ps(x, y);
                d2 = _mm256_fmadd_ps(d, d, d2);
            }
            if n >= 4 {
                let x = _mm256_zextps128_ps256(_mm_loadu_ps(a));
                let y = _mm256_zextps128_ps256(_mm_loadu_ps(b));
                a = a.add(4);
                b = b.add(4);
                n -= 4;
                let d = _mm256_sub_ps(x, y);
                d2 = _mm256_fmadd_ps(d, d, d2);
            }
            let mut d2 = emulate_mm256_reduce_add_ps(d2);
            // this hint is used to disable loop unrolling
            while std::hint::black_box(n) > 0 {
                let x = a.read();
                let y = b.read();
                a = a.add(1);
                b = b.add(1);
                n -= 1;
                let d = x - y;
                d2 += d * d;
            }
            d2
        }
    }

    #[cfg(all(target_arch = "x86_64", test))]
    #[test]
    fn reduce_sum_of_d2_v3_test() {
        use rand::Rng;
        const EPSILON: f32 = 0.02;
        detect::init();
        if !detect::v3::detect() {
            println!("test {} ... skipped (v3)", module_path!());
            return;
        }
        let mut rng = rand::thread_rng();
        for _ in 0..256 {
            let n = 4016;
            let lhs = (0..n)
                .map(|_| rng.gen_range(-1.0..=1.0))
                .collect::<Vec<_>>();
            let rhs = (0..n)
                .map(|_| rng.gen_range(-1.0..=1.0))
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

    #[detect::multiversion(v4 = import, v3 = import, v2, neon, fallback = export)]
    pub fn reduce_sum_of_d2(lhs: &[f32], rhs: &[f32]) -> f32 {
        assert!(lhs.len() == rhs.len());
        let n = lhs.len();
        let mut d2 = 0.0f32;
        for i in 0..n {
            let d = lhs[i] - rhs[i];
            d2 += d * d;
        }
        d2
    }
}

mod reduce_sum_of_sparse_xy {
    #[inline]
    #[cfg(target_arch = "x86_64")]
    #[detect::target_cpu(enable = "v4")]
    unsafe fn reduce_sum_of_sparse_xy_v4(li: &[u32], lv: &[f32], ri: &[u32], rv: &[f32]) -> f32 {
        use crate::scalar::emulate::emulate_mm512_2intersect_epi32;
        assert_eq!(li.len(), lv.len());
        assert_eq!(ri.len(), rv.len());
        let (mut lp, ln) = (0, li.len());
        let (mut rp, rn) = (0, ri.len());
        let (li, lv) = (li.as_ptr(), lv.as_ptr());
        let (ri, rv) = (ri.as_ptr(), rv.as_ptr());
        unsafe {
            use std::arch::x86_64::*;
            let mut xy = _mm512_setzero_ps();
            while lp + 16 <= ln && rp + 16 <= rn {
                let lx = _mm512_loadu_epi32(li.add(lp).cast());
                let rx = _mm512_loadu_epi32(ri.add(rp).cast());
                let (lk, rk) = emulate_mm512_2intersect_epi32(lx, rx);
                let lv = _mm512_maskz_compress_ps(lk, _mm512_loadu_ps(lv.add(lp)));
                let rv = _mm512_maskz_compress_ps(rk, _mm512_loadu_ps(rv.add(rp)));
                xy = _mm512_fmadd_ps(lv, rv, xy);
                let lt = li.add(lp + 16 - 1).read();
                let rt = ri.add(rp + 16 - 1).read();
                lp += (lt <= rt) as usize * 16;
                rp += (lt >= rt) as usize * 16;
            }
            while lp < ln && rp < rn {
                let lw = 16.min(ln - lp);
                let rw = 16.min(rn - rp);
                let lm = _bzhi_u32(0xffff, lw as _) as u16;
                let rm = _bzhi_u32(0xffff, rw as _) as u16;
                let lx = _mm512_mask_loadu_epi32(_mm512_set1_epi32(-1), lm, li.add(lp).cast());
                let rx = _mm512_mask_loadu_epi32(_mm512_set1_epi32(-1), rm, ri.add(rp).cast());
                let (lk, rk) = emulate_mm512_2intersect_epi32(lx, rx);
                let lv = _mm512_maskz_compress_ps(lk, _mm512_maskz_loadu_ps(lm, lv.add(lp)));
                let rv = _mm512_maskz_compress_ps(rk, _mm512_maskz_loadu_ps(rm, rv.add(rp)));
                xy = _mm512_fmadd_ps(lv, rv, xy);
                let lt = li.add(lp + lw - 1).read();
                let rt = ri.add(rp + rw - 1).read();
                lp += (lt <= rt) as usize * lw;
                rp += (lt >= rt) as usize * rw;
            }
            _mm512_reduce_add_ps(xy)
        }
    }

    #[cfg(all(target_arch = "x86_64", test))]
    #[test]
    fn reduce_sum_of_sparse_xy_v4_test() {
        use rand::Rng;
        const EPSILON: f32 = 0.000001;
        detect::init();
        if !detect::v4::detect() {
            println!("test {} ... skipped (v4)", module_path!());
            return;
        }
        let mut rng = rand::thread_rng();
        for _ in 0..256 {
            let lm = 300;
            let lidx = crate::rand::sample_u32_sorted(&mut rng, 10000, lm);
            let lval = (0..lm)
                .map(|_| rng.gen_range(-1.0..=1.0))
                .collect::<Vec<_>>();
            let rm = 350;
            let ridx = crate::rand::sample_u32_sorted(&mut rng, 10000, rm);
            let rval = (0..rm)
                .map(|_| rng.gen_range(-1.0..=1.0))
                .collect::<Vec<_>>();
            let specialized = unsafe { reduce_sum_of_sparse_xy_v4(&lidx, &lval, &ridx, &rval) };
            let fallback = unsafe { reduce_sum_of_sparse_xy_fallback(&lidx, &lval, &ridx, &rval) };
            assert!(
                (specialized - fallback).abs() < EPSILON,
                "specialized = {specialized}, fallback = {fallback}."
            );
        }
    }

    #[detect::multiversion(v4 = import, v3, v2, neon, fallback = export)]
    pub fn reduce_sum_of_sparse_xy(lidx: &[u32], lval: &[f32], ridx: &[u32], rval: &[f32]) -> f32 {
        use std::cmp::Ordering;
        assert_eq!(lidx.len(), lval.len());
        assert_eq!(ridx.len(), rval.len());
        let (mut lp, ln) = (0, lidx.len());
        let (mut rp, rn) = (0, ridx.len());
        let mut xy = 0.0f32;
        while lp < ln && rp < rn {
            match Ord::cmp(&lidx[lp], &ridx[rp]) {
                Ordering::Equal => {
                    xy += lval[lp] * rval[rp];
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
    #[inline]
    #[cfg(target_arch = "x86_64")]
    #[detect::target_cpu(enable = "v4")]
    unsafe fn reduce_sum_of_sparse_d2_v4(li: &[u32], lv: &[f32], ri: &[u32], rv: &[f32]) -> f32 {
        use crate::scalar::emulate::emulate_mm512_2intersect_epi32;
        assert_eq!(li.len(), lv.len());
        assert_eq!(ri.len(), rv.len());
        let (mut lp, ln) = (0, li.len());
        let (mut rp, rn) = (0, ri.len());
        let (li, lv) = (li.as_ptr(), lv.as_ptr());
        let (ri, rv) = (ri.as_ptr(), rv.as_ptr());
        unsafe {
            use std::arch::x86_64::*;
            let mut d2 = _mm512_setzero_ps();
            while lp + 16 <= ln && rp + 16 <= rn {
                let lx = _mm512_loadu_epi32(li.add(lp).cast());
                let rx = _mm512_loadu_epi32(ri.add(rp).cast());
                let (lk, rk) = emulate_mm512_2intersect_epi32(lx, rx);
                let lv = _mm512_maskz_compress_ps(lk, _mm512_loadu_ps(lv.add(lp)));
                let rv = _mm512_maskz_compress_ps(rk, _mm512_loadu_ps(rv.add(rp)));
                let d = _mm512_sub_ps(lv, rv);
                d2 = _mm512_fmadd_ps(d, d, d2);
                d2 = _mm512_sub_ps(d2, _mm512_mul_ps(lv, lv));
                d2 = _mm512_sub_ps(d2, _mm512_mul_ps(rv, rv));
                let lt = li.add(lp + 16 - 1).read();
                let rt = ri.add(rp + 16 - 1).read();
                lp += (lt <= rt) as usize * 16;
                rp += (lt >= rt) as usize * 16;
            }
            while lp < ln && rp < rn {
                let lw = 16.min(ln - lp);
                let rw = 16.min(rn - rp);
                let lm = _bzhi_u32(0xffff, lw as _) as u16;
                let rm = _bzhi_u32(0xffff, rw as _) as u16;
                let lx = _mm512_mask_loadu_epi32(_mm512_set1_epi32(-1), lm, li.add(lp).cast());
                let rx = _mm512_mask_loadu_epi32(_mm512_set1_epi32(-1), rm, ri.add(rp).cast());
                let (lk, rk) = emulate_mm512_2intersect_epi32(lx, rx);
                let lv = _mm512_maskz_compress_ps(lk, _mm512_maskz_loadu_ps(lm, lv.add(lp)));
                let rv = _mm512_maskz_compress_ps(rk, _mm512_maskz_loadu_ps(rm, rv.add(rp)));
                let d = _mm512_sub_ps(lv, rv);
                d2 = _mm512_fmadd_ps(d, d, d2);
                d2 = _mm512_sub_ps(d2, _mm512_mul_ps(lv, lv));
                d2 = _mm512_sub_ps(d2, _mm512_mul_ps(rv, rv));
                let lt = li.add(lp + lw - 1).read();
                let rt = ri.add(rp + rw - 1).read();
                lp += (lt <= rt) as usize * lw;
                rp += (lt >= rt) as usize * rw;
            }
            {
                let mut lp = 0;
                while lp + 16 <= ln {
                    let d = _mm512_loadu_ps(lv.add(lp));
                    d2 = _mm512_fmadd_ps(d, d, d2);
                    lp += 16;
                }
                if lp < ln {
                    let lw = ln - lp;
                    let lm = _bzhi_u32(0xffff, lw as _) as u16;
                    let d = _mm512_maskz_loadu_ps(lm, lv.add(lp));
                    d2 = _mm512_fmadd_ps(d, d, d2);
                }
            }
            {
                let mut rp = 0;
                while rp + 16 <= rn {
                    let d = _mm512_loadu_ps(rv.add(rp));
                    d2 = _mm512_fmadd_ps(d, d, d2);
                    rp += 16;
                }
                if rp < rn {
                    let rw = rn - rp;
                    let rm = _bzhi_u32(0xffff, rw as _) as u16;
                    let d = _mm512_maskz_loadu_ps(rm, rv.add(rp));
                    d2 = _mm512_fmadd_ps(d, d, d2);
                }
            }
            _mm512_reduce_add_ps(d2)
        }
    }

    #[cfg(all(target_arch = "x86_64", test))]
    #[test]
    fn reduce_sum_of_sparse_d2_v4_test() {
        use rand::Rng;
        const EPSILON: f32 = 0.0004;
        detect::init();
        if !detect::v4::detect() {
            println!("test {} ... skipped (v4)", module_path!());
            return;
        }
        let mut rng = rand::thread_rng();
        for _ in 0..256 {
            let lm = 300;
            let lidx = crate::rand::sample_u32_sorted(&mut rng, 10000, lm);
            let lval = (0..lm)
                .map(|_| rng.gen_range(-1.0..=1.0))
                .collect::<Vec<_>>();
            let rm = 350;
            let ridx = crate::rand::sample_u32_sorted(&mut rng, 10000, rm);
            let rval = (0..rm)
                .map(|_| rng.gen_range(-1.0..=1.0))
                .collect::<Vec<_>>();
            let specialized = unsafe { reduce_sum_of_sparse_d2_v4(&lidx, &lval, &ridx, &rval) };
            let fallback = unsafe { reduce_sum_of_sparse_d2_fallback(&lidx, &lval, &ridx, &rval) };
            assert!(
                (specialized - fallback).abs() < EPSILON,
                "specialized = {specialized}, fallback = {fallback}."
            );
        }
    }

    #[detect::multiversion(v4 = import, v3, v2, neon, fallback = export)]
    pub fn reduce_sum_of_sparse_d2(lidx: &[u32], lval: &[f32], ridx: &[u32], rval: &[f32]) -> f32 {
        use std::cmp::Ordering;
        assert_eq!(lidx.len(), lval.len());
        assert_eq!(ridx.len(), rval.len());
        let (mut lp, ln) = (0, lidx.len());
        let (mut rp, rn) = (0, ridx.len());
        let mut d2 = 0.0f32;
        while lp < ln && rp < rn {
            match Ord::cmp(&lidx[lp], &ridx[rp]) {
                Ordering::Equal => {
                    let d = lval[lp] - rval[rp];
                    d2 += d * d;
                    lp += 1;
                    rp += 1;
                }
                Ordering::Less => {
                    d2 += lval[lp] * lval[lp];
                    lp += 1;
                }
                Ordering::Greater => {
                    d2 += rval[rp] * rval[rp];
                    rp += 1;
                }
            }
        }
        for i in lp..ln {
            d2 += lval[i] * lval[i];
        }
        for i in rp..rn {
            d2 += rval[i] * rval[i];
        }
        d2
    }
}
