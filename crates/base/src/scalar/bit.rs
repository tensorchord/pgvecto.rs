#[inline(always)]
pub fn sum_of_and(lhs: &[u64], rhs: &[u64]) -> u32 {
    sum_of_and::sum_of_and(lhs, rhs)
}

mod sum_of_and {
    #[inline]
    #[cfg(target_arch = "x86_64")]
    #[detect::target_cpu(enable = "v4_avx512vpopcntdq")]
    unsafe fn sum_of_and_v4_avx512vpopcntdq(lhs: &[u64], rhs: &[u64]) -> u32 {
        assert!(lhs.len() == rhs.len());
        unsafe {
            use std::arch::x86_64::*;
            let mut and = _mm512_setzero_si512();
            let mut a = lhs.as_ptr();
            let mut b = rhs.as_ptr();
            let mut n = lhs.len();
            while n >= 8 {
                let x = _mm512_loadu_si512(a.cast());
                let y = _mm512_loadu_si512(b.cast());
                a = a.add(8);
                b = b.add(8);
                n -= 8;
                and = _mm512_add_epi64(and, _mm512_popcnt_epi64(_mm512_and_si512(x, y)));
            }
            if n > 0 {
                let mask = _bzhi_u32(0xff, n as u32) as u8;
                let x = _mm512_maskz_loadu_epi64(mask, a.cast());
                let y = _mm512_maskz_loadu_epi64(mask, b.cast());
                and = _mm512_add_epi64(and, _mm512_popcnt_epi64(_mm512_and_si512(x, y)));
            }
            _mm512_reduce_add_epi64(and) as u32
        }
    }

    #[cfg(all(target_arch = "x86_64", test))]
    #[test]
    fn sum_of_and_v4_avx512vpopcntdq_test() {
        detect::init();
        if !detect::v4_avx512vpopcntdq::detect() {
            println!("test {} ... skipped (v4_avx512vpopcntdq)", module_path!());
            return;
        }
        for _ in 0..300 {
            let lhs = (0..126).map(|_| rand::random::<u64>()).collect::<Vec<_>>();
            let rhs = (0..126).map(|_| rand::random::<u64>()).collect::<Vec<_>>();
            let specialized = unsafe { sum_of_and_v4_avx512vpopcntdq(&lhs, &rhs) };
            let fallback = unsafe { sum_of_and_fallback(&lhs, &rhs) };
            assert_eq!(specialized, fallback);
        }
    }

    #[detect::multiversion(v4_avx512vpopcntdq = import, v4, v3, v2, neon, fallback = export)]
    pub fn sum_of_and(lhs: &[u64], rhs: &[u64]) -> u32 {
        assert_eq!(lhs.len(), rhs.len());
        let n = lhs.len();
        let mut and = 0;
        for i in 0..n {
            and += (lhs[i] & rhs[i]).count_ones();
        }
        and
    }
}

#[inline(always)]
pub fn sum_of_or(lhs: &[u64], rhs: &[u64]) -> u32 {
    sum_of_or::sum_of_or(lhs, rhs)
}

mod sum_of_or {
    #[inline]
    #[cfg(target_arch = "x86_64")]
    #[detect::target_cpu(enable = "v4_avx512vpopcntdq")]
    unsafe fn sum_of_or_v4_avx512vpopcntdq(lhs: &[u64], rhs: &[u64]) -> u32 {
        assert!(lhs.len() == rhs.len());
        unsafe {
            use std::arch::x86_64::*;
            let mut or = _mm512_setzero_si512();
            let mut a = lhs.as_ptr();
            let mut b = rhs.as_ptr();
            let mut n = lhs.len();
            while n >= 8 {
                let x = _mm512_loadu_si512(a.cast());
                let y = _mm512_loadu_si512(b.cast());
                a = a.add(8);
                b = b.add(8);
                n -= 8;
                or = _mm512_add_epi64(or, _mm512_popcnt_epi64(_mm512_or_si512(x, y)));
            }
            if n > 0 {
                let mask = _bzhi_u32(0xff, n as u32) as u8;
                let x = _mm512_maskz_loadu_epi64(mask, a.cast());
                let y = _mm512_maskz_loadu_epi64(mask, b.cast());
                or = _mm512_add_epi64(or, _mm512_popcnt_epi64(_mm512_or_si512(x, y)));
            }
            _mm512_reduce_add_epi64(or) as u32
        }
    }

    #[cfg(all(target_arch = "x86_64", test))]
    #[test]
    fn sum_of_or_v4_avx512vpopcntdq_test() {
        detect::init();
        if !detect::v4_avx512vpopcntdq::detect() {
            println!("test {} ... skipped (v4_avx512vpopcntdq)", module_path!());
            return;
        }
        for _ in 0..300 {
            let lhs = (0..126).map(|_| rand::random::<u64>()).collect::<Vec<_>>();
            let rhs = (0..126).map(|_| rand::random::<u64>()).collect::<Vec<_>>();
            let specialized = unsafe { sum_of_or_v4_avx512vpopcntdq(&lhs, &rhs) };
            let fallback = unsafe { sum_of_or_fallback(&lhs, &rhs) };
            assert_eq!(specialized, fallback);
        }
    }

    #[detect::multiversion(v4_avx512vpopcntdq = import, v4, v3, v2, neon, fallback = export)]
    pub fn sum_of_or(lhs: &[u64], rhs: &[u64]) -> u32 {
        assert_eq!(lhs.len(), rhs.len());
        let n = lhs.len();
        let mut or = 0;
        for i in 0..n {
            or += (lhs[i] | rhs[i]).count_ones();
        }
        or
    }
}

#[inline(always)]
pub fn sum_of_xor(lhs: &[u64], rhs: &[u64]) -> u32 {
    sum_of_xor::sum_of_xor(lhs, rhs)
}

mod sum_of_xor {
    #[inline]
    #[cfg(target_arch = "x86_64")]
    #[detect::target_cpu(enable = "v4_avx512vpopcntdq")]
    unsafe fn sum_of_xor_v4_avx512vpopcntdq(lhs: &[u64], rhs: &[u64]) -> u32 {
        assert!(lhs.len() == rhs.len());
        unsafe {
            use std::arch::x86_64::*;
            let mut xor = _mm512_setzero_si512();
            let mut a = lhs.as_ptr();
            let mut b = rhs.as_ptr();
            let mut n = lhs.len();
            while n >= 8 {
                let x = _mm512_loadu_si512(a.cast());
                let y = _mm512_loadu_si512(b.cast());
                a = a.add(8);
                b = b.add(8);
                n -= 8;
                xor = _mm512_add_epi64(xor, _mm512_popcnt_epi64(_mm512_xor_si512(x, y)));
            }
            if n > 0 {
                let mask = _bzhi_u32(0xff, n as u32) as u8;
                let x = _mm512_maskz_loadu_epi64(mask, a.cast());
                let y = _mm512_maskz_loadu_epi64(mask, b.cast());
                xor = _mm512_add_epi64(xor, _mm512_popcnt_epi64(_mm512_xor_si512(x, y)));
            }
            _mm512_reduce_add_epi64(xor) as u32
        }
    }

    #[cfg(all(target_arch = "x86_64", test))]
    #[test]
    fn sum_of_xor_v4_avx512vpopcntdq_test() {
        detect::init();
        if !detect::v4_avx512vpopcntdq::detect() {
            println!("test {} ... skipped (v4_avx512vpopcntdq)", module_path!());
            return;
        }
        for _ in 0..300 {
            let lhs = (0..126).map(|_| rand::random::<u64>()).collect::<Vec<_>>();
            let rhs = (0..126).map(|_| rand::random::<u64>()).collect::<Vec<_>>();
            let specialized = unsafe { sum_of_xor_v4_avx512vpopcntdq(&lhs, &rhs) };
            let fallback = unsafe { sum_of_xor_fallback(&lhs, &rhs) };
            assert_eq!(specialized, fallback);
        }
    }

    #[detect::multiversion(v4_avx512vpopcntdq = import, v4, v3, v2, neon, fallback = export)]
    pub fn sum_of_xor(lhs: &[u64], rhs: &[u64]) -> u32 {
        assert_eq!(lhs.len(), rhs.len());
        let n = lhs.len();
        let mut xor = 0;
        for i in 0..n {
            xor += (lhs[i] ^ rhs[i]).count_ones();
        }
        xor
    }
}

#[inline(always)]
pub fn sum_of_and_or(lhs: &[u64], rhs: &[u64]) -> (u32, u32) {
    sum_of_and_or::sum_of_and_or(lhs, rhs)
}

mod sum_of_and_or {
    #[inline]
    #[cfg(target_arch = "x86_64")]
    #[detect::target_cpu(enable = "v4_avx512vpopcntdq")]
    unsafe fn sum_of_and_or_v4_avx512vpopcntdq(lhs: &[u64], rhs: &[u64]) -> (u32, u32) {
        assert!(lhs.len() == rhs.len());
        unsafe {
            use std::arch::x86_64::*;
            let mut and = _mm512_setzero_si512();
            let mut or = _mm512_setzero_si512();
            let mut a = lhs.as_ptr();
            let mut b = rhs.as_ptr();
            let mut n = lhs.len();
            while n >= 8 {
                let x = _mm512_loadu_si512(a.cast());
                let y = _mm512_loadu_si512(b.cast());
                a = a.add(8);
                b = b.add(8);
                n -= 8;
                and = _mm512_add_epi64(and, _mm512_popcnt_epi64(_mm512_and_si512(x, y)));
                or = _mm512_add_epi64(or, _mm512_popcnt_epi64(_mm512_or_si512(x, y)));
            }
            if n > 0 {
                let mask = _bzhi_u32(0xff, n as u32) as u8;
                let x = _mm512_maskz_loadu_epi64(mask, a.cast());
                let y = _mm512_maskz_loadu_epi64(mask, b.cast());
                and = _mm512_add_epi64(and, _mm512_popcnt_epi64(_mm512_and_si512(x, y)));
                or = _mm512_add_epi64(or, _mm512_popcnt_epi64(_mm512_or_si512(x, y)));
            }
            (
                _mm512_reduce_add_epi64(and) as u32,
                _mm512_reduce_add_epi64(or) as u32,
            )
        }
    }

    #[cfg(all(target_arch = "x86_64", test))]
    #[test]
    fn sum_of_xor_v4_avx512vpopcntdq_test() {
        detect::init();
        if !detect::v4_avx512vpopcntdq::detect() {
            println!("test {} ... skipped (v4_avx512vpopcntdq)", module_path!());
            return;
        }
        for _ in 0..300 {
            let lhs = (0..126).map(|_| rand::random::<u64>()).collect::<Vec<_>>();
            let rhs = (0..126).map(|_| rand::random::<u64>()).collect::<Vec<_>>();
            let specialized = unsafe { sum_of_and_or_v4_avx512vpopcntdq(&lhs, &rhs) };
            let fallback = unsafe { sum_of_and_or_fallback(&lhs, &rhs) };
            assert_eq!(specialized, fallback);
        }
    }

    #[detect::multiversion(v4_avx512vpopcntdq = import, v4, v3, v2, neon, fallback = export)]
    pub fn sum_of_and_or(lhs: &[u64], rhs: &[u64]) -> (u32, u32) {
        assert_eq!(lhs.len(), rhs.len());
        let n = lhs.len();
        let mut and = 0;
        let mut or = 0;
        for i in 0..n {
            and += (lhs[i] & rhs[i]).count_ones();
            or += (lhs[i] | rhs[i]).count_ones();
        }
        (and, or)
    }
}

#[inline(always)]
pub fn sum_of_x(this: &[u64]) -> u32 {
    sum_of_x::sum_of_x(this)
}

mod sum_of_x {
    #[inline]
    #[cfg(target_arch = "x86_64")]
    #[detect::target_cpu(enable = "v4_avx512vpopcntdq")]
    unsafe fn sum_of_x_v4_avx512vpopcntdq(this: &[u64]) -> u32 {
        unsafe {
            use std::arch::x86_64::*;
            let mut and = _mm512_setzero_si512();
            let mut a = this.as_ptr();
            let mut n = this.len();
            while n >= 8 {
                let x = _mm512_loadu_si512(a.cast());
                a = a.add(8);
                n -= 8;
                and = _mm512_add_epi64(and, _mm512_popcnt_epi64(x));
            }
            if n > 0 {
                let mask = _bzhi_u32(0xff, n as u32) as u8;
                let x = _mm512_maskz_loadu_epi64(mask, a.cast());
                and = _mm512_add_epi64(and, _mm512_popcnt_epi64(x));
            }
            _mm512_reduce_add_epi64(and) as u32
        }
    }

    #[cfg(all(target_arch = "x86_64", test))]
    #[test]
    fn sum_of_x_v4_avx512vpopcntdq_test() {
        detect::init();
        if !detect::v4_avx512vpopcntdq::detect() {
            println!("test {} ... skipped (v4_avx512vpopcntdq)", module_path!());
            return;
        }
        for _ in 0..300 {
            let this = (0..126).map(|_| rand::random::<u64>()).collect::<Vec<_>>();
            let specialized = unsafe { sum_of_x_v4_avx512vpopcntdq(&this) };
            let fallback = unsafe { sum_of_x_fallback(&this) };
            assert_eq!(specialized, fallback);
        }
    }

    #[detect::multiversion(v4_avx512vpopcntdq = import, v4, v3, v2, neon, fallback = export)]
    pub fn sum_of_x(this: &[u64]) -> u32 {
        let n = this.len();
        let mut and = 0;
        for i in 0..n {
            and += this[i].count_ones();
        }
        and
    }
}

#[detect::multiversion(v4, v3, v2, neon, fallback)]
pub fn vector_and(lhs: &[u64], rhs: &[u64]) -> Vec<u64> {
    assert_eq!(lhs.len(), rhs.len());
    let n = lhs.len();
    let mut r = Vec::<u64>::with_capacity(n);
    for i in 0..n {
        unsafe {
            r.as_mut_ptr().add(i).write(lhs[i] & rhs[i]);
        }
    }
    unsafe {
        r.set_len(n);
    }
    r
}

#[detect::multiversion(v4, v3, v2, neon, fallback)]
pub fn vector_or(lhs: &[u64], rhs: &[u64]) -> Vec<u64> {
    assert_eq!(lhs.len(), rhs.len());
    let n = lhs.len();
    let mut r = Vec::<u64>::with_capacity(n);
    for i in 0..n {
        unsafe {
            r.as_mut_ptr().add(i).write(lhs[i] | rhs[i]);
        }
    }
    unsafe {
        r.set_len(n);
    }
    r
}

#[detect::multiversion(v4, v3, v2, neon, fallback)]
pub fn vector_xor(lhs: &[u64], rhs: &[u64]) -> Vec<u64> {
    assert_eq!(lhs.len(), rhs.len());
    let n = lhs.len();
    let mut r = Vec::<u64>::with_capacity(n);
    for i in 0..n {
        unsafe {
            r.as_mut_ptr().add(i).write(lhs[i] ^ rhs[i]);
        }
    }
    unsafe {
        r.set_len(n);
    }
    r
}
