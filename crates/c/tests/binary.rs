#![cfg(target_arch = "x86_64")]

fn count_ones(mut x: usize) -> usize {
    let mut r = 0;
    while x != 0 {
        r += x & 1;
        x >>= 1;
    }
    r
}

#[test]
fn test_v_binary_cosine() {
    detect::initialize();
    const EPSILON: f32 = f32::EPSILON;
    unsafe fn v_binary_cosine(a: *const usize, b: *const usize, n: usize) -> f32 {
        let mut xy = 0.0f32;
        let mut xx = 0.0f32;
        let mut yy = 0.0f32;
        for i in 0..n {
            let x = a.add(i).read();
            let y = b.add(i).read();
            xy += count_ones(x & y) as f32;
            xx += count_ones(x) as f32;
            yy += count_ones(y) as f32;
        }
        xy / (xx * yy).sqrt()
    }
    let n = 4000;
    let a = (0..n).map(|_| rand::random::<usize>()).collect::<Vec<_>>();
    let b = (0..n).map(|_| rand::random::<usize>()).collect::<Vec<_>>();
    let r = unsafe { v_binary_cosine(a.as_ptr().cast(), b.as_ptr().cast(), n) };
    if detect::x86_64::detect_avx512vpopcntdq() {
        println!("detected avx512vpopcntdq");
        let c =
            unsafe { c::v_binary_cosine_avx512vpopcntdq(a.as_ptr().cast(), b.as_ptr().cast(), n) };
        assert!((c - r).abs() < EPSILON, "c = {c}, r = {r}.");
    } else {
        println!("detected no avx512vpopcntdq, skipped");
    }
    if detect::x86_64::detect_v4() {
        println!("detected v4");
        let c = unsafe { c::v_binary_cosine_v4(a.as_ptr().cast(), b.as_ptr().cast(), n) };
        assert!((c - r).abs() < EPSILON, "c = {c}, r = {r}.");
    } else {
        println!("detected no v4, skipped");
    }
    if detect::x86_64::detect_v3() {
        println!("detected v3");
        let c = unsafe { c::v_binary_cosine_v3(a.as_ptr().cast(), b.as_ptr().cast(), n) };
        assert!((c - r).abs() < EPSILON, "c = {c}, r = {r}.");
    } else {
        println!("detected no v3, skipped");
    }
}

#[test]
fn test_v_binary_dot() {
    detect::initialize();
    const EPSILON: f32 = f32::EPSILON;
    unsafe fn v_binary_dot(a: *const usize, b: *const usize, n: usize) -> f32 {
        let mut xy = 0.0f32;
        for i in 0..n {
            let x = a.add(i).read();
            let y = b.add(i).read();
            xy += count_ones(x & y) as f32;
        }
        xy
    }
    let n = 4000;
    let a = (0..n).map(|_| rand::random::<usize>()).collect::<Vec<_>>();
    let b = (0..n).map(|_| rand::random::<usize>()).collect::<Vec<_>>();
    let r = unsafe { v_binary_dot(a.as_ptr().cast(), b.as_ptr().cast(), n) };
    if detect::x86_64::detect_avx512vpopcntdq() {
        println!("detected avx512vpopcntdq");
        let c = unsafe { c::v_binary_dot_avx512vpopcntdq(a.as_ptr().cast(), b.as_ptr().cast(), n) };
        assert!((c - r).abs() < EPSILON, "c = {c}, r = {r}.");
    } else {
        println!("detected no avx512vpopcntdq, skipped");
    }
    if detect::x86_64::detect_v4() {
        println!("detected v4");
        let c = unsafe { c::v_binary_dot_v4(a.as_ptr().cast(), b.as_ptr().cast(), n) };
        assert!((c - r).abs() < EPSILON, "c = {c}, r = {r}.");
    } else {
        println!("detected no v4, skipped");
    }
    if detect::x86_64::detect_v3() {
        println!("detected v3");
        let c = unsafe { c::v_binary_dot_v3(a.as_ptr().cast(), b.as_ptr().cast(), n) };
        assert!((c - r).abs() < EPSILON, "c = {c}, r = {r}.");
    } else {
        println!("detected no v3, skipped");
    }
}

#[test]
fn test_v_binary_sl2() {
    detect::initialize();
    const EPSILON: f32 = f32::EPSILON;
    unsafe fn v_binary_sl2(a: *const usize, b: *const usize, n: usize) -> f32 {
        let mut dd = 0.0f32;
        for i in 0..n {
            let x = a.add(i).read();
            let y = b.add(i).read();
            dd += count_ones(x ^ y) as f32;
        }
        dd
    }
    let n = 4000;
    let a = (0..n).map(|_| rand::random::<usize>()).collect::<Vec<_>>();
    let b = (0..n).map(|_| rand::random::<usize>()).collect::<Vec<_>>();
    let r = unsafe { v_binary_sl2(a.as_ptr().cast(), b.as_ptr().cast(), n) };
    if detect::x86_64::detect_avx512vpopcntdq() {
        println!("detected avx512vpopcntdq");
        let c = unsafe { c::v_binary_sl2_avx512vpopcntdq(a.as_ptr().cast(), b.as_ptr().cast(), n) };
        assert!((c - r).abs() < EPSILON, "c = {c}, r = {r}.");
    } else {
        println!("detected no avx512vpopcntdq, skipped");
    }
    if detect::x86_64::detect_v4() {
        println!("detected v4");
        let c = unsafe { c::v_binary_sl2_v4(a.as_ptr().cast(), b.as_ptr().cast(), n) };
        assert!((c - r).abs() < EPSILON, "c = {c}, r = {r}.");
    } else {
        println!("detected no v4, skipped");
    }
    if detect::x86_64::detect_v3() {
        println!("detected v3");
        let c = unsafe { c::v_binary_sl2_v3(a.as_ptr().cast(), b.as_ptr().cast(), n) };
        assert!((c - r).abs() < EPSILON, "c = {c}, r = {r}.");
    } else {
        println!("detected no v3, skipped");
    }
}

#[test]
fn test_v_binary_cnt() {
    detect::initialize();
    const EPSILON: f32 = f32::EPSILON;
    unsafe fn v_binary_cnt(a: *const usize, n: usize) -> f32 {
        let mut cnt = 0.0f32;
        for i in 0..n {
            let x = a.add(i).read();
            cnt += count_ones(x) as f32;
        }
        cnt
    }
    let n = 4000;
    let a = (0..n).map(|_| rand::random::<usize>()).collect::<Vec<_>>();
    let r = unsafe { v_binary_cnt(a.as_ptr().cast(), n) };
    if detect::x86_64::detect_avx512vpopcntdq() {
        println!("detected avx512vpopcntdq");
        let c = unsafe { c::v_binary_cnt_avx512vpopcntdq(a.as_ptr().cast(), n) };
        assert!((c - r).abs() < EPSILON, "c = {c}, r = {r}.");
    } else {
        println!("detected no avx512vpopcntdq, skipped");
    }
    if detect::x86_64::detect_v4() {
        println!("detected v4");
        let c = unsafe { c::v_binary_cnt_v4(a.as_ptr().cast(), n) };
        assert!((c - r).abs() < EPSILON, "c = {c}, r = {r}.");
    } else {
        println!("detected no v4, skipped");
    }
    if detect::x86_64::detect_v3() {
        println!("detected v3");
        let c = unsafe { c::v_binary_cnt_v3(a.as_ptr().cast(), n) };
        assert!((c - r).abs() < EPSILON, "c = {c}, r = {r}.");
    } else {
        println!("detected no v3, skipped");
    }
}
