#![cfg(target_arch = "x86_64")]

#[test]
fn test_v_f16_cosine() {
    detect::initialize();
    const EPSILON: f32 = f16::EPSILON.to_f32_const();
    use half::f16;
    unsafe fn v_f16_cosine(a: *const u16, b: *const u16, n: usize) -> f32 {
        let mut xy = 0.0f32;
        let mut xx = 0.0f32;
        let mut yy = 0.0f32;
        for i in 0..n {
            let x = unsafe { a.add(i).cast::<f16>().read() }.to_f32();
            let y = unsafe { b.add(i).cast::<f16>().read() }.to_f32();
            xy += x * y;
            xx += x * x;
            yy += y * y;
        }
        xy / (xx * yy).sqrt()
    }
    let n = 4000;
    let a = (0..n).map(|_| rand::random::<f16>()).collect::<Vec<_>>();
    let b = (0..n).map(|_| rand::random::<f16>()).collect::<Vec<_>>();
    let r = unsafe { v_f16_cosine(a.as_ptr().cast(), b.as_ptr().cast(), n) };
    if detect::x86_64::detect_avx512fp16() {
        println!("detected avx512fp16");
        let c = unsafe { c::v_f16_cosine_avx512fp16(a.as_ptr().cast(), b.as_ptr().cast(), n) };
        assert!((c - r).abs() < EPSILON, "c = {c}, r = {r}.");
    } else {
        println!("detected no avx512fp16, skipped");
    }
    if detect::x86_64::detect_v4() {
        println!("detected v4");
        let c = unsafe { c::v_f16_cosine_v4(a.as_ptr().cast(), b.as_ptr().cast(), n) };
        assert!((c - r).abs() < EPSILON, "c = {c}, r = {r}.");
    } else {
        println!("detected no v4, skipped");
    }
    if detect::x86_64::detect_v3() {
        println!("detected v3");
        let c = unsafe { c::v_f16_cosine_v3(a.as_ptr().cast(), b.as_ptr().cast(), n) };
        assert!((c - r).abs() < EPSILON, "c = {c}, r = {r}.");
    } else {
        println!("detected no v3, skipped");
    }
}

#[test]
fn test_v_f16_dot() {
    detect::initialize();
    const EPSILON: f32 = 1.0f32;
    use half::f16;
    unsafe fn v_f16_dot(a: *const u16, b: *const u16, n: usize) -> f32 {
        let mut xy = 0.0f32;
        for i in 0..n {
            let x = unsafe { a.add(i).cast::<f16>().read() }.to_f32();
            let y = unsafe { b.add(i).cast::<f16>().read() }.to_f32();
            xy += x * y;
        }
        xy
    }
    let n = 4000;
    let a = (0..n).map(|_| rand::random::<f16>()).collect::<Vec<_>>();
    let b = (0..n).map(|_| rand::random::<f16>()).collect::<Vec<_>>();
    let r = unsafe { v_f16_dot(a.as_ptr().cast(), b.as_ptr().cast(), n) };
    if detect::x86_64::detect_avx512fp16() {
        println!("detected avx512fp16");
        let c = unsafe { c::v_f16_dot_avx512fp16(a.as_ptr().cast(), b.as_ptr().cast(), n) };
        assert!((c - r).abs() < EPSILON, "c = {c}, r = {r}.");
    } else {
        println!("detected no avx512fp16, skipped");
    }
    if detect::x86_64::detect_v4() {
        println!("detected v4");
        let c = unsafe { c::v_f16_dot_v4(a.as_ptr().cast(), b.as_ptr().cast(), n) };
        assert!((c - r).abs() < EPSILON, "c = {c}, r = {r}.");
    } else {
        println!("detected no v4, skipped");
    }
    if detect::x86_64::detect_v3() {
        println!("detected v3");
        let c = unsafe { c::v_f16_dot_v3(a.as_ptr().cast(), b.as_ptr().cast(), n) };
        assert!((c - r).abs() < EPSILON, "c = {c}, r = {r}.");
    } else {
        println!("detected no v3, skipped");
    }
}

#[test]
fn test_v_f16_sl2() {
    detect::initialize();
    const EPSILON: f32 = 1.0f32;
    use half::f16;
    unsafe fn v_f16_sl2(a: *const u16, b: *const u16, n: usize) -> f32 {
        let mut dd = 0.0f32;
        for i in 0..n {
            let x = unsafe { a.add(i).cast::<f16>().read() }.to_f32();
            let y = unsafe { b.add(i).cast::<f16>().read() }.to_f32();
            let d = x - y;
            dd += d * d;
        }
        dd
    }
    let n = 4000;
    let a = (0..n).map(|_| rand::random::<f16>()).collect::<Vec<_>>();
    let b = (0..n).map(|_| rand::random::<f16>()).collect::<Vec<_>>();
    let r = unsafe { v_f16_sl2(a.as_ptr().cast(), b.as_ptr().cast(), n) };
    if detect::x86_64::detect_avx512fp16() {
        println!("detected avx512fp16");
        let c = unsafe { c::v_f16_sl2_avx512fp16(a.as_ptr().cast(), b.as_ptr().cast(), n) };
        assert!((c - r).abs() < EPSILON, "c = {c}, r = {r}.");
    } else {
        println!("detected no avx512fp16, skipped");
    }
    if detect::x86_64::detect_v4() {
        println!("detected v4");
        let c = unsafe { c::v_f16_sl2_v4(a.as_ptr().cast(), b.as_ptr().cast(), n) };
        assert!((c - r).abs() < EPSILON, "c = {c}, r = {r}.");
    } else {
        println!("detected no v4, skipped");
    }
    if detect::x86_64::detect_v3() {
        println!("detected v3");
        let c = unsafe { c::v_f16_sl2_v3(a.as_ptr().cast(), b.as_ptr().cast(), n) };
        assert!((c - r).abs() < EPSILON, "c = {c}, r = {r}.");
    } else {
        println!("detected no v3, skipped");
    }
}
