use std::sync::atomic::{AtomicBool, Ordering};

static ATOMIC_AVX512FP16: AtomicBool = AtomicBool::new(false);

pub fn test_avx512fp16() -> bool {
    std_detect::is_x86_feature_detected!("avx512fp16") && test_v4()
}

pub fn ctor_avx512fp16() {
    ATOMIC_AVX512FP16.store(test_avx512fp16(), Ordering::Relaxed);
}

pub fn detect_avx512fp16() -> bool {
    ATOMIC_AVX512FP16.load(Ordering::Relaxed)
}

static ATOMIC_V4: AtomicBool = AtomicBool::new(false);

pub fn test_v4() -> bool {
    std_detect::is_x86_feature_detected!("avx512bw")
        && std_detect::is_x86_feature_detected!("avx512cd")
        && std_detect::is_x86_feature_detected!("avx512dq")
        && std_detect::is_x86_feature_detected!("avx512f")
        && std_detect::is_x86_feature_detected!("avx512vl")
        && test_v3()
}

pub fn ctor_v4() {
    ATOMIC_V4.store(test_v4(), Ordering::Relaxed);
}

pub fn detect_v4() -> bool {
    ATOMIC_V4.load(Ordering::Relaxed)
}

static ATOMIC_V3: AtomicBool = AtomicBool::new(false);

pub fn test_v3() -> bool {
    std_detect::is_x86_feature_detected!("avx")
        && std_detect::is_x86_feature_detected!("avx2")
        && std_detect::is_x86_feature_detected!("bmi1")
        && std_detect::is_x86_feature_detected!("bmi2")
        && std_detect::is_x86_feature_detected!("f16c")
        && std_detect::is_x86_feature_detected!("fma")
        && std_detect::is_x86_feature_detected!("lzcnt")
        && std_detect::is_x86_feature_detected!("movbe")
        && std_detect::is_x86_feature_detected!("xsave")
        && test_v2()
}

pub fn ctor_v3() {
    ATOMIC_V3.store(test_v3(), Ordering::Relaxed);
}

pub fn detect_v3() -> bool {
    ATOMIC_V3.load(Ordering::Relaxed)
}

static ATOMIC_V2: AtomicBool = AtomicBool::new(false);

pub fn test_v2() -> bool {
    std_detect::is_x86_feature_detected!("cmpxchg16b")
        && std_detect::is_x86_feature_detected!("fxsr")
        && std_detect::is_x86_feature_detected!("popcnt")
        && std_detect::is_x86_feature_detected!("sse")
        && std_detect::is_x86_feature_detected!("sse2")
        && std_detect::is_x86_feature_detected!("sse3")
        && std_detect::is_x86_feature_detected!("sse4.1")
        && std_detect::is_x86_feature_detected!("sse4.2")
        && std_detect::is_x86_feature_detected!("ssse3")
}

pub fn ctor_v2() {
    ATOMIC_V2.store(test_v2(), Ordering::Relaxed);
}

pub fn detect_v2() -> bool {
    ATOMIC_V2.load(Ordering::Relaxed)
}
