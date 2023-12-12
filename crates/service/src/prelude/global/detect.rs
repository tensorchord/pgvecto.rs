#[cfg(target_arch = "x86_64")]
pub fn detect_avx512fp16() -> bool {
    std_detect::is_x86_feature_detected!("avx512fp16")
        && std_detect::is_x86_feature_detected!("bmi2")
}

#[cfg(target_arch = "x86_64")]
pub fn detect_avx2() -> bool {
    std_detect::is_x86_feature_detected!("avx2")
}
