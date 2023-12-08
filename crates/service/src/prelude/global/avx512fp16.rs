// avx512fp16,avx512vl,avx512f,bmi2

#[cfg(not(target_arch = "x86_64"))]
pub fn detect() -> bool {
    false
}

#[cfg(target_arch = "x86_64")]
pub fn detect() -> bool {
    std_detect::is_x86_feature_detected!("avx512fp16")
        && std_detect::is_x86_feature_detected!("avx512vl")
        && std_detect::is_x86_feature_detected!("avx512f")
        && std_detect::is_x86_feature_detected!("bmi2")
}
