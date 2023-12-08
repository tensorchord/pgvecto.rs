#[cfg(not(target_arch = "x86_64"))]
pub fn detect() -> bool {
    false
}

#[cfg(target_arch = "x86_64")]
pub fn detect() -> bool {
    std_detect::is_x86_feature_detected!("avx2")
}
