#![cfg(target_arch = "x86_64")]

#[test]
fn print() {
    detect::initialize();
    assert_eq!(
        detect::x86_64::test_avx512fp16(),
        detect::x86_64::detect_avx512fp16()
    );
    assert_eq!(
        detect::x86_64::test_avx512vpopcntdq(),
        detect::x86_64::detect_avx512vpopcntdq()
    );
    assert_eq!(detect::x86_64::test_v4(), detect::x86_64::detect_v4());
    assert_eq!(detect::x86_64::test_v3(), detect::x86_64::detect_v3());
    assert_eq!(detect::x86_64::test_v2(), detect::x86_64::detect_v2());
}
