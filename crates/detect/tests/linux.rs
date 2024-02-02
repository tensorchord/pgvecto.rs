#![cfg(target_os = "linux")]

#[test]
fn print() {
    detect::initialize();
    assert_eq!(detect::linux::test_memfd(), detect::linux::detect_memfd());
}
