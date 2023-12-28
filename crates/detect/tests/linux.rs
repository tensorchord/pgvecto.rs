#![cfg(target_os = "linux")]

#[test]
fn print() {
    assert_eq!(detect::linux::test_memfd(), detect::linux::detect_memfd());
}
