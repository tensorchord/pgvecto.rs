/// Function multiversioning attribute macros for `pgvecto.rs`.
///
/// ```no_run
/// #![feature(doc_cfg)]
///
/// #[cfg(any(target_arch = "x86_64", doc))]
/// #[doc(cfg(target_arch = "x86_64"))]
/// #[detect::target_cpu(enable = "v3")]
/// unsafe fn g_v3(x: &[u32]) -> u32 {
///     todo!()
/// }
///
/// #[cfg(all(target_arch = "x86_64", test))]
/// #[test]
/// fn g_v3_test() {
///     const EPSILON: F32 = F32(1e-5);
///     detect::init();
///     if !detect::v3::detect() {
///         println!("test {} ... skipped (v3)", module_path!());
///         return;
///     }
///     let x = vec![0u32; 400];
///     x.fill_with(|| rand::random());
///     let specialized = unsafe { g_v3(&x) };
///     let fallback = unsafe { g_fallback(&x) };
///     assert!(
///         (specialized - fallback).abs() < EPSILON,
///         "specialized = {specialized}, fallback = {fallback}."
///     );
/// }
///
/// // It generates x86_64/v3, x86_64/v2, aarch64/neon and fallback versions of this function.
/// // It takes advantage of `g_v4` as x86_64/v4 version of this function.
/// // It exposes the fallback version with the name "g_fallback".
/// #[detect::multiversion(v3 = import, v2, neon, fallback = export)]
/// fn g(x: &[u32]) -> u32 {
///     let mut result = 0_u32;
///     for v in x {
///         result = result.wrapping_add(*v);
///     }
///     result
/// }
/// ```
pub use detect_macros::multiversion;

/// This macros allows you to enable a set of features by target cpu names.
pub use detect_macros::target_cpu;

detect_macros::main!();
