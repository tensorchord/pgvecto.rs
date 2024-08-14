#[inline(always)]
pub fn prefetch<T: ?Sized>(ptr: *const T) {
    #[cfg(target_arch = "x86_64")]
    {
        use std::arch::x86_64::*;
        unsafe {
            _mm_prefetch::<_MM_HINT_T0>(ptr.cast());
        }
    }
}
