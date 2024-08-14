#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[allow(non_snake_case)]
#[inline]
pub fn prefetch_read_NTA(ptr: *const i8) {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        #[cfg(target_arch = "x86")]
        use core::arch::x86::{_mm_prefetch, _MM_HINT_NTA};

        #[cfg(target_arch = "x86_64")]
        use core::arch::x86_64::{_mm_prefetch, _MM_HINT_NTA};

        unsafe {
            _mm_prefetch(ptr, _MM_HINT_NTA);
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        use core::arch::aarch64::{_prefetch, _PREFETCH_LOCALITY0, _PREFETCH_READ};

        unsafe {
            _prefetch(ptr, _PREFETCH_READ, _PREFETCH_LOCALITY0);
        }
    }
}
