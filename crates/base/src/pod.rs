// This module is a workaround for orphan rules

use crate::scalar::{F16, F32, I8};

/// # Safety
///
/// * No uninitialized bytes.
/// * Can be safely zero-initialized.
/// * Inhabited.
pub unsafe trait Pod: Copy {}

unsafe impl Pod for u8 {}
unsafe impl Pod for u16 {}
unsafe impl Pod for u32 {}
unsafe impl Pod for u64 {}
unsafe impl Pod for u128 {}
unsafe impl Pod for usize {}

unsafe impl Pod for i8 {}
unsafe impl Pod for i16 {}
unsafe impl Pod for i32 {}
unsafe impl Pod for i64 {}
unsafe impl Pod for i128 {}
unsafe impl Pod for isize {}

unsafe impl Pod for f32 {}
unsafe impl Pod for f64 {}

unsafe impl Pod for I8 {}
unsafe impl Pod for F16 {}
unsafe impl Pod for F32 {}

unsafe impl Pod for (F32, u32) {}

unsafe impl Pod for crate::search::Payload {}

pub fn bytes_of<T: Pod>(t: &T) -> &[u8] {
    unsafe {
        core::slice::from_raw_parts(
            std::ptr::addr_of!(*t) as *const u8,
            std::mem::size_of::<T>(),
        )
    }
}

pub fn zeroed_vec<T: Pod>(length: usize) -> Vec<T> {
    unsafe {
        let mut v = Vec::with_capacity(length);
        std::ptr::write_bytes(v.as_mut_ptr(), 0, length);
        v.set_len(length);
        v
    }
}

pub fn try_pod_read_unaligned<T: Pod>(bytes: &[u8]) -> T {
    unsafe { (bytes.as_ptr() as *const T).read_unaligned() }
}
