use std::alloc::Layout;
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;

pub struct AlignBytes<const ALIGN: usize> {
    ptr: NonNull<[u8]>,
}

unsafe impl<const ALIGN: usize> Send for AlignBytes<ALIGN> {}
unsafe impl<const ALIGN: usize> Sync for AlignBytes<ALIGN> {}

impl<const ALIGN: usize> AlignBytes<ALIGN> {
    pub fn new_zeroed(len: usize) -> Self {
        let layout = Layout::from_size_align(len, ALIGN).expect("len is too large");
        let ptr = if len != 0 {
            NonNull::new(unsafe { std::alloc::alloc_zeroed(layout) }).expect("failed to alloc")
        } else {
            NonNull::new(ALIGN as _).unwrap()
        };
        Self {
            ptr: NonNull::slice_from_raw_parts(ptr, len),
        }
    }
}

impl<const ALIGN: usize> Clone for AlignBytes<ALIGN> {
    fn clone(&self) -> Self {
        let mut buf = AlignBytes::<ALIGN>::new_zeroed(self.len());
        buf[..].copy_from_slice(&self[..]);
        buf
    }
}

impl<const ALIGN: usize> Deref for AlignBytes<ALIGN> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { self.ptr.as_ref() }
    }
}

impl<const ALIGN: usize> DerefMut for AlignBytes<ALIGN> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.ptr.as_mut() }
    }
}

impl<const ALIGN: usize> AsRef<[u8]> for AlignBytes<ALIGN> {
    fn as_ref(&self) -> &[u8] {
        unsafe { self.ptr.as_ref() }
    }
}

impl<const ALIGN: usize> AsMut<[u8]> for AlignBytes<ALIGN> {
    fn as_mut(&mut self) -> &mut [u8] {
        unsafe { self.ptr.as_mut() }
    }
}

impl<const ALIGN: usize> Drop for AlignBytes<ALIGN> {
    fn drop(&mut self) {
        if !self.ptr.is_empty() {
            unsafe {
                let layout = Layout::from_size_align_unchecked(self.ptr.len(), ALIGN);
                std::alloc::dealloc(self.ptr.as_ptr().cast(), layout);
            }
        }
    }
}