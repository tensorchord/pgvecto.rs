use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ops::Deref;

#[repr(C)]
pub struct UnsafeOnce<T> {
    inner: UnsafeCell<MaybeUninit<T>>,
}

impl<T> UnsafeOnce<T> {
    #[allow(unused)]
    pub const unsafe fn new() -> Self {
        Self {
            inner: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }
    pub fn set(&self, data: T) {
        unsafe {
            (*self.inner.get()).write(data);
        }
    }
}

impl<T> Deref for UnsafeOnce<T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe { (*self.inner.get()).assume_init_ref() }
    }
}
