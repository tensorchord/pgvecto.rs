use crate::memory::{using, Ptr};
use crate::prelude::Storage;
use std::alloc::Layout;
use std::borrow::{Borrow, BorrowMut};
use std::fmt::Debug;
use std::mem::MaybeUninit;
use std::ops::{Deref, DerefMut};

pub struct PBox<T: ?Sized>(Ptr<T>);

impl<T: Sized> PBox<T> {
    pub fn new(t: T, storage: Storage) -> anyhow::Result<Self> {
        let ptr = using()
            .allocate(storage, std::alloc::Layout::new::<T>())?
            .cast::<T>();
        unsafe {
            ptr.as_mut_ptr().write(t);
        }
        Ok(Self(ptr))
    }
}

impl<T: ?Sized> PBox<T> {
    pub fn into_raw(self) -> Ptr<T> {
        let raw = self.0;
        std::mem::forget(self);
        raw
    }
    #[allow(dead_code)]
    pub fn from_raw(raw: Ptr<T>) -> Self {
        Self(raw)
    }
}

impl<T: ?Sized> Deref for PBox<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { self.0.as_ref() }
    }
}

impl<T: ?Sized> DerefMut for PBox<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.0.as_mut() }
    }
}

impl<T: ?Sized> AsRef<T> for PBox<T> {
    fn as_ref(&self) -> &T {
        unsafe { self.0.as_ref() }
    }
}

impl<T: ?Sized> AsMut<T> for PBox<T> {
    fn as_mut(&mut self) -> &mut T {
        unsafe { self.0.as_mut() }
    }
}

impl<T: ?Sized> Borrow<T> for PBox<T> {
    fn borrow(&self) -> &T {
        unsafe { self.0.as_ref() }
    }
}

impl<T: ?Sized> BorrowMut<T> for PBox<T> {
    fn borrow_mut(&mut self) -> &mut T {
        unsafe { self.0.as_mut() }
    }
}

impl<T: ?Sized + Debug> Debug for PBox<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self.deref(), f)
    }
}

impl<T: Sized> PBox<MaybeUninit<T>> {
    #[allow(dead_code)]
    pub fn new_uninit(storage: Storage) -> anyhow::Result<PBox<MaybeUninit<T>>> {
        let ptr = using()
            .allocate(storage, std::alloc::Layout::new::<T>())?
            .cast::<MaybeUninit<T>>();
        Ok(Self(ptr))
    }
    #[allow(dead_code)]
    pub unsafe fn assume_init(self) -> PBox<T> {
        let ptr = PBox::into_raw(self);
        PBox(Ptr::new(ptr.address(), ()))
    }
}

impl<T: Sized> PBox<[MaybeUninit<T>]> {
    pub fn new_uninit_slice(
        len: usize,
        storage: Storage,
    ) -> anyhow::Result<PBox<[MaybeUninit<T>]>> {
        let ptr = using().allocate(storage, Layout::array::<T>(len)?)?;
        let ptr = Ptr::from_raw_parts(ptr, len);
        Ok(PBox(ptr))
    }
    pub fn new_zeroed_slice(
        len: usize,
        storage: Storage,
    ) -> anyhow::Result<PBox<[MaybeUninit<T>]>> {
        let ptr = using().allocate_zeroed(storage, Layout::array::<T>(len)?)?;
        let ptr = Ptr::from_raw_parts(ptr, len);
        Ok(PBox(ptr))
    }
    pub unsafe fn assume_init(self) -> PBox<[T]> {
        let ptr = PBox::into_raw(self);
        PBox(Ptr::new(ptr.address(), ptr.metadata()))
    }
}
