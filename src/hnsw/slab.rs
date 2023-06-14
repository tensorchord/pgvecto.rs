use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut, Index, IndexMut};
use std::sync::atomic::Ordering;
use std::{mem::MaybeUninit, sync::atomic::AtomicUsize};

pub struct Slab<T> {
    first: AtomicUsize,
    last: AtomicUsize,
    data: Box<[UnsafeCell<MaybeUninit<T>>]>,
}

impl<T> Slab<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            first: AtomicUsize::new(0),
            last: AtomicUsize::new(0),
            data: unsafe {
                let mut vec = Vec::with_capacity(capacity);
                vec.set_len(capacity);
                vec.into_boxed_slice()
            },
        }
    }
    pub fn get(&self, index: usize) -> Option<&T> {
        let n = self.first.load(Ordering::Acquire);
        if index < n {
            Some(unsafe { (*self.data.get_unchecked(index).get()).assume_init_ref() })
        } else {
            None
        }
    }
    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        let n = self.first.get_mut().clone();
        if index < n {
            Some(unsafe {
                self.data
                    .get_unchecked_mut(index)
                    .get_mut()
                    .assume_init_mut()
            })
        } else {
            None
        }
    }
    pub fn data(&self) -> &[T] {
        let n = self.first.load(Ordering::Acquire);
        unsafe { std::slice::from_raw_parts(self.data.as_ptr() as *const T, n) }
    }
    pub fn data_mut(&mut self) -> &mut [T] {
        let n = self.first.load(Ordering::Acquire);
        unsafe { std::slice::from_raw_parts_mut(self.data.as_mut_ptr() as *mut T, n) }
    }
    pub fn len(&self) -> usize {
        self.first.load(Ordering::Acquire)
    }
    pub fn len_mut(&mut self) -> usize {
        self.first.get_mut().clone()
    }
    pub fn capacity(&self) -> usize {
        self.data.len()
    }
    pub fn push(&self, data: T) -> Result<usize, T> {
        // If the index is approaching to `usize::MAX`, it will break. But it will not likely happen.
        let index = self.last.fetch_add(1, Ordering::AcqRel);
        if index >= self.data.len() {
            self.last.store(self.data.len(), Ordering::Release);
            return Err(data);
        }
        unsafe {
            (*self.data.get_unchecked(index).get()).write(data);
        }
        while let Err(_) =
            self.first
                .compare_exchange_weak(index, index + 1, Ordering::AcqRel, Ordering::Relaxed)
        {
            std::hint::spin_loop();
        }
        Ok(index)
    }
    pub fn push_mut(&mut self, data: T) -> Result<usize, T> {
        let index = *self.first.get_mut();
        if index >= self.data.len() {
            return Err(data);
        }
        unsafe {
            self.data.get_unchecked_mut(index).get_mut().write(data);
        }
        *self.first.get_mut() += 1;
        *self.last.get_mut() += 1;
        Ok(index)
    }
}

unsafe impl<T: Send> Send for Slab<T> {}
unsafe impl<T: Sync> Sync for Slab<T> {}

impl<T> Index<usize> for Slab<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        self.get(index).unwrap()
    }
}

impl<T> IndexMut<usize> for Slab<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.get_mut(index).unwrap()
    }
}

impl<T> Deref for Slab<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.data()
    }
}

impl<T> DerefMut for Slab<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.data_mut()
    }
}

impl<T> Drop for Slab<T> {
    fn drop(&mut self) {
        assert_eq!(self.first.get_mut(), self.last.get_mut());
        if std::mem::needs_drop::<T>() {
            let n = *self.first.get_mut();
            for i in 0..n {
                unsafe {
                    self.data[i].get_mut().assume_init_drop();
                }
            }
        }
    }
}
