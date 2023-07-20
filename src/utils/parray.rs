use crate::memory::PBox;
use crate::prelude::*;
use std::fmt::Debug;
use std::mem::MaybeUninit;
use std::ops::{Deref, DerefMut};

pub struct PArray<T> {
    data: PBox<[MaybeUninit<T>]>,
    len: usize,
}

impl<T> PArray<T> {
    pub fn new(capacity: usize, storage: Storage) -> anyhow::Result<Self> {
        Ok(Self {
            data: PBox::new_uninit_slice(capacity, storage)?,
            len: 0,
        })
    }
    pub fn clear(&mut self) {
        self.len = 0;
    }
    pub fn capacity(&self) -> usize {
        self.data.len()
    }
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn insert(&mut self, index: usize, element: T) -> anyhow::Result<()> {
        assert!(index <= self.len);
        if self.len == self.capacity() {
            anyhow::bail!("The vector is full.");
        }
        unsafe {
            if index < self.len {
                let p = self.data.as_ptr().add(index).cast_mut();
                std::ptr::copy(p, p.add(1), self.len - index);
            }
            self.data[index].write(element);
            self.len += 1;
        }
        Ok(())
    }
    pub fn push(&mut self, element: T) -> anyhow::Result<()> {
        if self.capacity() == self.len {
            anyhow::bail!("The vector is full.");
        }
        let index = self.len;
        self.data[index].write(element);
        self.len += 1;
        Ok(())
    }
    #[allow(dead_code)]
    pub fn pop(&mut self) -> Option<T> {
        if self.len == 0 {
            return None;
        }
        let value;
        unsafe {
            self.len -= 1;
            value = self.data[self.len].assume_init_read();
        }
        Some(value)
    }
}

impl<T> Deref for PArray<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        unsafe { MaybeUninit::slice_assume_init_ref(&self.data[..self.len]) }
    }
}

impl<T> DerefMut for PArray<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { MaybeUninit::slice_assume_init_mut(&mut self.data[..self.len]) }
    }
}

impl<T: Debug> Debug for PArray<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut list = f.debug_list();
        list.entries(self.deref());
        list.finish()
    }
}
