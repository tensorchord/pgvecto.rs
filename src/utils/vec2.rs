use crate::prelude::*;
use std::ops::{Deref, DerefMut, Index, IndexMut};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Vec2 {
    dims: u16,
    v: Box<[Scalar]>,
}

impl Vec2 {
    pub fn new(dims: u16, n: usize) -> Self {
        Self {
            dims,
            v: unsafe { Box::new_zeroed_slice(dims as usize * n).assume_init() },
        }
    }
    pub fn dims(&self) -> u16 {
        self.dims
    }
    pub fn len(&self) -> usize {
        self.v.len() / self.dims as usize
    }
    pub fn copy_within(&mut self, i: usize, j: usize) {
        assert!(i < self.len() && j < self.len());
        unsafe {
            if i != j {
                let src = self.v.as_ptr().add(self.dims as usize * i);
                let dst = self.v.as_mut_ptr().add(self.dims as usize * j);
                std::ptr::copy_nonoverlapping(src, dst, self.dims as usize);
            }
        }
    }
}

impl Index<usize> for Vec2 {
    type Output = [Scalar];

    fn index(&self, index: usize) -> &Self::Output {
        &self.v[self.dims as usize * index..][..self.dims as usize]
    }
}

impl IndexMut<usize> for Vec2 {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.v[self.dims as usize * index..][..self.dims as usize]
    }
}

impl Deref for Vec2 {
    type Target = [Scalar];

    fn deref(&self) -> &Self::Target {
        self.v.deref()
    }
}

impl DerefMut for Vec2 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.v.deref_mut()
    }
}
