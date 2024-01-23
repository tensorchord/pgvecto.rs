use bytemuck::Zeroable;
use std::ops::{Deref, DerefMut, Index, IndexMut};

#[derive(Debug, Clone)]
pub struct Vec2<T> {
    dims: u16,
    v: Vec<T>,
}

impl<T: Zeroable + Ord> Vec2<T> {
    pub fn new(dims: u16, n: usize) -> Self {
        Self {
            dims,
            v: bytemuck::zeroed_vec(dims as usize * n),
        }
    }
    pub fn dims(&self) -> u16 {
        self.dims
    }
    pub fn len(&self) -> usize {
        self.v.len() / self.dims as usize
    }
    pub fn argsort(&self) -> Vec<usize> {
        let mut index: Vec<usize> = (0..self.len()).collect();
        index.sort_by_key(|i| &self[*i]);
        index
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

impl<T> Index<usize> for Vec2<T> {
    type Output = [T];

    fn index(&self, index: usize) -> &Self::Output {
        &self.v[self.dims as usize * index..][..self.dims as usize]
    }
}

impl<T> IndexMut<usize> for Vec2<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.v[self.dims as usize * index..][..self.dims as usize]
    }
}

impl<T> Deref for Vec2<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.v.deref()
    }
}

impl<T> DerefMut for Vec2<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.v.deref_mut()
    }
}
