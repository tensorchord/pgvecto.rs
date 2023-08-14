use crate::bgworker::storage::{Storage, StoragePreallocator};
use crate::bgworker::storage_mmap::MmapBox;
use crate::prelude::*;
use std::ops::{Deref, DerefMut, Index, IndexMut};

#[derive(Debug)]
pub struct MmapVec2 {
    dims: u16,
    v: MmapBox<[Scalar]>,
}

impl MmapVec2 {
    pub fn prebuild(storage: &mut StoragePreallocator, dims: u16, n: usize) {
        storage.palloc_mmap_slice::<Scalar>(Memmap::Ram, dims as usize * n);
    }
    pub fn build(storage: &mut Storage, dims: u16, n: usize) -> Self {
        let v = unsafe {
            storage
                .alloc_mmap_slice(Memmap::Ram, dims as usize * n)
                .assume_init()
        };
        Self { dims, v }
    }
    pub fn load(storage: &mut Storage, dims: u16, n: usize) -> Self {
        let v = unsafe {
            storage
                .alloc_mmap_slice(Memmap::Ram, dims as usize * n)
                .assume_init()
        };
        Self { dims, v }
    }
}

impl Index<usize> for MmapVec2 {
    type Output = [Scalar];

    fn index(&self, index: usize) -> &Self::Output {
        &self.v[self.dims as usize * index..][..self.dims as usize]
    }
}

impl IndexMut<usize> for MmapVec2 {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.v[self.dims as usize * index..][..self.dims as usize]
    }
}

impl Deref for MmapVec2 {
    type Target = [Scalar];

    fn deref(&self) -> &Self::Target {
        self.v.deref()
    }
}

impl DerefMut for MmapVec2 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.v.deref_mut()
    }
}
