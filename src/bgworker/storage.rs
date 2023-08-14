use super::storage_mmap::{MmapBox, StorageMmap};
use crate::bgworker::storage_mmap;
use crate::prelude::*;
use std::mem::MaybeUninit;

#[derive(Debug, Clone)]
pub enum StoragePreallocatorElement {
    Mmap(storage_mmap::StorageMmapPreallocatorElement),
}

pub struct StoragePreallocator {
    sequence: Vec<StoragePreallocatorElement>,
}

impl StoragePreallocator {
    pub fn new() -> Self {
        Self {
            sequence: Vec::new(),
        }
    }
    pub fn palloc_mmap<T>(&mut self, memmap: Memmap) {
        use StoragePreallocatorElement::Mmap;
        self.sequence
            .push(Mmap(storage_mmap::prealloc::<T>(memmap)));
    }
    pub fn palloc_mmap_slice<T>(&mut self, memmap: Memmap, len: usize) {
        use StoragePreallocatorElement::Mmap;
        self.sequence
            .push(Mmap(storage_mmap::prealloc_slice::<T>(memmap, len)));
    }
}

pub struct Storage {
    storage_mmap: StorageMmap,
}

impl Storage {
    pub fn build(id: Id, preallocator: StoragePreallocator) -> Self {
        let mmap_iter = preallocator
            .sequence
            .iter()
            .filter_map(|x| {
                use StoragePreallocatorElement::Mmap;
                #[allow(unreachable_patterns)]
                match x.clone() {
                    Mmap(x) => Some(x),
                    _ => None,
                }
            })
            .collect::<Vec<_>>()
            .into_iter();
        let storage_mmap = StorageMmap::build(id, mmap_iter);
        Self { storage_mmap }
    }
    pub fn load(id: Id) -> Self {
        let storage_mmap = StorageMmap::load(id);
        Self { storage_mmap }
    }
    pub fn alloc_mmap<T>(&mut self, memmap: Memmap) -> MmapBox<MaybeUninit<T>> {
        self.storage_mmap.alloc_mmap(memmap)
    }
    pub fn alloc_mmap_slice<T>(&mut self, memmap: Memmap, len: usize) -> MmapBox<[MaybeUninit<T>]> {
        self.storage_mmap.alloc_mmap_slice(memmap, len)
    }
    pub fn persist(&self) {
        self.storage_mmap.persist();
    }
}
