use super::storage::Storage;
use super::storage::StoragePreallocator;
use super::storage_mmap::MmapBox;
use crate::bgworker::index::IndexOptions;
use crate::prelude::*;
use serde::{Deserialize, Serialize};
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorsOptions {
    #[serde(default)]
    pub memmap: Memmap,
}

impl Default for VectorsOptions {
    fn default() -> Self {
        Self {
            memmap: Default::default(),
        }
    }
}

type Boxed<T> = MmapBox<[UnsafeCell<MaybeUninit<T>>]>;

pub struct Vectors {
    dims: u16,
    capacity: usize,
    //
    len: MmapBox<AtomicUsize>,
    inflight: MmapBox<AtomicUsize>,
    data: Boxed<u64>,
    vector: Boxed<Scalar>,
}

unsafe impl Send for Vectors {}
unsafe impl Sync for Vectors {}

impl Vectors {
    pub fn prebuild(storage: &mut StoragePreallocator, options: IndexOptions) {
        let memmap = options.vectors.memmap;
        let len_data = options.capacity;
        let len_vector = options.capacity * options.dims as usize;
        storage.palloc_mmap::<AtomicUsize>(memmap);
        storage.palloc_mmap::<AtomicUsize>(memmap);
        storage.palloc_mmap_slice::<UnsafeCell<MaybeUninit<u64>>>(memmap, len_data);
        storage.palloc_mmap_slice::<UnsafeCell<MaybeUninit<Scalar>>>(memmap, len_vector);
    }
    pub fn build(storage: &mut Storage, options: IndexOptions) -> Self {
        let memmap = options.vectors.memmap;
        let len_data = options.capacity;
        let len_vector = options.capacity * options.dims as usize;
        Self {
            dims: options.dims,
            capacity: options.capacity,
            len: unsafe {
                let mut len = storage.alloc_mmap(memmap);
                len.write(AtomicUsize::new(0));
                len.assume_init()
            },
            inflight: unsafe {
                let mut inflight = storage.alloc_mmap(memmap);
                inflight.write(AtomicUsize::new(0));
                inflight.assume_init()
            },
            data: unsafe { storage.alloc_mmap_slice(memmap, len_data).assume_init() },
            vector: unsafe { storage.alloc_mmap_slice(memmap, len_vector).assume_init() },
        }
    }
    pub fn load(storage: &mut Storage, options: IndexOptions) -> Self {
        let memmap = options.vectors.memmap;
        let len_data = options.capacity;
        let len_vector = options.capacity * options.dims as usize;
        Self {
            capacity: options.capacity,
            dims: options.dims,
            len: unsafe { storage.alloc_mmap(memmap).assume_init() },
            inflight: unsafe { storage.alloc_mmap(memmap).assume_init() },
            data: unsafe { storage.alloc_mmap_slice(memmap, len_data).assume_init() },
            vector: unsafe { storage.alloc_mmap_slice(memmap, len_vector).assume_init() },
        }
    }
    pub fn put(&self, data: u64, vector: &[Scalar]) -> usize {
        // If the index is approaching to `usize::MAX`, it will break. But it will not likely happen.
        let i = self.inflight.fetch_add(1, Ordering::AcqRel);
        if i >= self.capacity {
            self.inflight.store(self.capacity, Ordering::Release);
            panic!("The capacity is used up.");
        }
        unsafe {
            let uninit_data = &mut *self.data[i].get();
            MaybeUninit::write(uninit_data, data);
            let slice_vector = &self.vector[i * self.dims as usize..][..self.dims as usize];
            let uninit_vector = assume_mutable(slice_vector);
            uninit_vector.copy_from_slice(std::slice::from_raw_parts(
                vector.as_ptr() as *const MaybeUninit<Scalar>,
                vector.len(),
            ));
        }
        while self
            .len
            .compare_exchange_weak(i, i + 1, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            std::hint::spin_loop();
        }
        i
    }
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Acquire)
    }
    pub fn get_data(&self, i: usize) -> u64 {
        unsafe { (*self.data[i].get()).assume_init_read() }
    }
    pub fn get_vector(&self, i: usize) -> &[Scalar] {
        unsafe {
            assume_immutable_init(&self.vector[i * self.dims as usize..][..self.dims as usize])
        }
    }
}

#[allow(clippy::mut_from_ref)]
unsafe fn assume_mutable<T>(slice: &[UnsafeCell<T>]) -> &mut [T] {
    let p = slice.as_ptr().cast::<UnsafeCell<T>>() as *mut T;
    std::slice::from_raw_parts_mut(p, slice.len())
}

unsafe fn assume_immutable_init<T>(slice: &[UnsafeCell<MaybeUninit<T>>]) -> &[T] {
    let p = slice.as_ptr().cast::<UnsafeCell<T>>() as *const T;
    std::slice::from_raw_parts(p, slice.len())
}
