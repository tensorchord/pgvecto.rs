use crate::memory::Address;
use crate::memory::PBox;
use crate::memory::Persistent;
use crate::memory::Ptr;
use crate::prelude::*;
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

type Boxed<T> = PBox<[UnsafeCell<MaybeUninit<T>>]>;

pub struct Root {
    len: AtomicUsize,
    inflight: AtomicUsize,
    // ----------------------
    data: Boxed<u64>,
    vector: Boxed<Scalar>,
}

static_assertions::assert_impl_all!(Root: Persistent);

pub struct Vectors {
    address: Address,
    root: &'static Root,
    dims: u16,
    capacity: usize,
}

impl Vectors {
    pub fn build(options: Options) -> anyhow::Result<Self> {
        let storage = options.storage_vectors;
        let ptr = PBox::new(
            unsafe {
                let len_data = options.capacity;
                let len_vector = options.capacity * options.dims as usize;
                Root {
                    len: AtomicUsize::new(0),
                    inflight: AtomicUsize::new(0),
                    data: PBox::new_uninit_slice(len_data, storage)?.assume_init(),
                    vector: PBox::new_uninit_slice(len_vector, storage)?.assume_init(),
                }
            },
            storage,
        )?
        .into_raw();
        let root = unsafe { ptr.as_ref() };
        let address = ptr.address();
        Ok(Self {
            root,
            dims: options.dims,
            capacity: options.capacity,
            address,
        })
    }
    pub fn address(&self) -> Address {
        self.address
    }
    pub fn load(options: Options, address: Address) -> anyhow::Result<Self> {
        Ok(Self {
            root: unsafe { Ptr::new(address, ()).as_ref() },
            capacity: options.capacity,
            dims: options.dims,
            address,
        })
    }
    pub fn put(&self, data: u64, vector: &[Scalar]) -> anyhow::Result<usize> {
        // If the index is approaching to `usize::MAX`, it will break. But it will not likely happen.
        let i = self.root.inflight.fetch_add(1, Ordering::AcqRel);
        if i >= self.capacity {
            self.root.inflight.store(self.capacity, Ordering::Release);
            anyhow::bail!("Full.");
        }
        unsafe {
            let uninit_data = &mut *self.root.data[i].get();
            MaybeUninit::write(uninit_data, data);
            let uninit_vector =
                assume_mutable(&self.root.vector[i * self.dims as usize..][..self.dims as usize]);
            uninit_vector.copy_from_slice(std::slice::from_raw_parts(
                vector.as_ptr() as *const MaybeUninit<Scalar>,
                vector.len(),
            ));
        }
        while self
            .root
            .len
            .compare_exchange_weak(i, i + 1, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            std::hint::spin_loop();
        }
        Ok(i)
    }
    pub fn len(&self) -> usize {
        self.root.len.load(Ordering::Acquire)
    }
    pub fn get_data(&self, i: usize) -> u64 {
        unsafe { (*self.root.data[i].get()).assume_init_read() }
    }
    pub fn get_vector(&self, i: usize) -> &[Scalar] {
        unsafe {
            assume_immutable_init(&self.root.vector[i * self.dims as usize..][..self.dims as usize])
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
