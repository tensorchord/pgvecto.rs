mod block;
mod pbox;

pub use pbox::PBox;

use self::block::Block;
use crate::prelude::*;
use serde::{Deserialize, Serialize};
use std::alloc::{AllocError, Layout};
use std::cell::Cell;
use std::fmt::Debug;
use std::ptr::{NonNull, Pointee};
use std::sync::Arc;
use std::thread::{Scope, ScopedJoinHandle};

pub unsafe auto trait Persistent {}

impl<T> !Persistent for *const T {}
impl<T> !Persistent for *mut T {}
impl<T> !Persistent for &'_ T {}
impl<T> !Persistent for &'_ mut T {}

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Address(usize);

impl Debug for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({:?}, {:#x})", self.storage(), self.offset())
    }
}

impl Address {
    pub fn storage(self) -> Storage {
        use Storage::*;
        if Ram as usize == (self.0 >> 63) {
            Storage::Ram
        } else {
            Storage::Disk
        }
    }
    pub fn offset(self) -> usize {
        self.0 & ((1usize << 63) - 1)
    }
    pub const fn new(storage: Storage, offset: usize) -> Self {
        debug_assert!(offset < (1 << 63));
        Self((storage as usize) << 63 | offset << 0)
    }
    pub const DANGLING: Self = Address(usize::MAX);
}

#[repr(C)]
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ptr<T: ?Sized> {
    address: Address,
    metadata: <T as Pointee>::Metadata,
}

impl<T: ?Sized> Debug for Ptr<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if std::mem::size_of::<<T as Pointee>::Metadata>() == 0 {
            write!(f, "({:?})", self.address())
        } else if std::mem::size_of::<<T as Pointee>::Metadata>() == std::mem::size_of::<usize>() {
            let metadata = unsafe { std::mem::transmute_copy::<_, usize>(&self.metadata()) };
            write!(f, "({:?}, {:#x})", self.address(), metadata)
        } else {
            write!(f, "({:?}, ?)", self.address())
        }
    }
}

impl<T: ?Sized> Clone for Ptr<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: ?Sized> Copy for Ptr<T> {}

impl<T: ?Sized> Ptr<T> {
    pub fn storage(self) -> Storage {
        self.address.storage()
    }
    pub fn offset(self) -> usize {
        self.address.offset()
    }
    pub fn address(self) -> Address {
        self.address
    }
    pub fn metadata(self) -> <T as Pointee>::Metadata {
        self.metadata
    }
    pub fn new(address: Address, metadata: <T as Pointee>::Metadata) -> Self {
        Self { address, metadata }
    }
    pub fn cast<U: Sized>(self) -> Ptr<U> {
        Ptr::new(self.address, ())
    }
    pub fn from_raw_parts(data_address: Ptr<()>, metadata: <T as Pointee>::Metadata) -> Self {
        Ptr::new(data_address.address(), metadata)
    }
    pub fn as_ptr(self) -> *const T {
        let data_address = (OFFSETS[self.storage() as usize].get() + self.offset()) as _;
        let metadata = self.metadata();
        std::ptr::from_raw_parts(data_address, metadata)
    }
    pub fn as_mut_ptr(self) -> *mut T {
        let data_address = (OFFSETS[self.storage() as usize].get() + self.offset()) as _;
        let metadata = self.metadata();
        std::ptr::from_raw_parts_mut(data_address, metadata)
    }
    pub unsafe fn as_ref<'a>(self) -> &'a T {
        &*self.as_ptr()
    }
    pub unsafe fn as_mut<'a>(self) -> &'a mut T {
        &mut *self.as_mut_ptr()
    }
}

#[thread_local]
static CONTEXT: Cell<Option<NonNull<Context>>> = Cell::new(None);

#[thread_local]
static OFFSETS: [Cell<usize>; 2] = [Cell::new(0), Cell::new(0)];

pub unsafe fn given(p: NonNull<Context>) -> impl Drop {
    pub struct Given;
    impl Drop for Given {
        fn drop(&mut self) {
            CONTEXT.take();
        }
    }
    let given = Given;
    CONTEXT.set(Some(p));
    OFFSETS[0].set(p.as_ref().block_ram.address());
    OFFSETS[1].set(p.as_ref().block_disk.address());
    given
}

pub fn using<'a>() -> &'a Context {
    let ctx = CONTEXT.get().expect("Never given a context to use.");
    unsafe { ctx.as_ref() }
}

pub struct Context {
    block_ram: Block,
    block_disk: Block,
}

impl Context {
    pub fn build(options: ContextOptions) -> anyhow::Result<Arc<Self>> {
        let block_ram = Block::build(options.block_ram.0, options.block_ram.1, Storage::Ram)?;
        let block_disk = Block::build(options.block_disk.0, options.block_disk.1, Storage::Disk)?;
        Ok(Arc::new(Self {
            block_ram,
            block_disk,
        }))
    }
    pub fn load(options: ContextOptions) -> anyhow::Result<Arc<Self>> {
        let block_ram = Block::load(options.block_ram.0, options.block_ram.1, Storage::Ram)?;
        let block_disk = Block::load(options.block_disk.0, options.block_disk.1, Storage::Disk)?;
        Ok(Arc::new(Self {
            block_ram,
            block_disk,
        }))
    }
    pub fn persist(&self) -> anyhow::Result<()> {
        self.block_ram.persist()?;
        self.block_disk.persist()?;
        Ok(())
    }
    pub fn allocate(&self, storage: Storage, layout: Layout) -> Result<Ptr<()>, AllocError> {
        use Storage::*;
        let offset = match storage {
            Ram => self.block_ram.allocate(layout),
            Disk => self.block_disk.allocate(layout),
        }?;
        let address = Address::new(storage, offset);
        let ptr = Ptr::new(address, ());
        Ok(ptr)
    }
    pub fn allocate_zeroed(&self, storage: Storage, layout: Layout) -> Result<Ptr<()>, AllocError> {
        use Storage::*;
        let offset = match storage {
            Ram => self.block_ram.allocate_zeroed(layout),
            Disk => self.block_disk.allocate_zeroed(layout),
        }?;
        let address = Address::new(storage, offset);
        let ptr = Ptr::new(address, ());
        Ok(ptr)
    }
    pub fn scope<'env, F, T>(&self, f: F) -> T
    where
        F: for<'scope> FnOnce(&'scope ContextScope<'scope, 'env>) -> T,
    {
        std::thread::scope(|scope| {
            f(unsafe { std::mem::transmute::<&Scope, &ContextScope>(scope) })
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextOptions {
    pub block_ram: (usize, String),
    pub block_disk: (usize, String),
}

#[repr(transparent)]
pub struct ContextScope<'scope, 'env: 'scope>(Scope<'scope, 'env>);

impl<'scope, 'env: 'scope> ContextScope<'scope, 'env> {
    pub fn spawn<F, T>(&'scope self, f: F) -> ScopedJoinHandle<'scope, T>
    where
        F: FnOnce() -> T + Send + 'scope,
        T: Send + 'scope,
    {
        struct AssertSend<T>(T);
        impl<T> AssertSend<T> {
            fn cosume(self) -> T {
                self.0
            }
        }
        unsafe impl<T> Send for AssertSend<T> {}
        let wrapped = AssertSend(CONTEXT.get().unwrap());
        self.0.spawn(move || {
            let context = wrapped.cosume();
            let _given = unsafe { given(context) };
            f()
        })
    }
}
