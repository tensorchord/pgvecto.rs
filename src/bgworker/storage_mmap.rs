use crate::prelude::{Id, Memmap};
use cstr::cstr;
use memmap2::MmapMut;
use std::alloc::Layout;
use std::borrow::{Borrow, BorrowMut};
use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::mem::MaybeUninit;
use std::ops::{Deref, DerefMut};
use std::os::fd::FromRawFd;

pub unsafe auto trait Pointerless {}

impl<T> !Pointerless for *const T {}
impl<T> !Pointerless for *mut T {}
impl<T> !Pointerless for &'_ T {}
impl<T> !Pointerless for &'_ mut T {}

pub type StorageMmapPreallocatorElement = (Memmap, Layout);

pub fn prealloc<T>(memmap: Memmap) -> StorageMmapPreallocatorElement {
    (memmap, std::alloc::Layout::new::<T>())
}

pub fn prealloc_slice<T>(memmap: Memmap, len: usize) -> StorageMmapPreallocatorElement {
    (memmap, std::alloc::Layout::array::<T>(len).unwrap())
}

pub struct StorageMmap {
    block_ram: Block,
    block_disk: Block,
}

impl StorageMmap {
    pub fn build(id: Id, iter: impl Iterator<Item = StorageMmapPreallocatorElement>) -> Self {
        let mut size_ram = 0usize;
        let mut size_disk = 0usize;
        for (memmap, layout) in iter {
            match memmap {
                Memmap::Ram => {
                    size_ram = size_ram.next_multiple_of(layout.align());
                    size_ram += layout.size();
                }
                Memmap::Disk => {
                    size_disk = size_disk.next_multiple_of(layout.align());
                    size_disk += layout.size();
                }
            }
        }
        let size_ram = size_ram.next_multiple_of(4096);
        let size_disk = size_disk.next_multiple_of(4096);
        let block_ram = Block::build(size_ram, format!("{}_ram", id), Memmap::Ram);
        let block_disk = Block::build(size_disk, format!("{}_disk", id), Memmap::Disk);
        Self {
            block_ram,
            block_disk,
        }
    }
    pub fn load(id: Id) -> Self {
        let block_ram = Block::load(format!("{}_ram", id), Memmap::Ram);
        let block_disk = Block::load(format!("{}_disk", id), Memmap::Disk);
        Self {
            block_ram,
            block_disk,
        }
    }
    pub fn alloc_mmap<T>(&mut self, memmap: Memmap) -> MmapBox<MaybeUninit<T>> {
        let ptr = match memmap {
            Memmap::Ram => self.block_ram.allocate(std::alloc::Layout::new::<T>()),
            Memmap::Disk => self.block_disk.allocate(std::alloc::Layout::new::<T>()),
        };
        MmapBox(ptr.cast())
    }
    pub fn alloc_mmap_slice<T>(&mut self, memmap: Memmap, len: usize) -> MmapBox<[MaybeUninit<T>]> {
        let ptr = match memmap {
            Memmap::Ram => self
                .block_ram
                .allocate(std::alloc::Layout::array::<T>(len).unwrap()),
            Memmap::Disk => self
                .block_disk
                .allocate(std::alloc::Layout::array::<T>(len).unwrap()),
        };
        MmapBox(unsafe { std::slice::from_raw_parts_mut(ptr.cast(), len) })
    }
    pub fn persist(&self) {
        self.block_ram.persist();
        self.block_disk.persist();
    }
}

struct Block {
    path: String,
    mmap: MmapMut,
    cursor: usize,
}

impl Block {
    fn build(size: usize, path: String, memmap: Memmap) -> Self {
        assert!(size % 4096 == 0);
        let file = tempfile(memmap).expect("Failed to create temp file.");
        file.set_len(size as u64)
            .expect("Failed to resize the file.");
        let mmap = unsafe { MmapMut::map_mut(&file).expect("Failed to create mmap.") };
        let _ = mmap.advise(memmap2::Advice::WillNeed);
        Self {
            path,
            mmap,
            cursor: 0,
        }
    }

    fn load(path: String, memmap: Memmap) -> Self {
        let mut file = tempfile(memmap).expect("Failed to create temp file.");
        let mut persistent_file = std::fs::OpenOptions::new()
            .read(true)
            .open(&path)
            .expect("Failed to read index.");
        std::io::copy(&mut persistent_file, &mut file).expect("Failed to write temp file.");
        let mmap = unsafe { MmapMut::map_mut(&file).expect("Failed to create mmap.") };
        Self {
            path,
            mmap,
            cursor: 0,
        }
    }

    fn allocate(&mut self, layout: Layout) -> *mut () {
        self.cursor = self.cursor.next_multiple_of(layout.align());
        let offset = self.cursor;
        self.cursor += layout.size();
        assert!(self.cursor <= self.mmap.len());
        unsafe { self.mmap.as_ptr().add(offset).cast_mut().cast() }
    }

    fn persist(&self) {
        use std::io::Write;
        let mut persistent_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&self.path)
            .expect("Failed to open the persistent file.");
        persistent_file
            .write_all(self.mmap.as_ref())
            .expect("Failed to write the persisent file.");
        persistent_file
            .sync_all()
            .expect("Failed to write the persisent file.");
    }
}

pub struct MmapBox<T: ?Sized>(*mut T);

impl<T> MmapBox<MaybeUninit<T>> {
    pub unsafe fn assume_init(self) -> MmapBox<T> {
        MmapBox(self.0.cast())
    }
}

impl<T> MmapBox<[MaybeUninit<T>]> {
    pub unsafe fn assume_init(self) -> MmapBox<[T]> {
        MmapBox(std::ptr::from_raw_parts_mut(
            self.0.cast(),
            std::ptr::metadata(self.0),
        ))
    }
}

unsafe impl<T: ?Sized + Send> Send for MmapBox<T> {}
unsafe impl<T: ?Sized + Sync> Sync for MmapBox<T> {}

impl<T: ?Sized> Deref for MmapBox<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.0 }
    }
}

impl<T: ?Sized> DerefMut for MmapBox<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.0 }
    }
}

impl<T: ?Sized> AsRef<T> for MmapBox<T> {
    fn as_ref(&self) -> &T {
        unsafe { &*self.0 }
    }
}

impl<T: ?Sized> AsMut<T> for MmapBox<T> {
    fn as_mut(&mut self) -> &mut T {
        unsafe { &mut *self.0 }
    }
}

impl<T: ?Sized> Borrow<T> for MmapBox<T> {
    fn borrow(&self) -> &T {
        unsafe { &*self.0 }
    }
}

impl<T: ?Sized> BorrowMut<T> for MmapBox<T> {
    fn borrow_mut(&mut self) -> &mut T {
        unsafe { &mut *self.0 }
    }
}

impl<T: ?Sized + Debug> Debug for MmapBox<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self.deref(), f)
    }
}

fn tempfile(memmap: Memmap) -> std::io::Result<File> {
    use Memmap::*;
    let file = match memmap {
        Disk => tempfile::tempfile()?,
        Ram => unsafe {
            let fd = libc::memfd_create(cstr!("file").as_ptr(), 0);
            if fd != -1 {
                File::from_raw_fd(fd)
            } else {
                return Err(std::io::Error::last_os_error());
            }
        },
    };
    Ok(file)
}
