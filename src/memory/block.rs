use crate::prelude::*;
use cstr::cstr;
use memmap2::MmapMut;
use std::alloc::{AllocError, Layout};
use std::fs::{File, OpenOptions};
use std::os::fd::FromRawFd;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct Block {
    #[allow(dead_code)]
    size: usize,
    path: String,
    mmap: MmapMut,
    bump: Bump,
}

impl Block {
    pub fn build(size: usize, path: String, storage: Storage) -> anyhow::Result<Self> {
        anyhow::ensure!(size % 4096 == 0);
        let file = tempfile(storage)?;
        file.set_len(size as u64)?;
        let mut mmap = unsafe { MmapMut::map_mut(&file) }?;
        mmap.advise(memmap2::Advice::WillNeed)?;
        let bump = unsafe { Bump::build(size, mmap.as_mut_ptr()) };
        Ok(Self {
            size,
            path,
            mmap,
            bump,
        })
    }

    pub fn load(size: usize, path: String, storage: Storage) -> anyhow::Result<Self> {
        anyhow::ensure!(size % 4096 == 0);
        let mut file = tempfile(storage)?;
        let mut persistent_file = std::fs::OpenOptions::new().read(true).open(&path)?;
        std::io::copy(&mut persistent_file, &mut file)?;
        let mut mmap = unsafe { MmapMut::map_mut(&file) }?;
        let bump = unsafe { Bump::load(size, mmap.as_mut_ptr()) };
        Ok(Self {
            size,
            path,
            mmap,
            bump,
        })
    }

    pub fn persist(&self) -> anyhow::Result<()> {
        use std::io::Write;
        let mut persistent_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        persistent_file.write_all(self.mmap.as_ref())?;
        persistent_file.sync_all()?;
        Ok(())
    }

    pub fn address(&self) -> usize {
        self.mmap.as_ptr() as usize
    }

    pub fn allocate(&self, layout: Layout) -> Result<usize, AllocError> {
        self.bump.allocate(layout)
    }

    pub fn allocate_zeroed(&self, layout: Layout) -> Result<usize, AllocError> {
        self.bump.allocate_zeroed(layout)
    }
}

pub struct Bump {
    size: usize,
    space: *mut Header,
}

impl Bump {
    pub unsafe fn build(size: usize, addr: *mut u8) -> Self {
        assert!(size >= 4096);
        let space = addr.cast::<Header>();
        space.write(Header {
            cursor: AtomicUsize::new(4096),
            objects: AtomicUsize::new(0),
        });
        Self { size, space }
    }
    pub unsafe fn load(size: usize, addr: *mut u8) -> Self {
        assert!(size >= 4096);
        let space = addr.cast::<Header>();
        Self { size, space }
    }
    pub fn allocate(&self, layout: Layout) -> Result<usize, AllocError> {
        if layout.size() == 0 {
            return Ok(0);
        }
        if layout.align() > 128 {
            return Err(AllocError);
        }
        let mut old = unsafe { (*self.space).cursor.load(Ordering::Relaxed) };
        let offset = loop {
            let offset = (old + layout.align() - 1) & !(layout.align() - 1);
            let new = offset + layout.size();
            if new > self.size {
                return Err(AllocError);
            }
            let exchange = unsafe {
                (*self.space).cursor.compare_exchange_weak(
                    old,
                    new,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
            };
            let Err(_old) = exchange else { break offset };
            old = _old;
        };
        unsafe {
            (*self.space).objects.fetch_add(1, Ordering::Relaxed);
        }
        Ok(offset)
    }
    pub fn allocate_zeroed(&self, layout: Layout) -> Result<usize, AllocError> {
        self.allocate(layout)
    }
}

unsafe impl Send for Bump {}
unsafe impl Sync for Bump {}

#[repr(C)]
struct Header {
    cursor: AtomicUsize,
    objects: AtomicUsize,
}

fn tempfile(storage: Storage) -> anyhow::Result<File> {
    use Storage::*;
    let file = match storage {
        Disk => tempfile::tempfile()?,
        Ram => unsafe {
            let fd = libc::memfd_create(cstr!("file").as_ptr(), 0);
            if fd != -1 {
                File::from_raw_fd(fd)
            } else {
                anyhow::bail!(std::io::Error::last_os_error());
            }
        },
    };
    Ok(file)
}
