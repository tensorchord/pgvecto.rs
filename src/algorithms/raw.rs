use crate::index::segments::growing::GrowingSegment;
use crate::index::segments::sealed::SealedSegment;
use crate::index::IndexOptions;
use crate::prelude::Scalar;
use crate::utils::mmap_array::MmapArray;
use std::path::PathBuf;
use std::sync::Arc;

pub struct Raw {
    mmap: RawMmap,
}

impl Raw {
    pub fn create(
        path: PathBuf,
        options: IndexOptions,
        sealed: Vec<Arc<SealedSegment>>,
        growing: Vec<Arc<GrowingSegment>>,
    ) -> Self {
        std::fs::create_dir(&path).unwrap();
        let ram = make(sealed, growing, options);
        let mmap = save(ram, path.clone());
        crate::utils::dir_ops::sync_dir(&path);
        Self { mmap }
    }

    pub fn open(path: PathBuf, options: IndexOptions) -> Self {
        let mmap = load(path.clone(), options);
        Self { mmap }
    }

    pub fn len(&self) -> u32 {
        self.mmap.len()
    }

    pub fn vector(&self, i: u32) -> &[Scalar] {
        self.mmap.vector(i)
    }

    pub fn data(&self, i: u32) -> u64 {
        self.mmap.data(i)
    }
}

unsafe impl Send for Raw {}
unsafe impl Sync for Raw {}

struct RawRam {
    sealed: Vec<Arc<SealedSegment>>,
    growing: Vec<Arc<GrowingSegment>>,
    dims: u16,
}

impl RawRam {
    fn len(&self) -> u32 {
        self.sealed.iter().map(|x| x.len()).sum::<u32>()
            + self.growing.iter().map(|x| x.len()).sum::<u32>()
    }
    fn vector(&self, mut index: u32) -> &[Scalar] {
        for x in self.sealed.iter() {
            if index < x.len() {
                return x.vector(index);
            }
            index -= x.len();
        }
        for x in self.growing.iter() {
            if index < x.len() {
                return x.vector(index);
            }
            index -= x.len();
        }
        panic!("Out of bound.")
    }
    fn data(&self, mut index: u32) -> u64 {
        for x in self.sealed.iter() {
            if index < x.len() {
                return x.data(index);
            }
            index -= x.len();
        }
        for x in self.growing.iter() {
            if index < x.len() {
                return x.data(index);
            }
            index -= x.len();
        }
        panic!("Out of bound.")
    }
}

struct RawMmap {
    vectors: MmapArray<Scalar>,
    data: MmapArray<u64>,
    dims: u16,
}

impl RawMmap {
    fn len(&self) -> u32 {
        self.data.len() as u32
    }

    fn vector(&self, i: u32) -> &[Scalar] {
        let s = i as usize * self.dims as usize;
        let e = (i + 1) as usize * self.dims as usize;
        &self.vectors[s..e]
    }

    fn data(&self, i: u32) -> u64 {
        self.data[i as usize]
    }
}

unsafe impl Send for RawMmap {}
unsafe impl Sync for RawMmap {}

fn make(
    sealed: Vec<Arc<SealedSegment>>,
    growing: Vec<Arc<GrowingSegment>>,
    options: IndexOptions,
) -> RawRam {
    RawRam {
        sealed,
        growing,
        dims: options.vector.dims,
    }
}

fn save(ram: RawRam, path: PathBuf) -> RawMmap {
    let n = ram.len();
    let vectors_iter = (0..n).flat_map(|i| ram.vector(i)).copied();
    let data_iter = (0..n).map(|i| ram.data(i));
    let vectors = MmapArray::create(path.join("vectors"), vectors_iter);
    let data = MmapArray::create(path.join("data"), data_iter);
    RawMmap {
        vectors,
        data,
        dims: ram.dims,
    }
}

fn load(path: PathBuf, options: IndexOptions) -> RawMmap {
    let vectors: MmapArray<Scalar> = MmapArray::open(path.join("vectors"));
    let data = MmapArray::open(path.join("data"));
    RawMmap {
        vectors,
        data,
        dims: options.vector.dims,
    }
}
