use crate::index::segments::growing::GrowingSegment;
use crate::index::segments::sealed::SealedSegment;
use crate::index::IndexOptions;
use crate::prelude::*;
use crate::utils::mmap_array::MmapArray;
use std::path::PathBuf;
use std::sync::Arc;

pub struct Raw<S: G> {
    mmap: RawMmap<S>,
}

impl<S: G> Raw<S> {
    pub fn create(
        path: PathBuf,
        options: IndexOptions,
        sealed: Vec<Arc<SealedSegment<S>>>,
        growing: Vec<Arc<GrowingSegment<S>>>,
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

    pub fn vector(&self, i: u32) -> &[S::Scalar] {
        self.mmap.vector(i)
    }

    pub fn payload(&self, i: u32) -> Payload {
        self.mmap.payload(i)
    }
}

unsafe impl<S: G> Send for Raw<S> {}
unsafe impl<S: G> Sync for Raw<S> {}

struct RawRam<S: G> {
    sealed: Vec<Arc<SealedSegment<S>>>,
    growing: Vec<Arc<GrowingSegment<S>>>,
    dims: u16,
}

impl<S: G> RawRam<S> {
    fn len(&self) -> u32 {
        self.sealed.iter().map(|x| x.len()).sum::<u32>()
            + self.growing.iter().map(|x| x.len()).sum::<u32>()
    }
    fn vector(&self, mut index: u32) -> &[S::Scalar] {
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
    fn payload(&self, mut index: u32) -> Payload {
        for x in self.sealed.iter() {
            if index < x.len() {
                return x.payload(index);
            }
            index -= x.len();
        }
        for x in self.growing.iter() {
            if index < x.len() {
                return x.payload(index);
            }
            index -= x.len();
        }
        panic!("Out of bound.")
    }
}

struct RawMmap<S: G> {
    vectors: MmapArray<S::Scalar>,
    payload: MmapArray<Payload>,
    dims: u16,
}

impl<S: G> RawMmap<S> {
    fn len(&self) -> u32 {
        self.payload.len() as u32
    }

    fn vector(&self, i: u32) -> &[S::Scalar] {
        let s = i as usize * self.dims as usize;
        let e = (i + 1) as usize * self.dims as usize;
        &self.vectors[s..e]
    }

    fn payload(&self, i: u32) -> Payload {
        self.payload[i as usize]
    }
}

unsafe impl<S: G> Send for RawMmap<S> {}
unsafe impl<S: G> Sync for RawMmap<S> {}

fn make<S: G>(
    sealed: Vec<Arc<SealedSegment<S>>>,
    growing: Vec<Arc<GrowingSegment<S>>>,
    options: IndexOptions,
) -> RawRam<S> {
    RawRam {
        sealed,
        growing,
        dims: options.vector.dims,
    }
}

fn save<S: G>(ram: RawRam<S>, path: PathBuf) -> RawMmap<S> {
    let n = ram.len();
    let vectors_iter = (0..n).flat_map(|i| ram.vector(i)).copied();
    let payload_iter = (0..n).map(|i| ram.payload(i));
    let vectors = MmapArray::create(path.join("vectors"), vectors_iter);
    let payload = MmapArray::create(path.join("payload"), payload_iter);
    RawMmap {
        vectors,
        payload,
        dims: ram.dims,
    }
}

fn load<S: G>(path: PathBuf, options: IndexOptions) -> RawMmap<S> {
    let vectors = MmapArray::open(path.join("vectors"));
    let payload = MmapArray::open(path.join("payload"));
    RawMmap {
        vectors,
        payload,
        dims: options.vector.dims,
    }
}
