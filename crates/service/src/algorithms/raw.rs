use crate::index::segments::growing::GrowingSegment;
use crate::index::segments::sealed::SealedSegment;
use crate::index::IndexOptions;
use crate::prelude::*;
use std::path::Path;
use std::sync::Arc;

pub struct Raw<S: G> {
    mmap: S::Storage,
}

impl<S: G> Raw<S> {
    pub fn create<I: for<'a> G<VectorRef<'a> = S::VectorRef<'a>>>(
        path: &Path,
        options: IndexOptions,
        sealed: Vec<Arc<SealedSegment<I>>>,
        growing: Vec<Arc<GrowingSegment<I>>>,
    ) -> Self {
        std::fs::create_dir(path).unwrap();
        let ram = make(sealed, growing, options);
        let mmap = S::Storage::save(path, ram);
        crate::utils::dir_ops::sync_dir(path);
        Self { mmap }
    }
}

impl<S: G> Raw<S> {
    pub fn dims(&self) -> u16 {
        self.mmap.dims()
    }

    pub fn len(&self) -> u32 {
        self.mmap.len()
    }

    pub fn content(&self, i: u32) -> S::VectorRef<'_> {
        self.mmap.content(i)
    }

    pub fn payload(&self, i: u32) -> Payload {
        self.mmap.payload(i)
    }

    pub fn load(path: &Path, options: IndexOptions) -> Self {
        Self {
            mmap: S::Storage::load(path, options),
        }
    }
}

unsafe impl<S: G> Send for Raw<S> {}
unsafe impl<S: G> Sync for Raw<S> {}

pub struct RawRam<S: G> {
    sealed: Vec<Arc<SealedSegment<S>>>,
    growing: Vec<Arc<GrowingSegment<S>>>,
    dims: u16,
}

impl<S: G> RawRam<S> {
    pub fn dims(&self) -> u16 {
        self.dims
    }

    pub fn len(&self) -> u32 {
        self.sealed.iter().map(|x| x.len()).sum::<u32>()
            + self.growing.iter().map(|x| x.len()).sum::<u32>()
    }

    pub fn content(&self, mut index: u32) -> S::VectorRef<'_> {
        for x in self.sealed.iter() {
            if index < x.len() {
                return x.content(index);
            }
            index -= x.len();
        }
        for x in self.growing.iter() {
            if index < x.len() {
                return x.content(index);
            }
            index -= x.len();
        }
        panic!("Out of bound.")
    }

    pub fn payload(&self, mut index: u32) -> Payload {
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
