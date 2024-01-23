use crate::index::segments::growing::GrowingSegment;
use crate::index::segments::sealed::SealedSegment;
use crate::index::IndexOptions;
use crate::prelude::*;
use std::path::Path;
use std::sync::Arc;

pub struct Raw<S> {
    mmap: S,
}

impl<S: AtomicStorage> Raw<S> {
    pub fn create<I: G<Element = S::Element>>(
        path: &Path,
        options: IndexOptions,
        sealed: Vec<Arc<SealedSegment<I>>>,
        growing: Vec<Arc<GrowingSegment<I>>>,
    ) -> Self {
        std::fs::create_dir(path).unwrap();
        let ram = make(sealed, growing, options);
        let mmap = S::save(path, ram);
        crate::utils::dir_ops::sync_dir(path);
        Self { mmap }
    }
}

impl<S: AtomicStorage> Storage for Raw<S> {
    type Element = S::Element;

    fn dims(&self) -> u16 {
        self.mmap.dims()
    }

    fn len(&self) -> u32 {
        self.mmap.len()
    }

    fn content(&self, i: u32) -> &[Self::Element] {
        self.mmap.content(i)
    }

    fn payload(&self, i: u32) -> Payload {
        self.mmap.payload(i)
    }

    fn load(path: &Path, options: IndexOptions) -> Self {
        Self {
            mmap: S::load(path, options),
        }
    }
}

unsafe impl<S: AtomicStorage> Send for Raw<S> {}
unsafe impl<S: AtomicStorage> Sync for Raw<S> {}

struct RawRam<S: G> {
    sealed: Vec<Arc<SealedSegment<S>>>,
    growing: Vec<Arc<GrowingSegment<S>>>,
    dims: u16,
}

impl<S: G> Storage for RawRam<S> {
    type Element = S::Element;

    fn dims(&self) -> u16 {
        self.dims
    }

    fn len(&self) -> u32 {
        self.sealed.iter().map(|x| x.len()).sum::<u32>()
            + self.growing.iter().map(|x| x.len()).sum::<u32>()
    }

    fn content(&self, mut index: u32) -> &[S::Element] {
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
