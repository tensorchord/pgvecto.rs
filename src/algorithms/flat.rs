use super::quantization::Quantization;
use super::raw::Raw;
use crate::index::segments::growing::GrowingSegment;
use crate::index::segments::sealed::SealedSegment;
use crate::index::IndexOptions;
use crate::prelude::*;
use crate::utils::dir_ops::sync_dir;
use std::fs::create_dir;
use std::path::PathBuf;
use std::sync::Arc;

pub struct Flat {
    mmap: FlatMmap,
}

impl Flat {
    pub fn create(
        path: PathBuf,
        options: IndexOptions,
        sealed: Vec<Arc<SealedSegment>>,
        growing: Vec<Arc<GrowingSegment>>,
    ) -> Self {
        create_dir(&path).unwrap();
        let ram = make(path.clone(), sealed, growing, options.clone());
        let mmap = save(ram, path.clone());
        sync_dir(&path);
        Self { mmap }
    }
    pub fn open(path: PathBuf, options: IndexOptions) -> Self {
        let mmap = load(path, options.clone());
        Self { mmap }
    }

    pub fn len(&self) -> u32 {
        self.mmap.raw.len()
    }

    pub fn vector(&self, i: u32) -> &[Scalar] {
        self.mmap.raw.vector(i)
    }

    pub fn payload(&self, i: u32) -> Payload {
        self.mmap.raw.payload(i)
    }

    pub fn search<F: FnMut(Payload) -> bool>(&self, k: usize, vector: &[Scalar], f: F) -> Heap {
        search(&self.mmap, k, vector, f)
    }
}

unsafe impl Send for Flat {}
unsafe impl Sync for Flat {}

pub struct FlatRam {
    raw: Arc<Raw>,
    quantization: Quantization,
    d: Distance,
}

pub struct FlatMmap {
    raw: Arc<Raw>,
    quantization: Quantization,
    d: Distance,
}

unsafe impl Send for FlatMmap {}
unsafe impl Sync for FlatMmap {}

pub fn make(
    path: PathBuf,
    sealed: Vec<Arc<SealedSegment>>,
    growing: Vec<Arc<GrowingSegment>>,
    options: IndexOptions,
) -> FlatRam {
    let idx_opts = options.indexing.clone().unwrap_flat();
    let raw = Arc::new(Raw::create(
        path.join("raw"),
        options.clone(),
        sealed,
        growing,
    ));
    let quantization = Quantization::create(
        path.join("quantization"),
        options.clone(),
        idx_opts.quantization,
        &raw,
    );
    FlatRam {
        raw,
        quantization,
        d: options.vector.d,
    }
}

pub fn save(ram: FlatRam, _: PathBuf) -> FlatMmap {
    FlatMmap {
        raw: ram.raw,
        quantization: ram.quantization,
        d: ram.d,
    }
}

pub fn load(path: PathBuf, options: IndexOptions) -> FlatMmap {
    let idx_opts = options.indexing.clone().unwrap_flat();
    let raw = Arc::new(Raw::open(path.join("raw"), options.clone()));
    let quantization = Quantization::open(
        path.join("quantization"),
        options.clone(),
        idx_opts.quantization,
        &raw,
    );
    FlatMmap {
        raw,
        quantization,
        d: options.vector.d,
    }
}

pub fn search<F: FnMut(Payload) -> bool>(
    mmap: &FlatMmap,
    k: usize,
    vector: &[Scalar],
    mut f: F,
) -> Heap {
    let mut result = Heap::new(k);
    for i in 0..mmap.raw.len() {
        let distance = mmap.quantization.distance(mmap.d, vector, i);
        let payload = mmap.raw.payload(i);
        if f(payload) {
            result.push(HeapElement { distance, payload });
        }
    }
    result
}
