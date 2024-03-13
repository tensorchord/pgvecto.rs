#![feature(trait_alias)]
#![allow(clippy::len_without_is_empty)]

use base::index::*;
use base::operator::*;
use base::search::*;
use common::dir_ops::sync_dir;
use quantization::operator::OperatorQuantization;
use quantization::Quantization;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::create_dir;
use std::path::Path;
use std::sync::Arc;
use storage::operator::OperatorStorage;
use storage::StorageCollection;

pub trait OperatorFlat = Operator + OperatorQuantization + OperatorStorage;

pub struct Flat<O: OperatorFlat> {
    mmap: FlatMmap<O>,
}

impl<O: OperatorFlat> Flat<O> {
    pub fn create<S: Source<O>>(path: &Path, options: IndexOptions, source: &S) -> Self {
        create_dir(path).unwrap();
        let ram = make(path, options, source);
        let mmap = save(path, ram);
        sync_dir(path);
        Self { mmap }
    }

    pub fn open(path: &Path, options: IndexOptions) -> Self {
        let mmap = open(path, options);
        Self { mmap }
    }

    pub fn basic(
        &self,
        vector: Borrowed<'_, O>,
        _opts: &SearchOptions,
        filter: impl Filter,
    ) -> BinaryHeap<Reverse<Element>> {
        basic(&self.mmap, vector, filter)
    }

    pub fn vbase<'a>(
        &'a self,
        vector: Borrowed<'a, O>,
        _opts: &'a SearchOptions,
        filter: impl Filter + 'a,
    ) -> (Vec<Element>, Box<(dyn Iterator<Item = Element> + 'a)>) {
        vbase(&self.mmap, vector, filter)
    }

    pub fn len(&self) -> u32 {
        self.mmap.storage.len()
    }

    pub fn vector(&self, i: u32) -> Borrowed<'_, O> {
        self.mmap.storage.vector(i)
    }

    pub fn payload(&self, i: u32) -> Payload {
        self.mmap.storage.payload(i)
    }
}

unsafe impl<O: OperatorFlat> Send for Flat<O> {}
unsafe impl<O: OperatorFlat> Sync for Flat<O> {}

pub struct FlatRam<O: OperatorFlat> {
    storage: Arc<StorageCollection<O>>,
    quantization: Quantization<O, StorageCollection<O>>,
}

pub struct FlatMmap<O: OperatorFlat> {
    storage: Arc<StorageCollection<O>>,
    quantization: Quantization<O, StorageCollection<O>>,
}

unsafe impl<O: OperatorFlat> Send for FlatMmap<O> {}
unsafe impl<O: OperatorFlat> Sync for FlatMmap<O> {}

pub fn make<O: OperatorFlat, S: Source<O>>(
    path: &Path,
    options: IndexOptions,
    source: &S,
) -> FlatRam<O> {
    let idx_opts = options.indexing.clone().unwrap_flat();
    let storage = Arc::new(StorageCollection::create(&path.join("raw"), source));
    let quantization = Quantization::create(
        &path.join("quantization"),
        options.clone(),
        idx_opts.quantization,
        &storage,
        (0..storage.len()).collect::<Vec<_>>(),
    );
    FlatRam {
        storage,
        quantization,
    }
}

pub fn save<O: OperatorFlat>(_: &Path, ram: FlatRam<O>) -> FlatMmap<O> {
    FlatMmap {
        storage: ram.storage,
        quantization: ram.quantization,
    }
}

pub fn open<O: OperatorFlat>(path: &Path, options: IndexOptions) -> FlatMmap<O> {
    let idx_opts = options.indexing.clone().unwrap_flat();
    let storage = Arc::new(StorageCollection::open(&path.join("raw"), options.clone()));
    rayon::check();
    let quantization = Quantization::open(
        &path.join("quantization"),
        options.clone(),
        idx_opts.quantization,
        &storage,
    );
    rayon::check();
    FlatMmap {
        storage,
        quantization,
    }
}

pub fn basic<O: OperatorFlat>(
    mmap: &FlatMmap<O>,
    vector: Borrowed<'_, O>,
    mut filter: impl Filter,
) -> BinaryHeap<Reverse<Element>> {
    let mut result = BinaryHeap::new();
    for i in 0..mmap.storage.len() {
        let distance = mmap.quantization.distance(vector, i);
        let payload = mmap.storage.payload(i);
        if filter.check(payload) {
            result.push(Reverse(Element { distance, payload }));
        }
    }
    result
}

pub fn vbase<'a, O: OperatorFlat>(
    mmap: &'a FlatMmap<O>,
    vector: Borrowed<'a, O>,
    mut filter: impl Filter + 'a,
) -> (Vec<Element>, Box<dyn Iterator<Item = Element> + 'a>) {
    let mut result = Vec::new();
    for i in 0..mmap.storage.len() {
        let distance = mmap.quantization.distance(vector, i);
        let payload = mmap.storage.payload(i);
        if filter.check(payload) {
            result.push(Element { distance, payload });
        }
    }
    (result, Box::new(std::iter::empty()))
}
