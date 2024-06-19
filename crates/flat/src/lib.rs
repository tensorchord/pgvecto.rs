#![allow(clippy::len_without_is_empty)]

use base::index::*;
use base::operator::*;
use base::search::*;
use common::dir_ops::sync_dir;
use common::mmap_array::MmapArray;
use common::remap::RemappedCollection;
use quantization::operator::OperatorQuantization;
use quantization::Quantization;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::create_dir;
use std::path::Path;
use storage::OperatorStorage;
use storage::Storage;

pub trait OperatorFlat: Operator + OperatorQuantization + OperatorStorage {}

impl<T: Operator + OperatorQuantization + OperatorStorage> OperatorFlat for T {}

pub struct Flat<O: OperatorFlat> {
    storage: O::Storage,
    quantization: Quantization<O>,
    payloads: MmapArray<Payload>,
}

impl<O: OperatorFlat> Flat<O> {
    pub fn create(path: impl AsRef<Path>, options: IndexOptions, source: &impl Source<O>) -> Self {
        let remapped = RemappedCollection::from_source(source);
        from_nothing(path, options, &remapped)
    }

    pub fn open(path: impl AsRef<Path>) -> Self {
        open(path)
    }

    pub fn basic(
        &self,
        vector: Borrowed<'_, O>,
        _: &SearchOptions,
    ) -> BinaryHeap<Reverse<Element>> {
        let mut result = BinaryHeap::new();
        for i in 0..self.storage.len() {
            let distance = self.quantization.distance(&self.storage, vector, i);
            let payload = self.payloads[i as usize];
            result.push(Reverse(Element { distance, payload }));
        }
        result
    }

    pub fn vbase<'a>(
        &'a self,
        vector: Borrowed<'a, O>,
        _: &'a SearchOptions,
    ) -> (Vec<Element>, Box<(dyn Iterator<Item = Element> + 'a)>) {
        let mut result = Vec::new();
        for i in 0..self.storage.len() {
            let distance = self.quantization.distance(&self.storage, vector, i);
            let payload = self.payloads[i as usize];
            result.push(Element { distance, payload });
        }
        (result, Box::new(std::iter::empty()))
    }

    pub fn len(&self) -> u32 {
        self.storage.len()
    }

    pub fn vector(&self, i: u32) -> Borrowed<'_, O> {
        self.storage.vector(i)
    }

    pub fn payload(&self, i: u32) -> Payload {
        self.payloads[i as usize]
    }
}

fn from_nothing<O: OperatorFlat>(
    path: impl AsRef<Path>,
    options: IndexOptions,
    collection: &impl Collection<O>,
) -> Flat<O> {
    create_dir(path.as_ref()).unwrap();
    let flat_indexing_options = options.indexing.clone().unwrap_flat();
    let storage = O::Storage::create(path.as_ref().join("storage"), collection);
    let quantization = Quantization::create(
        path.as_ref().join("quantization"),
        options.clone(),
        flat_indexing_options.quantization,
        collection,
    );
    let payloads = MmapArray::create(
        path.as_ref().join("payloads"),
        (0..collection.len()).map(|i| collection.payload(i)),
    );
    sync_dir(path.as_ref());
    Flat {
        storage,
        quantization,
        payloads,
    }
}

fn open<O: OperatorFlat>(path: impl AsRef<Path>) -> Flat<O> {
    let storage = O::Storage::open(path.as_ref().join("storage"));
    let quantization = Quantization::open(path.as_ref().join("quantization"));
    let payloads = MmapArray::open(path.as_ref().join("payloads"));
    Flat {
        storage,
        quantization,
        payloads,
    }
}
