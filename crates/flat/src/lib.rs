#![allow(clippy::len_without_is_empty)]

use base::index::*;
use base::operator::*;
use base::search::*;
use base::vector::VectorBorrowed;
use common::mmap_array::MmapArray;
use common::remap::RemappedCollection;
use quantization::operator::OperatorQuantization;
use quantization::Quantization;
use std::fs::create_dir;
use std::path::Path;
use storage::OperatorStorage;
use storage::Storage;

pub trait OperatorFlat: OperatorQuantization + OperatorStorage {}

impl<T: OperatorQuantization + OperatorStorage> OperatorFlat for T {}

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

    pub fn vbase<'a>(
        &'a self,
        vector: Borrowed<'a, O>,
        opts: &'a SearchOptions,
    ) -> (Vec<Element>, Box<dyn Iterator<Item = Element> + 'a>) {
        let mut reranker = self.quantization.flat_rerank(vector, opts, move |u| {
            (
                O::distance(vector, self.storage.vector(u)),
                self.payloads[u as usize],
            )
        });
        for i in 0..self.storage.len() {
            reranker.push(i, ());
        }
        (
            Vec::new(),
            Box::new(std::iter::from_fn(move || {
                reranker.pop().map(|(dis_u, _, payload_u)| Element {
                    distance: dis_u,
                    payload: payload_u,
                })
            })),
        )
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
        options.vector,
        flat_indexing_options.quantization,
        collection,
        |vector| vector.own(),
    );
    let payloads = MmapArray::create(
        path.as_ref().join("payloads"),
        (0..collection.len()).map(|i| collection.payload(i)),
    );
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
