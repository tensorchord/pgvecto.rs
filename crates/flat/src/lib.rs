#![allow(clippy::len_without_is_empty)]

use base::always_equal::AlwaysEqual;
use base::index::*;
use base::operator::*;
use base::search::*;
use base::vector::VectorBorrowed;
use base::vector::VectorOwned;
use common::mmap_array::MmapArray;
use common::remap::RemappedCollection;
use quantization::quantizer::Quantizer;
use quantization::Quantization;
use std::fs::create_dir;
use std::path::Path;
use storage::OperatorStorage;
use storage::Storage;

pub trait OperatorFlat: OperatorStorage {}

impl<T: OperatorStorage> OperatorFlat for T {}

pub struct Flat<O: OperatorFlat, Q: Quantizer<O>> {
    storage: O::Storage,
    quantization: Quantization<O, Q>,
    payloads: MmapArray<Payload>,
}

impl<O: OperatorFlat, Q: Quantizer<O>> Flat<O, Q> {
    pub fn create(
        path: impl AsRef<Path>,
        options: IndexOptions,
        source: &(impl Vectors<O::Vector> + Collection + Source + Sync),
    ) -> Self {
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
    ) -> Box<dyn Iterator<Item = Element> + 'a> {
        let mut heap = Q::flat_rerank_start();
        let lut = self
            .quantization
            .flat_rerank_preprocess(self.quantization.project(vector).as_borrowed(), opts);
        self.quantization
            .flat_rerank_continue(&lut, 0..self.storage.len(), &mut heap);
        let mut reranker = self.quantization.flat_rerank_break(
            heap,
            move |u| (O::distance(vector, self.storage.vector(u)), ()),
            opts,
        );
        Box::new(std::iter::from_fn(move || {
            reranker.pop().map(|(dis_u, u, ())| Element {
                distance: dis_u,
                payload: AlwaysEqual(self.payload(u)),
            })
        }))
    }

    pub fn dims(&self) -> u32 {
        self.storage.dims()
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

fn from_nothing<O: OperatorFlat, Q: Quantizer<O>>(
    path: impl AsRef<Path>,
    options: IndexOptions,
    collection: &(impl Vectors<O::Vector> + Collection + Sync),
) -> Flat<O, Q> {
    create_dir(path.as_ref()).unwrap();
    let flat_indexing_options = options.indexing.clone().unwrap_flat();
    let storage = O::Storage::create(path.as_ref().join("storage"), collection);
    let quantization = Quantization::<O, Q>::create(
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

fn open<O: OperatorFlat, Q: Quantizer<O>>(path: impl AsRef<Path>) -> Flat<O, Q> {
    let storage = O::Storage::open(path.as_ref().join("storage"));
    let quantization = Quantization::open(path.as_ref().join("quantization"));
    let payloads = MmapArray::open(path.as_ref().join("payloads"));
    Flat {
        storage,
        quantization,
        payloads,
    }
}
