use base::index::{IndexOptions, SearchOptions};
use base::operator::Borrowed;
use base::operator::Operator;
use base::search::{Collection, Element, Payload, Source, Vectors};
use base::vector::{VectorKind, VectorBorrowed};
use common::dir_ops::sync_dir;
use common::json::Json;
use common::mmap_array::MmapArray;
use common::remap::RemappedCollection;
use quantization::{operator::OperatorQuantization, Quantization};
use storage::{OperatorStorage, Storage};

use std::cmp::Reverse;
use std::collections::{BinaryHeap, BTreeMap, HashSet};
use std::path::Path;
use std::fs::create_dir;

pub trait OperatorInverted: Operator + OperatorQuantization + OperatorStorage {}

impl<T: Operator + OperatorQuantization + OperatorStorage> OperatorInverted for T {}

pub struct Inverted<O: OperatorInverted> {
    storage: O::Storage,
    quantization: Quantization<O>,
    payloads: MmapArray<Payload>,
    indexes: Json<Vec<u32>>,
    offsets: Json<Vec<u32>>,
}

impl<O: OperatorInverted> Inverted<O> {
    pub fn create(path: impl AsRef<Path>, options: IndexOptions, source: &impl Source<O>) -> Self {
        if options.vector.v != VectorKind::SVecf32 {
            panic!("inverted index only supports `SVecf32` vectors")
        }
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
        let mut visited = HashSet::new();
        let mut result = BinaryHeap::new();
        for (token, _) in vector.to_index_vec() {
            let start = self.offsets[token as usize];
            let end = self.offsets[token as usize + 1];
            for j in &self.indexes[(start as usize)..(end as usize)] {
                if visited.contains(j) {
                    continue;
                }
                visited.insert(j);
                result.push(Reverse(Element {
                    distance: self.quantization.distance(&self.storage, vector, *j),
                    payload: self.payloads[*j as usize],
                }))
            }
        }
        result
    }

    pub fn vbase<'a>(
        &'a self,
        vector: Borrowed<'a, O>,
        _: &'a SearchOptions,
    ) -> (Vec<Element>, Box<(dyn Iterator<Item = Element> + 'a)>) {
        let mut visited = HashSet::new();
        let mut result = Vec::new();
        for (token, _) in vector.to_index_vec() {
            let start = self.offsets[token as usize];
            let end = self.offsets[token as usize + 1];
            for j in &self.indexes[(start as usize)..(end as usize)] {
                if visited.contains(j) {
                    continue;
                }
                visited.insert(j);
                result.push(Element {
                    distance: self.quantization.distance(&self.storage, vector, *j),
                    payload: self.payloads[*j as usize],
                })
            }
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

fn from_nothing<O: OperatorInverted>(
    path: impl AsRef<Path>,
    options: IndexOptions,
    collection: &impl Collection<O>,
) -> Inverted<O> {
    create_dir(path.as_ref()).expect("failed to create path for inverted index");

    let inverted_options = options.indexing.clone().unwrap_inverted();
    let mut token_collection = BTreeMap::new();
    for i in 0..collection.len() {
        for (token, _) in collection.vector(i).to_index_vec() {
            token_collection.entry(token).or_insert_with(Vec::new).push(i);
        }
    }
    let (indexes, offsets) = build_compressed_matrix(token_collection);

    let storage = O::Storage::create(path.as_ref().join("storage"), collection);
    let quantization = Quantization::create(
        path.as_ref().join("quantization"),
        options.clone(),
        inverted_options.quantization,
        collection,
    );
    let payloads = MmapArray::create(
        path.as_ref().join("payloads"),
        (0..collection.len()).map(|i| collection.payload(i)),
    );
    let json_index = Json::create(path.as_ref().join("indexes"), indexes);
    let json_offset = Json::create(path.as_ref().join("offsets"), offsets);
    sync_dir(path);
    Inverted {
        storage,
        quantization,
        payloads,
        indexes: json_index,
        offsets: json_offset,
    
    }
}

fn open<O: OperatorInverted>(path: impl AsRef<Path>) -> Inverted<O> {
    let storage = O::Storage::open(path.as_ref().join("storage"));
    let quantization = Quantization::open(path.as_ref().join("quantization"));
    let payloads = MmapArray::open(path.as_ref().join("payloads"));
    let offsets = Json::open(path.as_ref().join("offsets"));
    let indexes = Json::open(path.as_ref().join("indexes"));
    Inverted {
        storage,
        quantization,
        payloads,
        indexes,
        offsets,
    }
}

fn build_compressed_matrix(token_collection: BTreeMap<u32, Vec<u32>>) -> (Vec<u32>, Vec<u32>) {
    let mut indexes = Vec::new();
    let mut offsets = Vec::new();

    let mut i = 0;
    let mut last: u32 = 0;
    offsets.push(0);
    for (token, ids) in token_collection.iter() {
        while *token != i {
            offsets.push(last);
            i += 1;
        }
        indexes.extend_from_slice(ids);
        last += ids.len() as u32;
        offsets.push(last);
        i += 1;
    }

    (indexes, offsets)
}
