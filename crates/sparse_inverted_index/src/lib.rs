#![allow(clippy::len_without_is_empty)]

pub mod operator;

use self::operator::OperatorSparseInvertedIndex;
use base::always_equal::AlwaysEqual;
use base::distance::Distance;
use base::index::{IndexOptions, SearchOptions};
use base::operator::Borrowed;
use base::scalar::ScalarLike;
use base::search::{Collection, Element, Payload, Source, Vectors};
use common::json::Json;
use common::mmap_array::MmapArray;
use common::remap::RemappedCollection;
use storage::Storage;

use std::collections::BinaryHeap;
use std::fs::create_dir;
use std::path::Path;

const ZERO: f32 = 0.0f32;

#[allow(dead_code)]
pub struct SparseInvertedIndex<O: OperatorSparseInvertedIndex> {
    storage: O::Storage,
    payloads: MmapArray<Payload>,
    indexes: Json<Vec<u32>>,
    offsets: Json<Vec<u32>>,
    scores: Json<Vec<f32>>,
}

impl<O: OperatorSparseInvertedIndex> SparseInvertedIndex<O> {
    pub fn create(
        path: impl AsRef<Path>,
        options: IndexOptions,
        source: &(impl Vectors<O::Vector> + Collection + Source),
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
        _: &'a SearchOptions,
    ) -> Box<dyn Iterator<Item = Element> + 'a> {
        let mut doc_score = vec![ZERO; self.payloads.len()];
        for (token, val) in O::to_index_vec(vector) {
            let start = self.offsets[token as usize];
            let end = self.offsets[token as usize + 1];
            for i in (start as usize)..(end as usize) {
                doc_score[self.indexes[i] as usize] += self.scores[i] * val;
            }
        }
        let mut candidates: BinaryHeap<(Distance, AlwaysEqual<u32>)> = doc_score
            .iter()
            .enumerate()
            .map(|(i, score)| (Distance::from(-score), AlwaysEqual(i as u32)))
            .collect::<Vec<_>>()
            .into();
        Box::new(std::iter::from_fn(move || {
            candidates.pop().map(|(distance, AlwaysEqual(u))| Element {
                distance,
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

fn from_nothing<O: OperatorSparseInvertedIndex>(
    path: impl AsRef<Path>,
    opts: IndexOptions,
    collection: &(impl Vectors<O::Vector> + Collection),
) -> SparseInvertedIndex<O> {
    create_dir(path.as_ref()).expect("failed to create path for inverted index");

    let mut token_collection = vec![Vec::new(); opts.vector.dims as usize];
    for i in 0..collection.len() {
        for (token, score) in O::to_index_vec(collection.vector(i)) {
            token_collection[token as usize].push((i, score.to_f32()));
        }
    }
    let (indexes, offsets, scores) = build_compressed_matrix(token_collection);

    let storage = O::Storage::create(path.as_ref().join("storage"), collection);
    let payloads = MmapArray::create(
        path.as_ref().join("payloads"),
        (0..collection.len()).map(|i| collection.payload(i)),
    );
    let json_index = Json::create(path.as_ref().join("indexes"), indexes);
    let json_offset = Json::create(path.as_ref().join("offsets"), offsets);
    let json_score = Json::create(path.as_ref().join("scores"), scores);
    SparseInvertedIndex {
        storage,
        payloads,
        indexes: json_index,
        offsets: json_offset,
        scores: json_score,
    }
}

fn open<O: OperatorSparseInvertedIndex>(path: impl AsRef<Path>) -> SparseInvertedIndex<O> {
    let storage = O::Storage::open(path.as_ref().join("storage"));
    let payloads = MmapArray::open(path.as_ref().join("payloads"));
    let offsets = Json::open(path.as_ref().join("offsets"));
    let indexes = Json::open(path.as_ref().join("indexes"));
    let scores = Json::open(path.as_ref().join("scores"));
    SparseInvertedIndex {
        storage,
        payloads,
        indexes,
        offsets,
        scores,
    }
}

fn build_compressed_matrix(
    token_collection: Vec<Vec<(u32, f32)>>,
) -> (Vec<u32>, Vec<u32>, Vec<f32>) {
    let mut indexes = Vec::new();
    let mut offsets = Vec::new();
    let mut scores = Vec::new();

    let mut last: u32 = 0;
    offsets.push(0);
    for doc_scores in token_collection.iter() {
        if doc_scores.is_empty() {
            offsets.push(last);
            continue;
        }
        for (id, score) in doc_scores {
            indexes.push(*id);
            scores.push(*score);
        }
        last += doc_scores.len() as u32;
        offsets.push(last);
    }

    (indexes, offsets, scores)
}
