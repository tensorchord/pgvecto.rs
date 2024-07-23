#![allow(clippy::len_without_is_empty)]

pub mod operator;

use self::operator::OperatorInverted;
use base::index::{IndexOptions, SearchOptions};
use base::operator::Borrowed;
use base::scalar::{ScalarLike, F32};
use base::search::{Collection, Element, Payload, Source, Vectors};
use common::dir_ops::sync_dir;
use common::json::Json;
use common::mmap_array::MmapArray;
use common::remap::RemappedCollection;
use storage::Storage;

use std::collections::BTreeMap;
use std::fs::create_dir;
use std::path::Path;

const ZERO: F32 = F32(0.0);
const NEGATIVE_ONE: F32 = F32(-1.0);

#[allow(dead_code)]
pub struct Inverted<O: OperatorInverted> {
    storage: O::Storage,
    payloads: MmapArray<Payload>,
    indexes: Json<Vec<u32>>,
    offsets: Json<Vec<u32>>,
    scores: Json<Vec<F32>>,
}

impl<O: OperatorInverted> Inverted<O> {
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
        _: &'a SearchOptions,
    ) -> (Vec<Element>, Box<(dyn Iterator<Item = Element> + 'a)>) {
        let mut doc_score = vec![ZERO; self.payloads.len()];
        for (token, _) in O::to_index_vec(vector) {
            let start = self.offsets[token as usize];
            let end = self.offsets[token as usize + 1];
            for i in (start as usize)..(end as usize) {
                doc_score[self.indexes[i] as usize] += self.scores[i];
            }
        }
        let mut candidates: Vec<Element> = doc_score
            .iter()
            .enumerate()
            .filter(|&(_, score)| *score > ZERO)
            .map(|(i, score)| Element {
                distance: *score * NEGATIVE_ONE, // use negative score to match the negative dot product distance
                payload: self.payloads[i],
            })
            .collect();
        candidates.sort_by(|a, b| a.distance.cmp(&b.distance));

        (Vec::new(), Box::new(candidates.into_iter()))
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
    _: IndexOptions,
    collection: &impl Collection<O>,
) -> Inverted<O> {
    create_dir(path.as_ref()).expect("failed to create path for inverted index");

    let mut token_collection = BTreeMap::new();
    for i in 0..collection.len() {
        for (token, score) in O::to_index_vec(collection.vector(i)) {
            token_collection
                .entry(token)
                .or_insert_with(Vec::new)
                .push((i, score.to_f()));
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
    sync_dir(path);
    Inverted {
        storage,
        payloads,
        indexes: json_index,
        offsets: json_offset,
        scores: json_score,
    }
}

fn open<O: OperatorInverted>(path: impl AsRef<Path>) -> Inverted<O> {
    let storage = O::Storage::open(path.as_ref().join("storage"));
    let payloads = MmapArray::open(path.as_ref().join("payloads"));
    let offsets = Json::open(path.as_ref().join("offsets"));
    let indexes = Json::open(path.as_ref().join("indexes"));
    let scores = Json::open(path.as_ref().join("scores"));
    Inverted {
        storage,
        payloads,
        indexes,
        offsets,
        scores,
    }
}

fn build_compressed_matrix(
    token_collection: BTreeMap<u32, Vec<(u32, F32)>>,
) -> (Vec<u32>, Vec<u32>, Vec<F32>) {
    let mut indexes = Vec::new();
    let mut offsets = Vec::new();
    let mut scores = Vec::new();

    let mut i = 0;
    let mut last: u32 = 0;
    offsets.push(0);
    for (token, id_scores) in token_collection.iter() {
        while *token != i {
            offsets.push(last);
            i += 1;
        }
        for (id, score) in id_scores {
            indexes.push(*id);
            scores.push(*score);
        }
        last += id_scores.len() as u32;
        offsets.push(last);
        i += 1;
    }

    (indexes, offsets, scores)
}
