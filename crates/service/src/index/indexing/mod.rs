pub mod flat;
pub mod hnsw;
pub mod ivf;

use self::flat::FlatIndexing;
use self::hnsw::HnswIndexing;
use self::ivf::IvfIndexing;
use super::segments::growing::GrowingSegment;
use super::segments::sealed::SealedSegment;
use crate::prelude::*;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::path::Path;
use std::sync::Arc;

pub trait AbstractIndexing<S: G> {
    fn create(
        path: &Path,
        options: IndexOptions,
        sealed: Vec<Arc<SealedSegment<S>>>,
        growing: Vec<Arc<GrowingSegment<S>>>,
    ) -> Self;
    fn basic(
        &self,
        vector: Borrowed<'_, S>,
        opts: &SearchOptions,
        filter: impl Filter,
    ) -> BinaryHeap<Reverse<Element>>;
    fn vbase<'a>(
        &'a self,
        vector: Borrowed<'a, S>,
        opts: &'a SearchOptions,
        filter: impl Filter + 'a,
    ) -> (Vec<Element>, Box<dyn Iterator<Item = Element> + 'a>);
}

pub enum DynamicIndexing<S: G> {
    Flat(FlatIndexing<S>),
    Ivf(IvfIndexing<S>),
    Hnsw(HnswIndexing<S>),
}

impl<S: G> DynamicIndexing<S> {
    pub fn create(
        path: &Path,
        options: IndexOptions,
        sealed: Vec<Arc<SealedSegment<S>>>,
        growing: Vec<Arc<GrowingSegment<S>>>,
    ) -> Self {
        match options.indexing {
            IndexingOptions::Flat(_) => {
                Self::Flat(FlatIndexing::create(path, options, sealed, growing))
            }
            IndexingOptions::Ivf(_) => {
                Self::Ivf(IvfIndexing::create(path, options, sealed, growing))
            }
            IndexingOptions::Hnsw(_) => {
                Self::Hnsw(HnswIndexing::create(path, options, sealed, growing))
            }
        }
    }

    pub fn basic(
        &self,
        vector: Borrowed<'_, S>,
        opts: &SearchOptions,
        filter: impl Filter,
    ) -> BinaryHeap<Reverse<Element>> {
        match self {
            DynamicIndexing::Flat(x) => x.basic(vector, opts, filter),
            DynamicIndexing::Ivf(x) => x.basic(vector, opts, filter),
            DynamicIndexing::Hnsw(x) => x.basic(vector, opts, filter),
        }
    }

    pub fn vbase<'a>(
        &'a self,
        vector: Borrowed<'a, S>,
        opts: &'a SearchOptions,
        filter: impl Filter + 'a,
    ) -> (Vec<Element>, Box<(dyn Iterator<Item = Element> + 'a)>) {
        match self {
            DynamicIndexing::Flat(x) => x.vbase(vector, opts, filter),
            DynamicIndexing::Ivf(x) => x.vbase(vector, opts, filter),
            DynamicIndexing::Hnsw(x) => x.vbase(vector, opts, filter),
        }
    }

    pub fn len(&self) -> u32 {
        match self {
            DynamicIndexing::Flat(x) => x.len(),
            DynamicIndexing::Ivf(x) => x.len(),
            DynamicIndexing::Hnsw(x) => x.len(),
        }
    }

    pub fn vector(&self, i: u32) -> Borrowed<'_, S> {
        match self {
            DynamicIndexing::Flat(x) => x.vector(i),
            DynamicIndexing::Ivf(x) => x.vector(i),
            DynamicIndexing::Hnsw(x) => x.vector(i),
        }
    }

    pub fn payload(&self, i: u32) -> Payload {
        match self {
            DynamicIndexing::Flat(x) => x.payload(i),
            DynamicIndexing::Ivf(x) => x.payload(i),
            DynamicIndexing::Hnsw(x) => x.payload(i),
        }
    }

    pub fn open(path: &Path, options: IndexOptions) -> Self {
        match options.indexing {
            IndexingOptions::Flat(_) => Self::Flat(FlatIndexing::open(path, options)),
            IndexingOptions::Ivf(_) => Self::Ivf(IvfIndexing::open(path, options)),
            IndexingOptions::Hnsw(_) => Self::Hnsw(HnswIndexing::open(path, options)),
        }
    }
}
