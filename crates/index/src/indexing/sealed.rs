use crate::Op;
use base::index::*;
use base::operator::*;
use base::search::*;
use flat::Flat;
use hnsw::Hnsw;
use inverted::Inverted;
use ivf::Ivf;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::path::Path;

pub enum SealedIndexing<O: Op> {
    Flat(Flat<O>),
    Ivf(Ivf<O>),
    Hnsw(Hnsw<O>),
    Inverted(Inverted<O>),
}

impl<O: Op> SealedIndexing<O> {
    pub fn create(
        path: impl AsRef<Path>,
        options: IndexOptions,
        source: &(impl Source<O> + Sync),
    ) -> Self {
        match options.indexing {
            IndexingOptions::Flat(_) => Self::Flat(Flat::create(path, options, source)),
            IndexingOptions::Ivf(_) => Self::Ivf(Ivf::create(path, options, source)),
            IndexingOptions::Hnsw(_) => Self::Hnsw(Hnsw::create(path, options, source)),
            IndexingOptions::Inverted(_) => Self::Inverted(Inverted::create(path, options, source)),
        }
    }

    pub fn open(path: impl AsRef<Path>, options: IndexOptions) -> Self {
        match options.indexing {
            IndexingOptions::Flat(_) => Self::Flat(Flat::open(path)),
            IndexingOptions::Ivf(_) => Self::Ivf(Ivf::open(path)),
            IndexingOptions::Hnsw(_) => Self::Hnsw(Hnsw::open(path)),
            IndexingOptions::Inverted(_) => Self::Inverted(Inverted::open(path)),
        }
    }

    pub fn basic(
        &self,
        vector: Borrowed<'_, O>,
        opts: &SearchOptions,
    ) -> BinaryHeap<Reverse<Element>> {
        match self {
            SealedIndexing::Flat(x) => x.basic(vector, opts),
            SealedIndexing::Ivf(x) => x.basic(vector, opts),
            SealedIndexing::Hnsw(x) => x.basic(vector, opts),
            SealedIndexing::Inverted(x) => x.basic(vector, opts),
        }
    }

    pub fn vbase<'a>(
        &'a self,
        vector: Borrowed<'a, O>,
        opts: &'a SearchOptions,
    ) -> (Vec<Element>, Box<(dyn Iterator<Item = Element> + 'a)>) {
        match self {
            SealedIndexing::Flat(x) => x.vbase(vector, opts),
            SealedIndexing::Ivf(x) => x.vbase(vector, opts),
            SealedIndexing::Hnsw(x) => x.vbase(vector, opts),
            SealedIndexing::Inverted(x) => x.vbase(vector, opts),
        }
    }

    pub fn len(&self) -> u32 {
        match self {
            SealedIndexing::Flat(x) => x.len(),
            SealedIndexing::Ivf(x) => x.len(),
            SealedIndexing::Hnsw(x) => x.len(),
            SealedIndexing::Inverted(x) => x.len(),
        }
    }

    pub fn vector(&self, i: u32) -> Borrowed<'_, O> {
        match self {
            SealedIndexing::Flat(x) => x.vector(i),
            SealedIndexing::Ivf(x) => x.vector(i),
            SealedIndexing::Hnsw(x) => x.vector(i),
            SealedIndexing::Inverted(x) => x.vector(i),
        }
    }

    pub fn payload(&self, i: u32) -> Payload {
        match self {
            SealedIndexing::Flat(x) => x.payload(i),
            SealedIndexing::Ivf(x) => x.payload(i),
            SealedIndexing::Hnsw(x) => x.payload(i),
            SealedIndexing::Inverted(x) => x.payload(i),
        }
    }
}
