use crate::Op;
pub use base::distance::*;
pub use base::index::*;
use base::operator::*;
pub use base::search::*;
pub use base::vector::*;
use flat::Flat;
use hnsw::Hnsw;
use ivf::Ivf;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::path::Path;

pub enum Indexing<O: Op> {
    Flat(Flat<O>),
    Ivf(Ivf<O>),
    Hnsw(Hnsw<O>),
}

impl<O: Op> Indexing<O> {
    pub fn create<S: Source<O>>(path: &Path, options: IndexOptions, source: &S) -> Self {
        match options.indexing {
            IndexingOptions::Flat(_) => Self::Flat(Flat::create(path, options, source)),
            IndexingOptions::Ivf(_) => Self::Ivf(Ivf::create(path, options, source)),
            IndexingOptions::Hnsw(_) => Self::Hnsw(Hnsw::create(path, options, source)),
        }
    }

    pub fn open(path: &Path, options: IndexOptions) -> Self {
        match options.indexing {
            IndexingOptions::Flat(_) => Self::Flat(Flat::open(path, options)),
            IndexingOptions::Ivf(_) => Self::Ivf(Ivf::open(path, options)),
            IndexingOptions::Hnsw(_) => Self::Hnsw(Hnsw::open(path, options)),
        }
    }

    pub fn basic(
        &self,
        vector: Borrowed<'_, O>,
        opts: &SearchOptions,
        filter: impl Filter,
    ) -> BinaryHeap<Reverse<Element>> {
        match self {
            Indexing::Flat(x) => x.basic(vector, opts, filter),
            Indexing::Ivf(x) => x.basic(vector, opts, filter),
            Indexing::Hnsw(x) => x.basic(vector, opts, filter),
        }
    }

    pub fn vbase<'a>(
        &'a self,
        vector: Borrowed<'a, O>,
        opts: &'a SearchOptions,
        filter: impl Filter + 'a,
    ) -> (Vec<Element>, Box<(dyn Iterator<Item = Element> + 'a)>) {
        match self {
            Indexing::Flat(x) => x.vbase(vector, opts, filter),
            Indexing::Ivf(x) => x.vbase(vector, opts, filter),
            Indexing::Hnsw(x) => x.vbase(vector, opts, filter),
        }
    }

    pub fn len(&self) -> u32 {
        match self {
            Indexing::Flat(x) => x.len(),
            Indexing::Ivf(x) => x.len(),
            Indexing::Hnsw(x) => x.len(),
        }
    }

    pub fn vector(&self, i: u32) -> Borrowed<'_, O> {
        match self {
            Indexing::Flat(x) => x.vector(i),
            Indexing::Ivf(x) => x.vector(i),
            Indexing::Hnsw(x) => x.vector(i),
        }
    }

    pub fn payload(&self, i: u32) -> Payload {
        match self {
            Indexing::Flat(x) => x.payload(i),
            Indexing::Ivf(x) => x.payload(i),
            Indexing::Hnsw(x) => x.payload(i),
        }
    }
}
