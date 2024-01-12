pub mod flat;
pub mod hnsw;
pub mod ivf;
pub mod diskann;

use self::flat::{FlatIndexing, FlatIndexingOptions};
use self::hnsw::{HnswIndexing, HnswIndexingOptions};
use self::ivf::{IvfIndexing, IvfIndexingOptions};
use self::diskann::{DiskANNIndexing, DiskANNIndexingOptions};
use super::segments::growing::GrowingSegment;
use super::segments::sealed::SealedSegment;
use super::IndexOptions;
use crate::index::SearchOptions;
use crate::prelude::*;
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::path::PathBuf;
use std::sync::Arc;
use validator::Validate;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub enum IndexingOptions {
    Flat(FlatIndexingOptions),
    Ivf(IvfIndexingOptions),
    Hnsw(HnswIndexingOptions),
    DiskANN(DiskANNIndexingOptions),
}

impl IndexingOptions {
    pub fn unwrap_flat(self) -> FlatIndexingOptions {
        let IndexingOptions::Flat(x) = self else {
            unreachable!()
        };
        x
    }
    pub fn unwrap_ivf(self) -> IvfIndexingOptions {
        let IndexingOptions::Ivf(x) = self else {
            unreachable!()
        };
        x
    }
    pub fn unwrap_hnsw(self) -> HnswIndexingOptions {
        let IndexingOptions::Hnsw(x) = self else {
            unreachable!()
        };
        x
    }
    pub fn unwrap_diskann(self) -> DiskANNIndexingOptions {
        let IndexingOptions::DiskANN(x) = self else {
            unreachable!()
        };
        x
    }
}

impl Default for IndexingOptions {
    fn default() -> Self {
        Self::Hnsw(Default::default())
    }
}

impl Validate for IndexingOptions {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            Self::Flat(x) => x.validate(),
            Self::Ivf(x) => x.validate(),
            Self::Hnsw(x) => x.validate(),
            Self::DiskANN(x) => x.validate(),
        }
    }
}

pub trait AbstractIndexing<S: G>: Sized {
    fn create(
        path: PathBuf,
        options: IndexOptions,
        sealed: Vec<Arc<SealedSegment<S>>>,
        growing: Vec<Arc<GrowingSegment<S>>>,
    ) -> Self;
    fn open(path: PathBuf, options: IndexOptions) -> Self;
    fn len(&self) -> u32;
    fn vector(&self, i: u32) -> &[S::Scalar];
    fn payload(&self, i: u32) -> Payload;
    fn basic(
        &self,
        vector: &[S::Scalar],
        opts: &SearchOptions,
        filter: impl Filter,
    ) -> BinaryHeap<Reverse<Element>>;
    fn vbase<'a>(
        &'a self,
        vector: &'a [S::Scalar],
        opts: &'a SearchOptions,
        filter: impl Filter + 'a,
    ) -> (Vec<Element>, Box<dyn Iterator<Item = Element> + 'a>);
}

pub enum DynamicIndexing<S: G> {
    Flat(FlatIndexing<S>),
    Ivf(IvfIndexing<S>),
    Hnsw(HnswIndexing<S>),
    DiskANN(DiskANNIndexing<S>),
}

impl<S: G> DynamicIndexing<S> {
    pub fn create(
        path: PathBuf,
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
            IndexingOptions::DiskANN(_) => {
                Self::DiskANN(DiskANNIndexing::create(path, options, sealed, growing))
            }
        }
    }

    pub fn open(path: PathBuf, options: IndexOptions) -> Self {
        match options.indexing {
            IndexingOptions::Flat(_) => Self::Flat(FlatIndexing::open(path, options)),
            IndexingOptions::Ivf(_) => Self::Ivf(IvfIndexing::open(path, options)),
            IndexingOptions::Hnsw(_) => Self::Hnsw(HnswIndexing::open(path, options)),
            IndexingOptions::DiskANN(_) => Self::DiskANN(DiskANNIndexing::open(path, options)),
        }
    }

    pub fn len(&self) -> u32 {
        match self {
            DynamicIndexing::Flat(x) => x.len(),
            DynamicIndexing::Ivf(x) => x.len(),
            DynamicIndexing::Hnsw(x) => x.len(),
            DynamicIndexing::DiskANN(x) => x.len(),
        }
    }

    pub fn vector(&self, i: u32) -> &[S::Scalar] {
        match self {
            DynamicIndexing::Flat(x) => x.vector(i),
            DynamicIndexing::Ivf(x) => x.vector(i),
            DynamicIndexing::Hnsw(x) => x.vector(i),
            DynamicIndexing::DiskANN(x) => x.vector(i),
        }
    }

    pub fn payload(&self, i: u32) -> Payload {
        match self {
            DynamicIndexing::Flat(x) => x.payload(i),
            DynamicIndexing::Ivf(x) => x.payload(i),
            DynamicIndexing::Hnsw(x) => x.payload(i),
            DynamicIndexing::DiskANN(x) => x.payload(i),
        }
    }

    pub fn basic(
        &self,
        vector: &[S::Scalar],
        opts: &SearchOptions,
        filter: impl Filter,
    ) -> BinaryHeap<Reverse<Element>> {
        match self {
            DynamicIndexing::Flat(x) => x.basic(vector, opts, filter),
            DynamicIndexing::Ivf(x) => x.basic(vector, opts, filter),
            DynamicIndexing::Hnsw(x) => x.basic(vector, opts, filter),
            DynamicIndexing::DiskANN(x) => x.basic(vector, opts, filter),
        }
    }

    pub fn vbase<'a>(
        &'a self,
        vector: &'a [S::Scalar],
        opts: &'a SearchOptions,
        filter: impl Filter + 'a,
    ) -> (Vec<Element>, Box<(dyn Iterator<Item = Element> + 'a)>) {
        match self {
            DynamicIndexing::Flat(x) => x.vbase(vector, opts, filter),
            DynamicIndexing::Ivf(x) => x.vbase(vector, opts, filter),
            DynamicIndexing::Hnsw(x) => x.vbase(vector, opts, filter),
        }
    }
}
