pub mod flat;
pub mod hnsw;
pub mod ivf;

use self::flat::{FlatIndexing, FlatIndexingOptions};
use self::hnsw::{HnswIndexing, HnswIndexingOptions};
use self::ivf::{IvfIndexing, IvfIndexingOptions};
use super::segments::growing::GrowingSegment;
use super::segments::sealed::SealedSegment;
use super::IndexOptions;
use crate::prelude::*;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use validator::Validate;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexingOptions {
    Flat(FlatIndexingOptions),
    Ivf(IvfIndexingOptions),
    Hnsw(HnswIndexingOptions),
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
        }
    }
}

pub trait AbstractIndexing: Sized {
    fn create(
        path: PathBuf,
        options: IndexOptions,
        sealed: Vec<Arc<SealedSegment>>,
        growing: Vec<Arc<GrowingSegment>>,
    ) -> Self;
    fn open(path: PathBuf, options: IndexOptions) -> Self;
    fn len(&self) -> u32;
    fn vector(&self, i: u32) -> &[Scalar];
    fn payload(&self, i: u32) -> Payload;
    fn search<F: FnMut(Payload) -> bool>(&self, k: usize, vector: &[Scalar], f: F) -> Heap;
}

pub enum DynamicIndexing {
    Flat(FlatIndexing),
    Ivf(IvfIndexing),
    Hnsw(HnswIndexing),
}

impl DynamicIndexing {
    pub fn create(
        path: PathBuf,
        options: IndexOptions,
        sealed: Vec<Arc<SealedSegment>>,
        growing: Vec<Arc<GrowingSegment>>,
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

    pub fn open(path: PathBuf, options: IndexOptions) -> Self {
        match options.indexing {
            IndexingOptions::Flat(_) => Self::Flat(FlatIndexing::open(path, options)),
            IndexingOptions::Ivf(_) => Self::Ivf(IvfIndexing::open(path, options)),
            IndexingOptions::Hnsw(_) => Self::Hnsw(HnswIndexing::open(path, options)),
        }
    }

    pub fn len(&self) -> u32 {
        match self {
            DynamicIndexing::Flat(x) => x.len(),
            DynamicIndexing::Ivf(x) => x.len(),
            DynamicIndexing::Hnsw(x) => x.len(),
        }
    }

    pub fn vector(&self, i: u32) -> &[Scalar] {
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

    pub fn search<F: FnMut(Payload) -> bool>(&self, k: usize, vector: &[Scalar], f: F) -> Heap {
        match self {
            DynamicIndexing::Flat(x) => x.search(k, vector, f),
            DynamicIndexing::Ivf(x) => x.search(k, vector, f),
            DynamicIndexing::Hnsw(x) => x.search(k, vector, f),
        }
    }
}
