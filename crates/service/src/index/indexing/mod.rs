pub mod flat;
pub mod hnsw;
pub mod ivf;

use self::flat::{FlatIndexing, FlatIndexingOptions};
use self::hnsw::{HnswIndexing, HnswIndexingOptions};
use self::ivf::{IvfIndexing, IvfIndexingOptions};
use super::segments::growing::GrowingSegment;
use super::segments::sealed::SealedSegment;
use super::IndexOptions;
use crate::algorithms::quantization::QuantizationOptions;
use crate::index::SearchOptions;
use crate::prelude::*;
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::path::Path;
use std::sync::Arc;
use validator::Validate;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    pub fn has_quantization(&self) -> bool {
        let option = match self {
            Self::Flat(x) => &x.quantization,
            Self::Ivf(x) => &x.quantization,
            Self::Hnsw(x) => &x.quantization,
        };
        !matches!(option, QuantizationOptions::Trivial(_))
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

pub trait AbstractIndexing<S: G> {
    fn create(
        path: &Path,
        options: IndexOptions,
        sealed: Vec<Arc<SealedSegment<S>>>,
        growing: Vec<Arc<GrowingSegment<S>>>,
    ) -> Self;
    fn basic(
        &self,
        vector: S::VectorRef<'_>,
        opts: &SearchOptions,
        filter: impl Filter,
    ) -> BinaryHeap<Reverse<Element>>;
    fn vbase<'a>(
        &'a self,
        vector: S::VectorRef<'a>,
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
        vector: S::VectorRef<'_>,
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
        vector: S::VectorRef<'a>,
        opts: &'a SearchOptions,
        filter: impl Filter + 'a,
    ) -> (Vec<Element>, Box<(dyn Iterator<Item = Element> + 'a)>) {
        match self {
            DynamicIndexing::Flat(x) => x.vbase(vector, opts, filter),
            DynamicIndexing::Ivf(x) => x.vbase(vector, opts, filter),
            DynamicIndexing::Hnsw(x) => x.vbase(vector, opts, filter),
        }
    }

    pub fn dims(&self) -> u16 {
        match self {
            DynamicIndexing::Flat(x) => x.dims(),
            DynamicIndexing::Ivf(x) => x.dims(),
            DynamicIndexing::Hnsw(x) => x.dims(),
        }
    }

    pub fn len(&self) -> u32 {
        match self {
            DynamicIndexing::Flat(x) => x.len(),
            DynamicIndexing::Ivf(x) => x.len(),
            DynamicIndexing::Hnsw(x) => x.len(),
        }
    }

    pub fn content(&self, i: u32) -> S::VectorRef<'_> {
        match self {
            DynamicIndexing::Flat(x) => x.content(i),
            DynamicIndexing::Ivf(x) => x.content(i),
            DynamicIndexing::Hnsw(x) => x.content(i),
        }
    }

    pub fn payload(&self, i: u32) -> Payload {
        match self {
            DynamicIndexing::Flat(x) => x.payload(i),
            DynamicIndexing::Ivf(x) => x.payload(i),
            DynamicIndexing::Hnsw(x) => x.payload(i),
        }
    }

    pub fn load(path: &Path, options: IndexOptions) -> Self {
        match options.indexing {
            IndexingOptions::Flat(_) => Self::Flat(FlatIndexing::load(path, options)),
            IndexingOptions::Ivf(_) => Self::Ivf(IvfIndexing::load(path, options)),
            IndexingOptions::Hnsw(_) => Self::Hnsw(HnswIndexing::load(path, options)),
        }
    }
}
