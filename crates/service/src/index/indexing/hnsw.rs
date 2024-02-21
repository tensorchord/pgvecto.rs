use super::AbstractIndexing;
use crate::algorithms::hnsw::Hnsw;
use crate::algorithms::quantization::QuantizationOptions;
use crate::index::segments::growing::GrowingSegment;
use crate::index::segments::sealed::SealedSegment;
use crate::index::IndexOptions;
use crate::index::SearchOptions;
use crate::prelude::*;
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::path::Path;
use std::sync::Arc;
use validator::Validate;

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct HnswIndexingOptions {
    #[serde(default = "HnswIndexingOptions::default_m")]
    #[validate(range(min = 4, max = 128))]
    pub m: u32,
    #[serde(default = "HnswIndexingOptions::default_ef_construction")]
    #[validate(range(min = 10, max = 2000))]
    pub ef_construction: usize,
    #[serde(default)]
    #[validate]
    pub quantization: QuantizationOptions,
}

impl HnswIndexingOptions {
    fn default_m() -> u32 {
        12
    }
    fn default_ef_construction() -> usize {
        300
    }
}

impl Default for HnswIndexingOptions {
    fn default() -> Self {
        Self {
            m: Self::default_m(),
            ef_construction: Self::default_ef_construction(),
            quantization: Default::default(),
        }
    }
}

pub struct HnswIndexing<S: G> {
    raw: Hnsw<S>,
}

impl<S: G> AbstractIndexing<S> for HnswIndexing<S> {
    fn create(
        path: &Path,
        options: IndexOptions,
        sealed: Vec<Arc<SealedSegment<S>>>,
        growing: Vec<Arc<GrowingSegment<S>>>,
    ) -> Self {
        let raw = Hnsw::create(path, options, sealed, growing);
        Self { raw }
    }

    fn basic(
        &self,
        vector: S::VectorRef<'_>,
        opts: &SearchOptions,
        filter: impl Filter,
    ) -> BinaryHeap<Reverse<Element>> {
        self.raw.basic(vector, opts, filter)
    }

    fn vbase<'a>(
        &'a self,
        vector: S::VectorRef<'a>,
        opts: &'a SearchOptions,
        filter: impl Filter + 'a,
    ) -> (Vec<Element>, Box<(dyn Iterator<Item = Element> + 'a)>) {
        self.raw.vbase(vector, opts, filter)
    }
}

impl<S: G> HnswIndexing<S> {
    pub fn len(&self) -> u32 {
        self.raw.len()
    }

    pub fn vector(&self, i: u32) -> S::VectorRef<'_> {
        self.raw.vector(i)
    }

    pub fn payload(&self, i: u32) -> Payload {
        self.raw.payload(i)
    }

    pub fn open(path: &Path, options: IndexOptions) -> Self {
        Self {
            raw: Hnsw::open(path, options),
        }
    }
}
