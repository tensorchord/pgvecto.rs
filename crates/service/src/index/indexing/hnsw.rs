use super::AbstractIndexing;
use crate::algorithms::hnsw::Hnsw;
use crate::algorithms::quantization::QuantizationOptions;
use crate::index::segments::growing::GrowingSegment;
use crate::index::segments::sealed::SealedSegment;
use crate::index::IndexOptions;
use crate::index::SearchOptions;
use crate::prelude::*;
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, sync::Arc};
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
        path: PathBuf,
        options: IndexOptions,
        sealed: Vec<Arc<SealedSegment<S>>>,
        growing: Vec<Arc<GrowingSegment<S>>>,
    ) -> Self {
        let raw = Hnsw::create(path, options, sealed, growing);
        Self { raw }
    }

    fn open(path: PathBuf, options: IndexOptions) -> Self {
        let raw = Hnsw::open(path, options);
        Self { raw }
    }

    fn len(&self) -> u32 {
        self.raw.len()
    }

    fn vector(&self, i: u32) -> &[S::Scalar] {
        self.raw.vector(i)
    }

    fn payload(&self, i: u32) -> Payload {
        self.raw.payload(i)
    }

    fn search(&self, vector: &[S::Scalar], opts: &SearchOptions, filter: &mut impl Filter) -> Heap {
        self.raw.search(vector, opts, filter)
    }

    fn vbase<'a>(
        &'a self,
        vector: &'a [S::Scalar],
        opts: &'a SearchOptions,
    ) -> (
        Vec<HeapElement>,
        Box<(dyn Iterator<Item = HeapElement> + 'a)>,
    ) {
        self.raw.vbase(vector, opts)
    }
}
