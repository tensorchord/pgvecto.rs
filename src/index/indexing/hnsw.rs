use super::AbstractIndexing;
use crate::algorithms::hnsw::HnswIndexIter;
use crate::algorithms::quantization::QuantizationOptions;
use crate::index::segments::growing::GrowingSegment;
use crate::index::IndexOptions;
use crate::prelude::*;
use crate::{algorithms::hnsw::Hnsw, index::segments::sealed::SealedSegment};
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, sync::Arc};
use validator::Validate;

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
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

pub struct HnswIndexing {
    raw: Hnsw,
}

impl AbstractIndexing for HnswIndexing {
    fn create(
        path: PathBuf,
        options: IndexOptions,
        sealed: Vec<Arc<SealedSegment>>,
        growing: Vec<Arc<GrowingSegment>>,
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

    fn vector(&self, i: u32) -> &[Scalar] {
        self.raw.vector(i)
    }

    fn payload(&self, i: u32) -> Payload {
        self.raw.payload(i)
    }

    fn search(&self, k: usize, vector: &[Scalar], filter: &mut impl Filter) -> Heap {
        self.raw.search(k, vector, filter)
    }
}

impl HnswIndexing {
    pub fn search_vbase<'index, 'vector>(
        &'index self,
        range: usize,
        vector: &'vector [Scalar],
    ) -> HnswIndexIter<'index, 'vector> {
        self.raw.search_vbase(range, vector)
    }
}
