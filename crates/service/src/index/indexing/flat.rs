use super::AbstractIndexing;
use crate::algorithms::quantization::QuantizationOptions;
use crate::index::segments::growing::GrowingSegment;
use crate::index::IndexOptions;
use crate::index::SearchOptions;
use crate::prelude::*;
use crate::{algorithms::flat::Flat, index::segments::sealed::SealedSegment};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use validator::Validate;

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct FlatIndexingOptions {
    #[serde(default)]
    #[validate]
    pub quantization: QuantizationOptions,
}

impl Default for FlatIndexingOptions {
    fn default() -> Self {
        Self {
            quantization: QuantizationOptions::default(),
        }
    }
}

pub struct FlatIndexing<S: G> {
    raw: crate::algorithms::flat::Flat<S>,
}

impl<S: G> AbstractIndexing<S> for FlatIndexing<S> {
    fn create(
        path: PathBuf,
        options: IndexOptions,
        sealed: Vec<Arc<SealedSegment<S>>>,
        growing: Vec<Arc<GrowingSegment<S>>>,
    ) -> Self {
        let raw = Flat::create(path, options, sealed, growing);
        Self { raw }
    }

    fn open(path: PathBuf, options: IndexOptions) -> Self {
        let raw = Flat::open(path, options);
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
        self.raw.search(vector, opts.search_k, filter)
    }

    fn vbase<'a>(
        &'a self,
        vector: &'a [S::Scalar],
        _opts: &'a SearchOptions,
    ) -> (
        Vec<HeapElement>,
        Box<(dyn Iterator<Item = HeapElement> + 'a)>,
    ) {
        self.raw.vbase(vector)
    }
}
