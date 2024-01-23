use super::AbstractIndexing;
use crate::algorithms::quantization::QuantizationOptions;
use crate::index::segments::growing::GrowingSegment;
use crate::index::IndexOptions;
use crate::index::SearchOptions;
use crate::prelude::*;
use crate::{algorithms::flat::Flat, index::segments::sealed::SealedSegment};
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::path::Path;
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
    raw: Flat<S>,
}

impl<S: G> AbstractIndexing<S> for FlatIndexing<S> {
    fn create(
        path: &Path,
        options: IndexOptions,
        sealed: Vec<Arc<SealedSegment<S>>>,
        growing: Vec<Arc<GrowingSegment<S>>>,
    ) -> Self {
        let raw = Flat::create(path, options, sealed, growing);
        Self { raw }
    }

    fn basic(
        &self,
        vector: &[S::Element],
        opts: &SearchOptions,
        filter: impl Filter,
    ) -> BinaryHeap<Reverse<Element>> {
        self.raw.basic(vector, opts, filter)
    }

    fn vbase<'a>(
        &'a self,
        vector: &'a [S::Element],
        opts: &'a SearchOptions,
        filter: impl Filter + 'a,
    ) -> (Vec<Element>, Box<(dyn Iterator<Item = Element> + 'a)>) {
        self.raw.vbase(vector, opts, filter)
    }
}

impl<S: G> Storage for FlatIndexing<S> {
    type Element = S::Element;

    fn dims(&self) -> u16 {
        self.raw.dims()
    }

    fn len(&self) -> u32 {
        self.raw.len()
    }

    fn content(&self, i: u32) -> &[Self::Element] {
        self.raw.content(i)
    }

    fn payload(&self, i: u32) -> Payload {
        self.raw.payload(i)
    }

    fn load(path: &Path, options: IndexOptions) -> Self {
        Self {
            raw: Flat::load(path, options),
        }
    }
}
