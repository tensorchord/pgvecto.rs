use super::AbstractIndexing;
use crate::algorithms::ivf::Ivf;
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
pub struct IvfIndexingOptions {
    #[serde(default = "IvfIndexingOptions::default_least_iterations")]
    #[validate(range(min = 1, max = 1_000_000))]
    pub least_iterations: u32,
    #[serde(default = "IvfIndexingOptions::default_iterations")]
    #[validate(range(min = 1, max = 1_000_000))]
    pub iterations: u32,
    #[serde(default = "IvfIndexingOptions::default_nlist")]
    #[validate(range(min = 1, max = 1_000_000))]
    pub nlist: u32,
    #[serde(default = "IvfIndexingOptions::default_nsample")]
    #[validate(range(min = 1, max = 1_000_000))]
    pub nsample: u32,
    #[serde(default)]
    #[validate]
    pub quantization: QuantizationOptions,
}

impl IvfIndexingOptions {
    fn default_least_iterations() -> u32 {
        16
    }
    fn default_iterations() -> u32 {
        500
    }
    fn default_nlist() -> u32 {
        1000
    }
    fn default_nsample() -> u32 {
        65536
    }
}

impl Default for IvfIndexingOptions {
    fn default() -> Self {
        Self {
            least_iterations: Self::default_least_iterations(),
            iterations: Self::default_iterations(),
            nlist: Self::default_nlist(),
            nsample: Self::default_nsample(),
            quantization: Default::default(),
        }
    }
}

pub struct IvfIndexing<S: G> {
    raw: Ivf<S>,
}

impl<S: G> AbstractIndexing<S> for IvfIndexing<S> {
    fn create(
        path: &Path,
        options: IndexOptions,
        sealed: Vec<Arc<SealedSegment<S>>>,
        growing: Vec<Arc<GrowingSegment<S>>>,
    ) -> Self {
        let raw = Ivf::create(path, options, sealed, growing);
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

impl<S: G> IvfIndexing<S> {
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
            raw: Ivf::open(path, options),
        }
    }
}
