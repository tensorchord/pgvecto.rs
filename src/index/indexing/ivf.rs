use super::AbstractIndexing;
use crate::algorithms::ivf::Ivf;
use crate::algorithms::quantization::QuantizationOptions;
use crate::index::segments::growing::GrowingSegment;
use crate::index::segments::sealed::SealedSegment;
use crate::index::IndexOptions;
use crate::prelude::Scalar;
use crate::prelude::*;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use validator::Validate;

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
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
    #[serde(default = "IvfIndexingOptions::default_nprobe")]
    #[validate(range(min = 1, max = 1_000_000))]
    pub nprobe: u32,
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
    fn default_nprobe() -> u32 {
        10
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
            nprobe: Self::default_nprobe(),
            nsample: Self::default_nsample(),
            quantization: Default::default(),
        }
    }
}

pub struct IvfIndexing {
    raw: Ivf,
}

impl AbstractIndexing for IvfIndexing {
    fn create(
        path: PathBuf,
        options: IndexOptions,
        sealed: Vec<Arc<SealedSegment>>,
        growing: Vec<Arc<GrowingSegment>>,
    ) -> Self {
        let raw = Ivf::create(path, options, sealed, growing);
        Self { raw }
    }

    fn open(path: PathBuf, options: IndexOptions) -> Self {
        let raw = Ivf::open(path, options);
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

    fn search<F: FnMut(Payload) -> bool>(&self, k: usize, vector: &[Scalar], f: F) -> Heap {
        self.raw.search(k, vector, f)
    }
}
