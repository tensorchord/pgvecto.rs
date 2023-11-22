use super::AbstractIndexing;
use crate::algorithms::quantization::QuantizationOptions;
use crate::index::segments::growing::GrowingSegment;
use crate::index::IndexOptions;
use crate::prelude::*;
use crate::{algorithms::flat::Flat, index::segments::sealed::SealedSegment};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use validator::Validate;

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
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

pub struct FlatIndexing {
    raw: crate::algorithms::flat::Flat,
}

impl AbstractIndexing for FlatIndexing {
    fn create(
        path: PathBuf,
        options: IndexOptions,
        sealed: Vec<Arc<SealedSegment>>,
        growing: Vec<Arc<GrowingSegment>>,
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
