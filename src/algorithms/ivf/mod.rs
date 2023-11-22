pub mod ivf_naive;
pub mod ivf_pq;

use self::ivf_naive::IvfNaive;
use self::ivf_pq::IvfPq;
use crate::index::segments::growing::GrowingSegment;
use crate::index::segments::sealed::SealedSegment;
use crate::index::IndexOptions;
use crate::prelude::*;
use std::path::PathBuf;
use std::sync::Arc;

pub enum Ivf {
    Naive(IvfNaive),
    Pq(IvfPq),
}

impl Ivf {
    pub fn create(
        path: PathBuf,
        options: IndexOptions,
        sealed: Vec<Arc<SealedSegment>>,
        growing: Vec<Arc<GrowingSegment>>,
    ) -> Self {
        if options
            .indexing
            .clone()
            .unwrap_ivf()
            .quantization
            .is_product_quantization()
        {
            Self::Pq(IvfPq::create(path, options, sealed, growing))
        } else {
            Self::Naive(IvfNaive::create(path, options, sealed, growing))
        }
    }

    pub fn open(path: PathBuf, options: IndexOptions) -> Self {
        if options
            .indexing
            .clone()
            .unwrap_ivf()
            .quantization
            .is_product_quantization()
        {
            Self::Pq(IvfPq::open(path, options))
        } else {
            Self::Naive(IvfNaive::open(path, options))
        }
    }

    pub fn len(&self) -> u32 {
        match self {
            Ivf::Naive(x) => x.len(),
            Ivf::Pq(x) => x.len(),
        }
    }

    pub fn vector(&self, i: u32) -> &[Scalar] {
        match self {
            Ivf::Naive(x) => x.vector(i),
            Ivf::Pq(x) => x.vector(i),
        }
    }

    pub fn payload(&self, i: u32) -> Payload {
        match self {
            Ivf::Naive(x) => x.payload(i),
            Ivf::Pq(x) => x.payload(i),
        }
    }

    pub fn search<F: FnMut(Payload) -> bool>(&self, k: usize, vector: &[Scalar], f: F) -> Heap {
        match self {
            Ivf::Naive(x) => x.search(k, vector, f),
            Ivf::Pq(x) => x.search(k, vector, f),
        }
    }
}
