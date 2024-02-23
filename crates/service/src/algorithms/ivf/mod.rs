pub mod ivf_naive;
pub mod ivf_pq;

use self::ivf_naive::IvfNaive;
use self::ivf_pq::IvfPq;
use crate::index::segments::growing::GrowingSegment;
use crate::index::segments::sealed::SealedSegment;
use crate::prelude::*;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::path::Path;
use std::sync::Arc;

pub enum Ivf<S: G> {
    Naive(IvfNaive<S>),
    Pq(IvfPq<S>),
}

impl<S: G> Ivf<S> {
    pub fn create(
        path: &Path,
        options: IndexOptions,
        sealed: Vec<Arc<SealedSegment<S>>>,
        growing: Vec<Arc<GrowingSegment<S>>>,
    ) -> Self {
        if matches!(
            options.indexing.clone().unwrap_ivf().quantization,
            QuantizationOptions::Product(_)
        ) {
            Self::Pq(IvfPq::create(path, options, sealed, growing))
        } else {
            Self::Naive(IvfNaive::create(path, options, sealed, growing))
        }
    }

    pub fn open(path: &Path, options: IndexOptions) -> Self {
        if matches!(
            options.indexing.clone().unwrap_ivf().quantization,
            QuantizationOptions::Product(_)
        ) {
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

    pub fn vector(&self, i: u32) -> Borrowed<'_, S> {
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

    pub fn basic(
        &self,
        vector: Borrowed<'_, S>,
        opts: &SearchOptions,
        filter: impl Filter,
    ) -> BinaryHeap<Reverse<Element>> {
        match self {
            Ivf::Naive(x) => x.basic(vector, opts, filter),
            Ivf::Pq(x) => x.basic(vector, opts, filter),
        }
    }

    pub fn vbase<'a>(
        &'a self,
        vector: Borrowed<'a, S>,
        opts: &'a SearchOptions,
        filter: impl Filter + 'a,
    ) -> (Vec<Element>, Box<(dyn Iterator<Item = Element> + 'a)>) {
        match self {
            Ivf::Naive(x) => x.vbase(vector, opts, filter),
            Ivf::Pq(x) => x.vbase(vector, opts, filter),
        }
    }
}
