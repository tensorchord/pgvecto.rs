pub mod ivf_naive;
pub mod ivf_pq;

use self::ivf_naive::IvfNaive;
use self::ivf_pq::IvfPq;
use crate::index::segments::growing::GrowingSegment;
use crate::index::segments::sealed::SealedSegment;
use crate::index::IndexOptions;
use crate::index::SearchOptions;
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

    pub fn load(path: &Path, options: IndexOptions) -> Self {
        if options
            .indexing
            .clone()
            .unwrap_ivf()
            .quantization
            .is_product_quantization()
        {
            Self::Pq(IvfPq::load(path, options))
        } else {
            Self::Naive(IvfNaive::load(path, options))
        }
    }

    pub fn dims(&self) -> u16 {
        match self {
            Ivf::Naive(x) => x.dims(),
            Ivf::Pq(x) => x.dims(),
        }
    }

    pub fn len(&self) -> u32 {
        match self {
            Ivf::Naive(x) => x.len(),
            Ivf::Pq(x) => x.len(),
        }
    }

    pub fn content(&self, i: u32) -> <S::Storage as Storage>::VectorRef<'_> {
        match self {
            Ivf::Naive(x) => x.content(i),
            Ivf::Pq(x) => x.content(i),
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
        vector: &[S::Element],
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
        vector: &'a [S::Element],
        opts: &'a SearchOptions,
        filter: impl Filter + 'a,
    ) -> (Vec<Element>, Box<(dyn Iterator<Item = Element> + 'a)>) {
        match self {
            Ivf::Naive(x) => x.vbase(vector, opts, filter),
            Ivf::Pq(x) => x.vbase(vector, opts, filter),
        }
    }
}
