#![feature(trait_alias)]
#![allow(clippy::len_without_is_empty)]
#![allow(clippy::needless_range_loop)]

pub mod ivf_naive;
pub mod ivf_pq;

use self::ivf_naive::IvfNaive;
use self::ivf_pq::IvfPq;
use base::index::*;
use base::operator::*;
use base::search::*;
use elkan_k_means::operator::OperatorElkanKMeans;
use quantization::operator::OperatorQuantization;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::path::Path;
use storage::operator::OperatorStorage;

pub trait OperatorIvf = Operator + OperatorElkanKMeans + OperatorQuantization + OperatorStorage;

pub enum Ivf<O: OperatorIvf> {
    Naive(IvfNaive<O>),
    Pq(IvfPq<O>),
}

impl<O: OperatorIvf> Ivf<O> {
    pub fn create<S: Source<O>>(path: &Path, options: IndexOptions, source: &S) -> Self {
        if matches!(
            options.indexing.clone().unwrap_ivf().quantization,
            QuantizationOptions::Product(_)
        ) {
            Self::Pq(IvfPq::create(path, options, source))
        } else {
            Self::Naive(IvfNaive::create(path, options, source))
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

    pub fn vector(&self, i: u32) -> Borrowed<'_, O> {
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
        vector: Borrowed<'_, O>,
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
        vector: Borrowed<'a, O>,
        opts: &'a SearchOptions,
        filter: impl Filter + 'a,
    ) -> (Vec<Element>, Box<(dyn Iterator<Item = Element> + 'a)>) {
        match self {
            Ivf::Naive(x) => x.vbase(vector, opts, filter),
            Ivf::Pq(x) => x.vbase(vector, opts, filter),
        }
    }
}
