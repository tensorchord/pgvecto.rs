#![allow(clippy::len_without_is_empty)]
#![allow(clippy::needless_range_loop)]

pub mod ivf_naive;
pub mod ivf_pq;

use self::ivf_naive::IvfNaive;
use base::index::*;
use base::operator::*;
use base::search::*;
use common::variants::variants;
use elkan_k_means::operator::OperatorElkanKMeans;
use ivf_pq::IvfPq;
use quantization::operator::OperatorQuantization;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::path::Path;
use storage::OperatorStorage;

pub trait OperatorIvf:
    Operator + OperatorElkanKMeans + OperatorQuantization + OperatorStorage
{
}

impl<T: Operator + OperatorElkanKMeans + OperatorQuantization + OperatorStorage> OperatorIvf for T {}

pub enum Ivf<O: OperatorIvf> {
    Naive(IvfNaive<O>),
    Pq(IvfPq<O>),
}

impl<O: OperatorIvf> Ivf<O> {
    pub fn create(path: impl AsRef<Path>, options: IndexOptions, source: &impl Source<O>) -> Self {
        let IvfIndexingOptions {
            quantization: quantization_options,
            ..
        } = options.indexing.clone().unwrap_ivf();
        std::fs::create_dir(path.as_ref()).unwrap();
        let this = if matches!(quantization_options, QuantizationOptions::Product(_)) {
            Self::Pq(IvfPq::create(path.as_ref().join("ivf_pq"), options, source))
        } else {
            Self::Naive(IvfNaive::create(
                path.as_ref().join("ivf_naive"),
                options,
                source,
            ))
        };
        this
    }

    pub fn open(path: impl AsRef<Path>) -> Self {
        match variants(path.as_ref(), ["ivf_naive", "ivf_pq"]) {
            "ivf_naive" => Self::Naive(IvfNaive::open(path.as_ref().join("naive"))),
            "ivf_pq" => todo!(),
            _ => unreachable!(),
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
    ) -> BinaryHeap<Reverse<Element>> {
        match self {
            Ivf::Naive(x) => x.basic(vector, opts),
            Ivf::Pq(x) => x.basic(vector, opts),
        }
    }

    pub fn vbase<'a>(
        &'a self,
        vector: Borrowed<'a, O>,
        opts: &'a SearchOptions,
    ) -> (Vec<Element>, Box<(dyn Iterator<Item = Element> + 'a)>) {
        match self {
            Ivf::Naive(x) => x.vbase(vector, opts),
            Ivf::Pq(x) => x.vbase(vector, opts),
        }
    }
}
