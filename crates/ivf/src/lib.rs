#![allow(clippy::len_without_is_empty)]
#![allow(clippy::needless_range_loop)]

pub mod ivf_naive;
pub mod ivf_residual;
pub mod operator;

use self::ivf_naive::IvfNaive;
use crate::operator::OperatorIvf;
use base::index::*;
use base::operator::*;
use base::search::*;
use common::variants::variants;
use ivf_residual::IvfResidual;
use std::path::Path;

pub enum Ivf<O: OperatorIvf> {
    Naive(IvfNaive<O>),
    Residual(IvfResidual<O>),
}

impl<O: OperatorIvf> Ivf<O> {
    pub fn create(
        path: impl AsRef<Path>,
        options: IndexOptions,
        source: &(impl Vectors<Owned<O>> + Collection + Source + Sync),
    ) -> Self {
        let IvfIndexingOptions {
            quantization: quantization_options,
            residual_quantization,
            ..
        } = options.indexing.clone().unwrap_ivf();
        std::fs::create_dir(path.as_ref()).unwrap();
        let this = if !residual_quantization
            || matches!(quantization_options, QuantizationOptions::Trivial(_))
            || !O::RESIDUAL
        {
            Self::Naive(IvfNaive::create(
                path.as_ref().join("ivf_naive"),
                options,
                source,
            ))
        } else {
            Self::Residual(IvfResidual::create(
                path.as_ref().join("ivf_residual"),
                options,
                source,
            ))
        };
        this
    }

    pub fn open(path: impl AsRef<Path>) -> Self {
        match variants(path.as_ref(), ["ivf_naive", "ivf_residual"]) {
            "ivf_naive" => Self::Naive(IvfNaive::open(path.as_ref().join("ivf_naive"))),
            "ivf_residual" => Self::Residual(IvfResidual::open(path.as_ref().join("ivf_residual"))),
            _ => unreachable!(),
        }
    }

    pub fn dims(&self) -> u32 {
        match self {
            Ivf::Naive(x) => x.dims(),
            Ivf::Residual(x) => x.dims(),
        }
    }

    pub fn len(&self) -> u32 {
        match self {
            Ivf::Naive(x) => x.len(),
            Ivf::Residual(x) => x.len(),
        }
    }

    pub fn vector(&self, i: u32) -> Borrowed<'_, O> {
        match self {
            Ivf::Naive(x) => x.vector(i),
            Ivf::Residual(x) => x.vector(i),
        }
    }

    pub fn payload(&self, i: u32) -> Payload {
        match self {
            Ivf::Naive(x) => x.payload(i),
            Ivf::Residual(x) => x.payload(i),
        }
    }

    pub fn vbase<'a>(
        &'a self,
        vector: Borrowed<'a, O>,
        opts: &'a SearchOptions,
    ) -> Box<dyn Iterator<Item = Element> + 'a> {
        match self {
            Ivf::Naive(x) => x.vbase(vector, opts),
            Ivf::Residual(x) => x.vbase(vector, opts),
        }
    }
}
