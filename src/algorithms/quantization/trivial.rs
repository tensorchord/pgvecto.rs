use crate::algorithms::quantization::Quan;
use crate::algorithms::quantization::QuantizationOptions;
use crate::algorithms::raw::Raw;
use crate::index::IndexOptions;
use crate::prelude::*;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use validator::Validate;

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct TrivialQuantizationOptions {}

impl Default for TrivialQuantizationOptions {
    fn default() -> Self {
        Self {}
    }
}

pub struct TrivialQuantization {
    raw: Arc<Raw>,
}

impl Quan for TrivialQuantization {
    fn create(_: PathBuf, _: IndexOptions, _: QuantizationOptions, raw: &Arc<Raw>) -> Self {
        Self { raw: raw.clone() }
    }

    fn open(_: PathBuf, _: IndexOptions, _: QuantizationOptions, raw: &Arc<Raw>) -> Self {
        Self { raw: raw.clone() }
    }

    fn distance(&self, d: Distance, lhs: &[Scalar], rhs: u32) -> Scalar {
        d.distance(lhs, self.raw.vector(rhs))
    }

    fn distance2(&self, d: Distance, lhs: u32, rhs: u32) -> Scalar {
        d.distance(self.raw.vector(lhs), self.raw.vector(rhs))
    }
}
