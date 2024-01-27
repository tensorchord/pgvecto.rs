use crate::algorithms::quantization::Quan;
use crate::algorithms::quantization::QuantizationOptions;
use crate::algorithms::raw::Raw;
use crate::index::IndexOptions;
use crate::prelude::*;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use validator::Validate;

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct TrivialQuantizationOptions {}

impl Default for TrivialQuantizationOptions {
    fn default() -> Self {
        Self {}
    }
}

pub struct TrivialQuantization<S: G> {
    raw: Arc<Raw<S::Storage>>,
}

impl<S: G> Quan<S> for TrivialQuantization<S> {
    fn create(
        _: &Path,
        _: IndexOptions,
        _: QuantizationOptions,
        raw: &Arc<Raw<S::Storage>>,
    ) -> Self {
        Self { raw: raw.clone() }
    }

    fn open(_: &Path, _: IndexOptions, _: QuantizationOptions, raw: &Arc<Raw<S::Storage>>) -> Self {
        Self { raw: raw.clone() }
    }

    fn distance(&self, lhs: &[S::Element], rhs: u32) -> F32 {
        S::distance(lhs, self.raw.content(rhs).vector())
    }

    fn distance2(&self, lhs: u32, rhs: u32) -> F32 {
        S::distance(
            self.raw.content(lhs).vector(),
            self.raw.content(rhs).vector(),
        )
    }
}
