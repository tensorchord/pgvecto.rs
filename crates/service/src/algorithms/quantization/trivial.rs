use crate::algorithms::quantization::Quan;
use crate::algorithms::raw::Raw;
use crate::prelude::*;
use crate::utils::dir_ops::sync_dir;
use std::path::Path;
use std::sync::Arc;

pub struct TrivialQuantization<S: G> {
    raw: Arc<Raw<S>>,
    permutation: Vec<u32>,
}

impl<S: G> TrivialQuantization<S> {
    pub fn codes(&self, i: u32) -> Borrowed<'_, S> {
        self.raw.vector(self.permutation[i as usize])
    }
}

impl<S: G> Quan<S> for TrivialQuantization<S> {
    // permutation is the mapping from placements to original ids
    fn create(
        path: &Path,
        _: IndexOptions,
        _: QuantizationOptions,
        raw: &Arc<Raw<S>>,
        permutation: Vec<u32>,
    ) -> Self {
        // here we cannot modify raw, so we record permutation for translation
        std::fs::create_dir(path).unwrap();
        sync_dir(path);
        std::fs::write(
            path.join("permutation"),
            serde_json::to_string(&permutation).unwrap(),
        )
        .unwrap();
        Self {
            raw: raw.clone(),
            permutation,
        }
    }

    fn open2(path: &Path, _: IndexOptions, _: QuantizationOptions, raw: &Arc<Raw<S>>) -> Self {
        let permutation =
            serde_json::from_slice(&std::fs::read(path.join("permutation")).unwrap()).unwrap();
        Self {
            raw: raw.clone(),
            permutation,
        }
    }

    fn distance(&self, lhs: Borrowed<'_, S>, rhs: u32) -> F32 {
        S::distance(lhs, self.codes(rhs))
    }

    fn distance2(&self, lhs: u32, rhs: u32) -> F32 {
        S::distance(self.codes(lhs), self.codes(rhs))
    }
}
