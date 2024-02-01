use crate::algorithms::quantization::Quan;
use crate::algorithms::quantization::QuantizationOptions;
use crate::algorithms::raw::Raw;
use crate::index::IndexOptions;
use crate::prelude::*;
use crate::utils::dir_ops::sync_dir;
use crate::utils::mmap_array::MmapArray;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use validator::Validate;

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct ScalarQuantizationOptions {}

impl Default for ScalarQuantizationOptions {
    fn default() -> Self {
        Self {}
    }
}

pub struct ScalarQuantization<S: G> {
    dims: u16,
    max: Vec<S::Scalar>,
    min: Vec<S::Scalar>,
    codes: MmapArray<u8>,
}

unsafe impl<S: G> Send for ScalarQuantization<S> {}
unsafe impl<S: G> Sync for ScalarQuantization<S> {}

impl<S: G> ScalarQuantization<S> {
    pub fn codes(&self, i: u32) -> &[u8] {
        let s = i as usize * self.dims as usize;
        let e = (i + 1) as usize * self.dims as usize;
        &self.codes[s..e]
    }

    pub fn set_codes(&mut self, codes: MmapArray<u8>) {
        self.codes = codes;
    }
}

impl<S: G> Quan<S> for ScalarQuantization<S> {
    fn create(
        path: PathBuf,
        options: IndexOptions,
        _: QuantizationOptions,
        raw: &Arc<Raw<S>>,
    ) -> Self {
        std::fs::create_dir(&path).unwrap();
        let dims = options.vector.dims;
        let mut max = vec![S::Scalar::neg_infinity(); dims as usize];
        let mut min = vec![S::Scalar::infinity(); dims as usize];
        let n = raw.len();
        for i in 0..n {
            let vector = raw.vector(i);
            for j in 0..dims as usize {
                max[j] = std::cmp::max(max[j], vector[j]);
                min[j] = std::cmp::min(min[j], vector[j]);
            }
        }
        std::fs::write(path.join("max"), serde_json::to_string(&max).unwrap()).unwrap();
        std::fs::write(path.join("min"), serde_json::to_string(&min).unwrap()).unwrap();
        let codes_iter = (0..n).flat_map(|i| {
            let vector = raw.vector(i);
            let mut result = vec![0u8; dims as usize];
            for i in 0..dims as usize {
                let w = (((vector[i] - min[i]) / (max[i] - min[i])).to_f32() * 256.0) as u32;
                result[i] = w.clamp(0, 255) as u8;
            }
            result.into_iter()
        });
        let codes = MmapArray::create(path.join("codes"), codes_iter);
        sync_dir(&path);
        Self {
            dims,
            max,
            min,
            codes,
        }
    }

    fn open(path: PathBuf, options: IndexOptions, _: QuantizationOptions, _: &Arc<Raw<S>>) -> Self {
        let dims = options.vector.dims;
        let max = serde_json::from_slice(&std::fs::read("max").unwrap()).unwrap();
        let min = serde_json::from_slice(&std::fs::read("min").unwrap()).unwrap();
        let codes = MmapArray::open(path.join("codes"));
        Self {
            dims,
            max,
            min,
            codes,
        }
    }

    fn distance(&self, lhs: &[S::Scalar], rhs: u32) -> F32 {
        let dims = self.dims;
        let rhs = self.codes(rhs);
        S::scalar_quantization_distance(dims, &self.max, &self.min, lhs, rhs)
    }

    fn distance2(&self, lhs: u32, rhs: u32) -> F32 {
        let dims = self.dims;
        let lhs = self.codes(lhs);
        let rhs = self.codes(rhs);
        S::scalar_quantization_distance2(dims, &self.max, &self.min, lhs, rhs)
    }
}
