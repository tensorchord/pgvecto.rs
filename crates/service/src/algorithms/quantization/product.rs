use crate::algorithms::clustering::elkan_k_means::ElkanKMeans;
use crate::algorithms::quantization::Quan;
use crate::algorithms::quantization::QuantizationOptions;
use crate::algorithms::raw::Raw;
use crate::index::IndexOptions;
use crate::prelude::*;
use crate::utils::dir_ops::sync_dir;
use crate::utils::mmap_array::MmapArray;
use crate::utils::vec2::Vec2;
use rand::seq::index::sample;
use rand::thread_rng;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;
use validator::Validate;

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct ProductQuantizationOptions {
    #[serde(default = "ProductQuantizationOptions::default_sample")]
    pub sample: u32,
    #[serde(default)]
    pub ratio: ProductQuantizationOptionsRatio,
}

impl ProductQuantizationOptions {
    fn default_sample() -> u32 {
        65535
    }
}

impl Default for ProductQuantizationOptions {
    fn default() -> Self {
        Self {
            sample: Self::default_sample(),
            ratio: Default::default(),
        }
    }
}

#[repr(u16)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProductQuantizationOptionsRatio {
    X4 = 1,
    X8 = 2,
    X16 = 4,
    X32 = 8,
    X64 = 16,
}

impl Default for ProductQuantizationOptionsRatio {
    fn default() -> Self {
        Self::X4
    }
}

pub struct ProductQuantization<S: G> {
    dims: u16,
    ratio: u16,
    centroids: Vec<S::Scalar>,
    codes: MmapArray<u8>,
}

unsafe impl<S: G> Send for ProductQuantization<S> {}
unsafe impl<S: G> Sync for ProductQuantization<S> {}

impl<S: G> ProductQuantization<S> {
    fn codes(&self, i: u32) -> &[u8] {
        let width = self.dims.div_ceil(self.ratio);
        let s = i as usize * width as usize;
        let e = (i + 1) as usize * width as usize;
        &self.codes[s..e]
    }
}

impl<S: G> Quan<S> for ProductQuantization<S> {
    fn create(
        path: PathBuf,
        options: IndexOptions,
        quantization_options: QuantizationOptions,
        raw: &Arc<Raw<S>>,
    ) -> Self {
        Self::with_normalizer(path, options, quantization_options, raw, |_, _| ())
    }

    fn open(
        path: PathBuf,
        options: IndexOptions,
        quantization_options: QuantizationOptions,
        _: &Arc<Raw<S>>,
    ) -> Self {
        let centroids =
            serde_json::from_slice(&std::fs::read(path.join("centroids")).unwrap()).unwrap();
        let codes = MmapArray::open(path.join("codes"));
        Self {
            dims: options.vector.dims,
            ratio: quantization_options.unwrap_product_quantization().ratio as _,
            centroids,
            codes,
        }
    }

    fn distance(&self, lhs: &[S::Scalar], rhs: u32) -> F32 {
        let dims = self.dims;
        let ratio = self.ratio;
        let rhs = self.codes(rhs);
        S::product_quantization_distance(dims, ratio, &self.centroids, lhs, rhs)
    }

    fn distance2(&self, lhs: u32, rhs: u32) -> F32 {
        let dims = self.dims;
        let ratio = self.ratio;
        let lhs = self.codes(lhs);
        let rhs = self.codes(rhs);
        S::product_quantization_distance2(dims, ratio, &self.centroids, lhs, rhs)
    }
}

impl<S: G> ProductQuantization<S> {
    pub fn with_normalizer<F>(
        path: PathBuf,
        options: IndexOptions,
        quantization_options: QuantizationOptions,
        raw: &Raw<S>,
        normalizer: F,
    ) -> Self
    where
        F: Fn(u32, &mut [S::Scalar]),
    {
        std::fs::create_dir(&path).unwrap();
        let quantization_options = quantization_options.unwrap_product_quantization();
        let dims = options.vector.dims;
        let ratio = quantization_options.ratio as u16;
        let n = raw.len();
        let m = std::cmp::min(n, quantization_options.sample);
        let samples = {
            let f = sample(&mut thread_rng(), n as usize, m as usize).into_vec();
            let mut samples = Vec2::<S>::new(options.vector.dims, m as usize);
            for i in 0..m {
                samples[i as usize].copy_from_slice(raw.vector(f[i as usize] as u32));
            }
            samples
        };
        let width = dims.div_ceil(ratio);
        let mut centroids = vec![S::Scalar::zero(); 256 * dims as usize];
        for i in 0..width {
            let subdims = std::cmp::min(ratio, dims - ratio * i);
            let mut subsamples = Vec2::<S::L2>::new(subdims, m as usize);
            for j in 0..m {
                let src = &samples[j as usize][(i * ratio) as usize..][..subdims as usize];
                subsamples[j as usize].copy_from_slice(src);
            }
            let mut k_means = ElkanKMeans::<S::L2>::new(256, subsamples);
            for _ in 0..25 {
                if k_means.iterate() {
                    break;
                }
            }
            let centroid = k_means.finish();
            for j in 0u8..=255 {
                centroids[j as usize * dims as usize..][(i * ratio) as usize..][..subdims as usize]
                    .copy_from_slice(&centroid[j as usize]);
            }
        }
        let codes_iter = (0..n).flat_map(|i| {
            let mut vector = raw.vector(i).to_vec();
            normalizer(i, &mut vector);
            let width = dims.div_ceil(ratio);
            let mut result = Vec::with_capacity(width as usize);
            for i in 0..width {
                let subdims = std::cmp::min(ratio, dims - ratio * i);
                let mut minimal = F32::infinity();
                let mut target = 0u8;
                let left = &vector[(i * ratio) as usize..][..subdims as usize];
                for j in 0u8..=255 {
                    let right = &centroids[j as usize * dims as usize..][(i * ratio) as usize..]
                        [..subdims as usize];
                    let dis = S::L2::distance(left, right);
                    if dis < minimal {
                        minimal = dis;
                        target = j;
                    }
                }
                result.push(target);
            }
            result.into_iter()
        });
        sync_dir(&path);
        std::fs::write(
            path.join("centroids"),
            serde_json::to_string(&centroids).unwrap(),
        )
        .unwrap();
        let codes = MmapArray::create(path.join("codes"), codes_iter);
        Self {
            dims,
            ratio,
            centroids,
            codes,
        }
    }

    pub fn distance_with_delta(&self, lhs: &[S::Scalar], rhs: u32, delta: &[S::Scalar]) -> F32 {
        let dims = self.dims;
        let ratio = self.ratio;
        let rhs = self.codes(rhs);
        S::product_quantization_distance_with_delta(dims, ratio, &self.centroids, lhs, rhs, delta)
    }
}
