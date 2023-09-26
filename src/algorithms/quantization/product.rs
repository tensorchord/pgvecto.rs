use crate::algorithms::impls::elkan_k_means::ElkanKMeans;
use crate::algorithms::quantization::Quan;
use crate::algorithms::quantization::QuantizationError;
use crate::algorithms::quantization::QuantizationOptions;
use crate::algorithms::utils::vec2::Vec2;
use crate::bgworker::index::IndexOptions;
use crate::bgworker::storage::{Storage, StoragePreallocator};
use crate::bgworker::storage_mmap::MmapBox;
use crate::bgworker::vectors::Vectors;
use crate::prelude::*;
use rand::seq::index::sample;
use rand::thread_rng;

use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductQuantizationOptions {
    #[serde(default)]
    pub memmap: Memmap,
    #[serde(default = "ProductQuantizationOptions::default_sample")]
    pub sample: usize,
    #[serde(default)]
    pub ratio: ProductQuantizationOptionsRatio,
}

impl ProductQuantizationOptions {
    fn default_sample() -> usize {
        65535
    }
}

impl Default for ProductQuantizationOptions {
    fn default() -> Self {
        Self {
            memmap: Default::default(),
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

#[derive(Debug)]
pub struct ProductQuantization {
    dims: u16,
    centroids: MmapBox<[Scalar]>,
    data: MmapBox<[u8]>,
    ratio: u16,
}

impl ProductQuantization {
    fn process(&self, vector: &[Scalar]) -> Vec<u8> {
        let dims = self.dims;
        let ratio = self.ratio;
        assert!(dims as usize == vector.len());
        let width = dims.div_ceil(ratio);
        let mut result = Vec::with_capacity(width as usize);
        for i in 0..width {
            let subdims = std::cmp::min(ratio, dims - ratio * i);
            let mut minimal = Scalar::INFINITY;
            let mut target = 0u8;
            let left = &vector[(i * ratio) as usize..][..subdims as usize];
            for j in 0u8..=255 {
                let right = &self.centroids[j as usize * dims as usize..][(i * ratio) as usize..]
                    [..subdims as usize];
                let dis = Distance::L2.distance(left, right);
                if dis < minimal {
                    minimal = dis;
                    target = j;
                }
            }
            result.push(target);
        }
        result
    }
}

impl Quan for ProductQuantization {
    fn prebuild(
        storage: &mut StoragePreallocator,
        index_options: IndexOptions,
        quantization_options: QuantizationOptions,
    ) {
        let quantization_options = quantization_options.unwrap_product_quantization();
        let dims = index_options.dims;
        let ratio = quantization_options.ratio as u16;
        storage.palloc_mmap_slice::<Scalar>(quantization_options.memmap, 256 * dims as usize);
        let width = dims.div_ceil(ratio);
        storage.palloc_mmap_slice::<u8>(
            quantization_options.memmap,
            width as usize * index_options.capacity,
        );
    }

    fn build(
        storage: &mut Storage,
        index_options: IndexOptions,
        quantization_options: QuantizationOptions,
        vectors: Arc<Vectors>,
    ) -> Self {
        Self::build_with_normalizer(
            storage,
            index_options,
            quantization_options,
            vectors,
            |_| (),
        )
    }

    fn load(
        storage: &mut Storage,
        index_options: IndexOptions,
        quantization_options: QuantizationOptions,
        _vectors: Arc<Vectors>,
    ) -> Self {
        let quantization_options = quantization_options.unwrap_product_quantization();
        let dims = index_options.dims;
        let ratio = quantization_options.ratio as u16;
        let centroids = unsafe {
            storage
                .alloc_mmap_slice::<Scalar>(quantization_options.memmap, 256 * dims as usize)
                .assume_init()
        };
        let width = dims.div_ceil(ratio);
        let data = unsafe {
            storage
                .alloc_mmap_slice::<u8>(
                    quantization_options.memmap,
                    width as usize * index_options.capacity,
                )
                .assume_init()
        };
        Self {
            dims,
            centroids,
            data,
            ratio,
        }
    }

    fn insert(&self, x: usize, vector: &[Scalar]) -> Result<(), QuantizationError> {
        let ratio = self.ratio;
        let width = self.dims.div_ceil(ratio);
        let p = self.process(vector);
        unsafe {
            std::ptr::copy_nonoverlapping(
                p.as_ptr(),
                self.data[x * width as usize..][..width as usize].as_ptr() as *mut u8,
                width as usize,
            );
        }
        Ok(())
    }

    fn distance(&self, d: Distance, lhs: &[Scalar], rhs: usize) -> Scalar {
        let dims = self.dims;
        let ratio = self.ratio;
        let width = dims.div_ceil(ratio);
        let rhs = &self.data[rhs * width as usize..][..width as usize];
        d.product_quantization_distance(dims, ratio, &self.centroids, lhs, rhs)
    }

    fn distance2(&self, d: Distance, lhs: usize, rhs: usize) -> Scalar {
        let dims = self.dims;
        let ratio = self.ratio;
        let width = dims.div_ceil(ratio);
        let lhs = &self.data[lhs * width as usize..][..width as usize];
        let rhs = &self.data[rhs * width as usize..][..width as usize];
        d.product_quantization_distance2(dims, ratio, &self.centroids, lhs, rhs)
    }
}

impl ProductQuantization {
    pub fn build_with_normalizer<F>(
        storage: &mut Storage,
        index_options: IndexOptions,
        quantization_options: QuantizationOptions,
        vectors: Arc<Vectors>,
        normalizer: F,
    ) -> Self
    where
        F: Fn(&mut [Scalar]),
    {
        let quantization_options = quantization_options.unwrap_product_quantization();
        let dims = index_options.dims;
        let ratio = quantization_options.ratio as u16;
        let n = vectors.len();
        let m = std::cmp::min(n, quantization_options.sample);
        let f = sample(&mut thread_rng(), n, m).into_vec();
        let mut samples = Vec2::new(index_options.dims, m);
        for i in 0..m {
            samples[i].copy_from_slice(vectors.get_vector(f[i]));
            normalizer(&mut samples[i]);
        }
        let width = dims.div_ceil(ratio);
        let mut centroids = unsafe {
            storage
                .alloc_mmap_slice(quantization_options.memmap, 256 * dims as usize)
                .assume_init()
        };
        for i in 0..width {
            let subdims = std::cmp::min(ratio, dims - ratio * i);
            let mut subsamples = Vec2::new(subdims, m);
            for j in 0..m {
                let src = &samples[j][(i * ratio) as usize..][..subdims as usize];
                subsamples[j].copy_from_slice(src);
            }
            let mut k_means = ElkanKMeans::new(256, subsamples, Distance::L2);
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
        let data = unsafe {
            storage
                .alloc_mmap_slice::<u8>(
                    quantization_options.memmap,
                    width as usize * index_options.capacity,
                )
                .assume_init()
        };
        Self {
            dims,
            centroids,
            data,
            ratio,
        }
    }

    pub fn distance_with_delta(
        &self,
        d: Distance,
        lhs: &[Scalar],
        rhs: usize,
        delta: &[Scalar],
    ) -> Scalar {
        let dims = self.dims;
        let ratio = self.ratio;
        let width = dims.div_ceil(ratio);
        let rhs = &self.data[rhs * width as usize..][..width as usize];
        d.product_quantization_distance_with_delta(dims, ratio, &self.centroids, lhs, rhs, delta)
    }
}
