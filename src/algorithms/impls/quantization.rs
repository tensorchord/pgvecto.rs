use super::elkan_k_means::ElkanKMeans;
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
use std::marker::PhantomData;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Clone, Error, Serialize, Deserialize)]
pub enum QuantizationError {
    //
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuantizationOptions {
    Trivial(TrivialQuantizationOptions),
    Scalar(ScalarQuantizationOptions),
    Product(ProductQuantizationOptions),
}

impl Default for QuantizationOptions {
    fn default() -> Self {
        Self::Trivial(TrivialQuantizationOptions {})
    }
}

impl QuantizationOptions {
    fn unwrap_trivial_quantization(self) -> TrivialQuantizationOptions {
        match self {
            Self::Trivial(x) => x,
            _ => unreachable!(),
        }
    }
    fn unwrap_scalar_quantization(self) -> ScalarQuantizationOptions {
        match self {
            Self::Scalar(x) => x,
            _ => unreachable!(),
        }
    }
    fn unwrap_product_quantization(self) -> ProductQuantizationOptions {
        match self {
            Self::Product(x) => x,
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrivialQuantizationOptions {}

impl TrivialQuantizationOptions {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalarQuantizationOptions {
    #[serde(default = "ScalarQuantizationOptions::default_memmap")]
    pub memmap: Memmap,
    #[serde(default = "ScalarQuantizationOptions::default_sample_size")]
    pub sample: usize,
}

impl ScalarQuantizationOptions {
    fn default_memmap() -> Memmap {
        Memmap::Ram
    }
    fn default_sample_size() -> usize {
        65535
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductQuantizationOptions {
    #[serde(default = "ProductQuantizationOptions::default_memmap")]
    pub memmap: Memmap,
    #[serde(default = "ProductQuantizationOptions::default_sample_size")]
    pub sample: usize,
}

impl ProductQuantizationOptions {
    fn default_memmap() -> Memmap {
        Memmap::Ram
    }
    fn default_sample_size() -> usize {
        65535
    }
}

const RATIO: u16 = 1;

pub trait Quantization {
    fn prebuild(
        storage: &mut StoragePreallocator,
        index_options: IndexOptions,
        quantization_options: QuantizationOptions,
    ) where
        Self: Sized;
    fn build(
        storage: &mut Storage,
        index_options: IndexOptions,
        quantization_options: QuantizationOptions,
        vectors: Arc<Vectors>,
    ) -> Self
    where
        Self: Sized;
    fn load(
        storage: &mut Storage,
        index_options: IndexOptions,
        quantization_options: QuantizationOptions,
        vectors: Arc<Vectors>,
    ) -> Self
    where
        Self: Sized;
    fn insert(&self, i: usize, point: &[Scalar]) -> Result<(), QuantizationError>;
    fn distance(&self, lhs: &[Scalar], rhs: usize) -> Scalar;
}

#[derive(Clone)]
pub struct TrivialQuantization<D: DistanceFamily> {
    vectors: Arc<Vectors>,
    _maker: PhantomData<D>,
}

impl<D: DistanceFamily> Quantization for TrivialQuantization<D> {
    fn prebuild(_: &mut StoragePreallocator, _: IndexOptions, _: QuantizationOptions)
    where
        Self: Sized,
    {
    }

    fn build(
        _: &mut Storage,
        _: IndexOptions,
        quantization_options: QuantizationOptions,
        vectors: Arc<Vectors>,
    ) -> Self
    where
        Self: Sized,
    {
        let _quantization_options = quantization_options.unwrap_trivial_quantization();
        Self {
            vectors,
            _maker: PhantomData,
        }
    }

    fn load(_: &mut Storage, _: IndexOptions, _: QuantizationOptions, vectors: Arc<Vectors>) -> Self
    where
        Self: Sized,
    {
        Self {
            vectors,
            _maker: PhantomData,
        }
    }

    fn insert(&self, _: usize, _: &[Scalar]) -> Result<(), QuantizationError> {
        Ok(())
    }

    fn distance(&self, lhs: &[Scalar], rhs: usize) -> Scalar {
        D::distance(lhs, self.vectors.get_vector(rhs))
    }
}

#[derive(Debug)]
pub struct ScalarQuantization<D: DistanceFamily> {
    dims: u16,
    max: MmapBox<[Scalar]>,
    min: MmapBox<[Scalar]>,
    data: MmapBox<[u8]>,
    _maker: PhantomData<D>,
}

impl<D: DistanceFamily> ScalarQuantization<D> {
    fn process(&self, vector: &[Scalar]) -> Vec<u8> {
        let dims = self.dims;
        assert!(dims as usize == vector.len());
        let mut result = vec![0u8; dims as usize];
        for i in 0..dims as usize {
            let w = ((vector[i] - self.min[i]) / (self.max[i] - self.min[i]) * 256.0).0 as u32;
            result[i] = w.clamp(0, 255) as u8;
        }
        result
    }
}

impl<D: DistanceFamily> Quantization for ScalarQuantization<D> {
    fn prebuild(
        storage: &mut StoragePreallocator,
        index_options: IndexOptions,
        _quantization_options: QuantizationOptions,
    ) where
        Self: Sized,
    {
        let dims = index_options.dims;
        storage.palloc_mmap_slice::<Scalar>(Memmap::Ram, dims as usize);
        storage.palloc_mmap_slice::<Scalar>(Memmap::Ram, dims as usize);
        storage.palloc_mmap_slice::<u8>(Memmap::Ram, dims as usize * index_options.capacity);
    }

    fn build(
        storage: &mut Storage,
        index_options: IndexOptions,
        quantization_options: QuantizationOptions,
        vectors: Arc<Vectors>,
    ) -> Self
    where
        Self: Sized,
    {
        let quantization_options = quantization_options.unwrap_scalar_quantization();
        let dims = index_options.dims;
        let n = vectors.len();
        let m = std::cmp::min(n, quantization_options.sample);
        let f = sample(&mut thread_rng(), n, m).into_vec();
        let mut samples = Vec2::new(dims, m);
        for i in 0..m {
            samples[i].copy_from_slice(vectors.get_vector(f[i]));
        }
        let dims = samples.dims();
        let mut max = unsafe {
            storage
                .alloc_mmap_slice::<Scalar>(Memmap::Ram, dims as usize)
                .assume_init()
        };
        let mut min = unsafe {
            storage
                .alloc_mmap_slice::<Scalar>(Memmap::Ram, dims as usize)
                .assume_init()
        };
        max.fill(Scalar::NEG_INFINITY);
        min.fill(Scalar::INFINITY);
        for i in 0..samples.len() {
            for j in 0..dims as usize {
                max[j] = std::cmp::max(max[j], samples[i][j]);
                min[j] = std::cmp::max(min[j], samples[i][j]);
            }
        }
        let data = unsafe {
            storage
                .alloc_mmap_slice::<u8>(Memmap::Ram, dims as usize * index_options.capacity)
                .assume_init()
        };
        Self {
            dims,
            max,
            min,
            data,
            _maker: PhantomData,
        }
    }

    fn distance(&self, lhs: &[Scalar], rhs: usize) -> Scalar {
        let dims = self.dims;
        assert!(dims as usize == lhs.len());
        let rhs = &self.data[rhs * dims as usize..][..dims as usize];
        let mut result = D::QUANTIZATION_INITIAL_STATE;
        for i in 0..dims as usize {
            let lhs = lhs[i];
            let rhs = Scalar(rhs[i] as Float / 256.0) * (self.max[i] - self.min[i]) + self.min[i];
            result = D::quantization_merge(result, D::quantization_new(&[lhs], &[rhs]));
        }
        D::quantization_finish(result)
    }

    fn load(
        storage: &mut Storage,
        index_options: IndexOptions,
        _quantization_options: QuantizationOptions,
        _vectors: Arc<Vectors>,
    ) -> Self
    where
        Self: Sized,
    {
        let dims = index_options.dims;
        let max = unsafe {
            storage
                .alloc_mmap_slice::<Scalar>(Memmap::Ram, dims as usize)
                .assume_init()
        };
        let min = unsafe {
            storage
                .alloc_mmap_slice::<Scalar>(Memmap::Ram, dims as usize)
                .assume_init()
        };
        let data = unsafe {
            storage
                .alloc_mmap_slice::<u8>(Memmap::Ram, dims as usize * index_options.capacity)
                .assume_init()
        };
        Self {
            dims,
            max,
            min,
            data,
            _maker: PhantomData,
        }
    }

    fn insert(&self, i: usize, point: &[Scalar]) -> Result<(), QuantizationError> {
        let p = self.process(point);
        unsafe {
            std::ptr::copy_nonoverlapping(
                p.as_ptr(),
                self.data[i * self.dims as usize..][..self.dims as usize].as_ptr() as *mut u8,
                self.dims as usize,
            );
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct ProductQuantization<D: DistanceFamily> {
    dims: u16,
    centroids: MmapBox<[Scalar]>,
    data: MmapBox<[u8]>,
    _maker: PhantomData<D>,
}

impl<D: DistanceFamily> ProductQuantization<D> {
    fn process(&self, vector: &[Scalar]) -> Vec<u8> {
        let dims = self.dims;
        assert!(dims as usize == vector.len());
        let width = dims.div_ceil(RATIO);
        let mut result = Vec::with_capacity(width as usize);
        for i in 0..width {
            let subdims = std::cmp::min(RATIO, dims - RATIO * i);
            let mut minimal = Scalar::INFINITY;
            let mut target = 0u8;
            let left = &vector[(i * RATIO) as usize..][..subdims as usize];
            for j in 0u8..=255 {
                let right = &self.centroids[j as usize * dims as usize..][(i * RATIO) as usize..]
                    [..subdims as usize];
                let dis = L2::distance(left, right);
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

impl<D: DistanceFamily> Quantization for ProductQuantization<D> {
    fn prebuild(
        storage: &mut StoragePreallocator,
        index_options: IndexOptions,
        _quantization_options: QuantizationOptions,
    ) {
        let dims = index_options.dims;
        storage.palloc_mmap_slice::<Scalar>(Memmap::Ram, 256 * dims as usize);
        let width = dims.div_ceil(RATIO);
        storage.palloc_mmap_slice::<u8>(Memmap::Ram, width as usize * index_options.capacity);
    }

    fn build(
        storage: &mut Storage,
        index_options: IndexOptions,
        quantization_options: QuantizationOptions,
        vectors: Arc<Vectors>,
    ) -> Self {
        let quantization_options = quantization_options.unwrap_product_quantization();
        let dims = index_options.dims;
        let n = vectors.len();
        let m = std::cmp::min(n, quantization_options.sample);
        let f = sample(&mut thread_rng(), n, m).into_vec();
        let mut samples = Vec2::new(index_options.dims, m);
        for i in 0..m {
            samples[i].copy_from_slice(vectors.get_vector(f[i]));
        }
        let width = dims.div_ceil(RATIO);
        let mut centroids = unsafe {
            storage
                .alloc_mmap_slice(Memmap::Ram, 256 * dims as usize)
                .assume_init()
        };
        for i in 0..width {
            let subdims = std::cmp::min(RATIO, dims - RATIO * i);
            let mut subsamples = Vec2::new(subdims, m);
            for j in 0..m {
                let src = &samples[j][(i * RATIO) as usize..][..subdims as usize];
                subsamples[j].copy_from_slice(src);
            }
            let mut k_means = ElkanKMeans::<L2>::new(256, subsamples);
            for _ in 0..25 {
                if k_means.iterate() {
                    break;
                }
            }
            let centroid = k_means.finish();
            for j in 0u8..=255 {
                centroids[j as usize * dims as usize..][(i * RATIO) as usize..][..subdims as usize]
                    .copy_from_slice(&centroid[j as usize]);
            }
        }
        let data = unsafe {
            storage
                .alloc_mmap_slice::<u8>(Memmap::Ram, width as usize * index_options.capacity)
                .assume_init()
        };
        Self {
            dims,
            centroids,
            data,
            _maker: PhantomData,
        }
    }

    fn load(
        storage: &mut Storage,
        index_options: IndexOptions,
        _quantization_options: QuantizationOptions,
        _vectors: Arc<Vectors>,
    ) -> Self {
        let dims = index_options.dims;
        let centroids = unsafe {
            storage
                .alloc_mmap_slice::<Scalar>(Memmap::Ram, 256 * dims as usize)
                .assume_init()
        };
        let width = dims.div_ceil(RATIO);
        let data = unsafe {
            storage
                .alloc_mmap_slice::<u8>(Memmap::Ram, width as usize * index_options.capacity)
                .assume_init()
        };
        Self {
            dims,
            centroids,
            data,
            _maker: PhantomData,
        }
    }

    fn insert(&self, x: usize, vector: &[Scalar]) -> Result<(), QuantizationError> {
        let width = self.dims.div_ceil(RATIO);
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

    fn distance(&self, lhs: &[Scalar], rhs: usize) -> Scalar {
        let dims = self.dims;
        let width = dims.div_ceil(RATIO);
        assert!(lhs.len() == width as usize);
        let rhs = &self.data[rhs * width as usize..][..width as usize];
        let mut result = D::QUANTIZATION_INITIAL_STATE;
        for i in 0..width {
            let subdims = std::cmp::min(RATIO, dims - RATIO * i);
            let lhs = &lhs[(i * RATIO) as usize..][..subdims as usize];
            let rhs = &self.centroids[rhs[i as usize] as usize * dims as usize..]
                [(i * RATIO) as usize..][..subdims as usize];
            let delta = D::quantization_new(lhs, rhs);
            result = D::quantization_merge(result, delta);
        }
        D::quantization_finish(result)
    }
}
