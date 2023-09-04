use super::elkan_k_means::ElkanKMeans;
use crate::algorithms::utils::vec2::Vec2;
use crate::bgworker::storage::{Storage, StoragePreallocator};
use crate::bgworker::storage_mmap::MmapBox;
use crate::bgworker::vectors::Vectors;
use crate::prelude::*;
use rand::seq::index::sample;
use rand::thread_rng;
use serde::{Deserialize, Serialize};
use std::cell::UnsafeCell;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Clone, Error, Serialize, Deserialize)]
pub enum QuantizationError {
    //
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuantizationOptions {
    #[serde(default = "QuantizationOptions::default_memmap")]
    pub memmap: Memmap,
    #[serde(default = "QuantizationOptions::default_sample_size")]
    pub sample: usize,
}

impl QuantizationOptions {
    fn default_memmap() -> Memmap {
        Memmap::Ram
    }
    fn default_sample_size() -> usize {
        65535
    }
}

pub struct QuantizationImpl<Q: Quantization> {
    data: Option<MmapBox<[UnsafeCell<MaybeUninit<Q::Type>>]>>,
    vectors: Arc<Vectors>,
    width: usize,
    quantization: Q,
}

impl<Q: Quantization> QuantizationImpl<Q> {
    pub fn prebuild(
        storage: &mut StoragePreallocator,
        dims: u16,
        capacity: usize,
        options: QuantizationOptions,
    ) -> Result<(), QuantizationError> {
        let width = Q::width_by_dims(dims);
        if !Q::NOP {
            storage
                .palloc_mmap_slice::<UnsafeCell<MaybeUninit<u8>>>(options.memmap, width * capacity);
        }
        Ok(())
    }
    pub fn new(
        storage: &mut Storage,
        vectors: Arc<Vectors>,
        dims: u16,
        n: usize,
        capacity: usize,
        options: QuantizationOptions,
    ) -> Result<Self, QuantizationError> {
        let m = std::cmp::min(n, options.sample);
        let f = sample(&mut thread_rng(), n, m).into_vec();
        let mut samples = Vec2::new(dims, m);
        for i in 0..m {
            samples[i].copy_from_slice(vectors.get_vector(f[i]));
        }
        let quantization = Q::build(samples);
        let width = quantization.width();
        let data = if !Q::NOP {
            let data = unsafe {
                storage
                    .alloc_mmap_slice(options.memmap, width * capacity)
                    .assume_init()
            };
            for i in 0..n {
                let p = quantization.process(vectors.get_vector(i));
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        p.as_ptr(),
                        data[i * width..][..width].as_ptr() as *mut Q::Type,
                        width,
                    );
                }
            }
            Some(data)
        } else {
            None
        };
        Ok(Self {
            data,
            width,
            vectors,
            quantization,
        })
    }
    pub fn save(&self) -> Q {
        self.quantization.clone()
    }
    pub fn load(
        storage: &mut Storage,
        vectors: Arc<Vectors>,
        quantization: Q,
        capacity: usize,
        options: QuantizationOptions,
    ) -> Result<Self, QuantizationError> {
        let width = quantization.width();
        Ok(Self {
            data: if !Q::NOP {
                let data = unsafe {
                    storage
                        .alloc_mmap_slice(options.memmap, width * capacity)
                        .assume_init()
                };
                Some(data)
            } else {
                None
            },
            vectors,
            width: quantization.width(),
            quantization,
        })
    }
    pub fn insert(&self, x: usize) -> Result<(), QuantizationError> {
        if let Some(data) = self.data.as_ref() {
            let p = self.quantization.process(self.vectors.get_vector(x));
            unsafe {
                std::ptr::copy_nonoverlapping(
                    p.as_ptr(),
                    data[x * self.width..][..self.width].as_ptr() as *mut Q::Type,
                    self.width,
                );
            }
        }
        Ok(())
    }
    pub fn asymmetric_distance(&self, lhs: &[Scalar], rhs: &[Q::Type]) -> Scalar {
        self.quantization.asymmetric_distance(lhs, rhs)
    }
    pub fn get_vector(&self, i: usize) -> &[Q::Type] {
        if let Some(data) = self.data.as_ref() {
            unsafe { assume_immutable_init(&data[i * self.width..][..self.width]) }
        } else {
            unsafe { std::mem::transmute(self.vectors.get_vector(i)) }
        }
    }
}

pub trait Quantization: Clone + serde::Serialize + for<'a> serde::Deserialize<'a> {
    const NOP: bool;
    type Type: Debug + Clone + Copy;
    fn build(samples: Vec2) -> Self
    where
        Self: Sized;
    fn process(&self, point: &[Scalar]) -> Vec<Self::Type>;
    fn asymmetric_distance(&self, lhs: &[Scalar], rhs: &[Self::Type]) -> Scalar;
    fn width_by_dims(dims: u16) -> usize;
    fn width(&self) -> usize;
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NopQuantization<D: DistanceFamily> {
    dims: u16,
    _maker: PhantomData<D>,
}

impl<D: DistanceFamily> Quantization for NopQuantization<D> {
    const NOP: bool = true;

    type Type = Scalar;

    fn build(samples: Vec2) -> Self
    where
        Self: Sized,
    {
        Self {
            dims: samples.dims(),
            _maker: PhantomData,
        }
    }

    fn process(&self, point: &[Scalar]) -> Vec<Self::Type> {
        point.to_vec()
    }

    fn asymmetric_distance(&self, lhs: &[Scalar], rhs: &[Self::Type]) -> Scalar {
        D::distance(lhs, rhs)
    }

    fn width_by_dims(dims: u16) -> usize {
        dims as usize
    }

    fn width(&self) -> usize {
        self.dims as usize
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ScalarQuantization<D: DistanceFamily> {
    dims: u16,
    max: Vec<Scalar>,
    min: Vec<Scalar>,
    _maker: PhantomData<D>,
}

impl<D: DistanceFamily> Quantization for ScalarQuantization<D> {
    const NOP: bool = false;

    type Type = u8;

    fn build(samples: Vec2) -> Self {
        let dims = samples.dims();
        let mut max = vec![Scalar::NEG_INFINITY; dims as _];
        let mut min = vec![Scalar::INFINITY; dims as _];
        for i in 0..samples.len() {
            for j in 0..dims as usize {
                max[j] = std::cmp::max(max[j], samples[i][j]);
                min[j] = std::cmp::max(min[j], samples[i][j]);
            }
        }
        Self {
            dims,
            max,
            min,
            _maker: PhantomData,
        }
    }

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

    fn asymmetric_distance(&self, lhs: &[Scalar], rhs: &[u8]) -> Scalar {
        let dims = self.dims;
        assert!(dims as usize == lhs.len());
        assert!(dims as usize == rhs.len());
        let mut result = D::QUANTIZATION_INITIAL_STATE;
        for i in 0..dims as usize {
            let lhs = lhs[i];
            let rhs = Scalar(rhs[i] as Float / 256.0) * (self.max[i] - self.min[i]) + self.min[i];
            result = D::quantization_merge(result, D::quantization_new(&[lhs], &[rhs]));
        }
        D::quantization_finish(result)
    }

    fn width_by_dims(dims: u16) -> usize {
        dims as usize
    }

    fn width(&self) -> usize {
        self.dims as usize
    }
}

const RATIO: u16 = 1;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ProductQuantization<D: DistanceFamily> {
    dims: u16,
    centroids: Vec<Vec2 /* n = 256, 1 <= dims <= 16 */>,
    _maker: PhantomData<D>,
}

impl<D: DistanceFamily> Quantization for ProductQuantization<D> {
    const NOP: bool = false;

    type Type = u8;

    fn build(samples: Vec2) -> Self {
        let n = samples.len();
        let dims = samples.dims();
        let width = dims.div_ceil(RATIO);
        let mut centroids = Vec::with_capacity(width as usize);
        for i in 0..width {
            let subdims = std::cmp::min(RATIO, dims - RATIO * i);
            let mut subsamples = Vec2::new(subdims, n);
            for j in 0..n {
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
            centroids.push(centroid);
        }
        Self {
            dims,
            centroids,
            _maker: PhantomData,
        }
    }

    fn process(&self, vector: &[Scalar]) -> Vec<u8> {
        let dims = self.dims;
        assert!(dims as usize == vector.len());
        let width = dims.div_ceil(RATIO);
        let mut result = Vec::with_capacity(width as usize);
        for i in 0..width {
            let subdims = std::cmp::min(RATIO, dims - RATIO * i);
            let mut minimal = Scalar::INFINITY;
            let mut target = 0u8;
            for j in 0u8..=255 {
                let left = &vector[(i * RATIO) as usize..][..subdims as usize];
                let right = &self.centroids[i as usize][j as usize];
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

    fn asymmetric_distance(&self, lhs: &[Scalar], rhs: &[Self::Type]) -> Scalar {
        let dims = self.dims;
        let width = dims.div_ceil(RATIO);
        assert!(lhs.len() == width as usize);
        assert!(rhs.len() == width as usize);
        let mut result = D::QUANTIZATION_INITIAL_STATE;
        for i in 0..width {
            let subdims = std::cmp::min(RATIO, dims - RATIO * i);
            let lhs = &lhs[(i * RATIO) as usize..][..subdims as usize];
            let rhs = &self.centroids[i as usize][rhs[i as usize] as usize];
            let delta = D::quantization_new(lhs, rhs);
            result = D::quantization_merge(result, delta);
        }
        D::quantization_finish(result)
    }

    fn width_by_dims(dims: u16) -> usize {
        dims.div_ceil(RATIO) as usize
    }

    fn width(&self) -> usize {
        self.dims.div_ceil(RATIO) as usize
    }
}

unsafe fn assume_immutable_init<T>(slice: &[UnsafeCell<MaybeUninit<T>>]) -> &[T] {
    let p = slice.as_ptr().cast::<UnsafeCell<T>>() as *const T;
    std::slice::from_raw_parts(p, slice.len())
}
