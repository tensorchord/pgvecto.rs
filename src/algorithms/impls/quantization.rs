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
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ops::{Index, IndexMut};
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Clone, Error, Serialize, Deserialize)]
pub enum QuantizationError {
    //
}

pub struct QuantizationImpl<Q: Quantization> {
    data: MmapBox<[UnsafeCell<MaybeUninit<u8>>]>,
    vectors: Arc<Vectors>,
    width: usize,
    quantization: Q,
}

impl<Q: Quantization> QuantizationImpl<Q> {
    pub fn prebuild(
        storage: &mut StoragePreallocator,
        dims: u16,
        capacity: usize,
        memmap: Memmap,
    ) -> Result<(), QuantizationError> {
        let width = Q::width_dims(dims);
        storage.palloc_mmap_slice::<UnsafeCell<MaybeUninit<u8>>>(memmap, width * capacity);
        Ok(())
    }
    pub fn new(
        storage: &mut Storage,
        vectors: Arc<Vectors>,
        dims: u16,
        n: usize,
        nsample: usize,
        capacity: usize,
        memmap: Memmap,
    ) -> Result<Self, QuantizationError> {
        let m = std::cmp::min(n, nsample);
        let f = sample(&mut thread_rng(), n, m).into_vec();
        let mut samples = Vec2::new(dims, m);
        for i in 0..m {
            samples[i].copy_from_slice(vectors.get_vector(f[i]));
        }
        let quantization = Q::build(samples);
        let width = quantization.width();
        let data = unsafe {
            storage
                .alloc_mmap_slice(memmap, width * capacity)
                .assume_init()
        };
        for i in 0..n {
            let p = quantization.process(vectors.get_vector(i));
            unsafe {
                std::ptr::copy_nonoverlapping(
                    p.as_ptr(),
                    data[i * width..][..width].as_ptr() as *mut u8,
                    width,
                );
            }
        }
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
        memmap: Memmap,
    ) -> Result<Self, QuantizationError> {
        let width = quantization.width();
        Ok(Self {
            data: unsafe {
                storage
                    .alloc_mmap_slice(memmap, width * capacity)
                    .assume_init()
            },
            vectors,
            width: quantization.width(),
            quantization,
        })
    }
    pub fn insert(&self, x: usize) -> Result<(), QuantizationError> {
        let p = self.quantization.process(self.vectors.get_vector(x));
        unsafe {
            std::ptr::copy_nonoverlapping(
                p.as_ptr(),
                self.data[x * self.width..][..self.width].as_ptr() as *mut u8,
                self.width,
            );
        }
        Ok(())
    }
    pub fn process(&self, vector: &[Scalar]) -> Vec<u8> {
        self.quantization.process(vector)
    }
    pub fn distance(&self, lhs: &[u8], rhs: &[u8]) -> Scalar {
        self.quantization.distance(lhs, rhs)
    }
    pub fn get_vector(&self, i: usize) -> &[u8] {
        unsafe { assume_immutable_init(&self.data[i * self.width..][..self.width]) }
    }
}

pub trait Quantization: Clone + serde::Serialize + for<'a> serde::Deserialize<'a> {
    fn build(samples: Vec2) -> Self
    where
        Self: Sized;
    fn process(&self, point: &[Scalar]) -> Vec<u8>;
    fn distance(&self, lhs: &[u8], rhs: &[u8]) -> Scalar;
    fn width_dims(dims: u16) -> usize;
    fn width(&self) -> usize;
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ScalarQuantization<D: DistanceFamily> {
    dims: u16,
    max: Vec<Scalar>,
    min: Vec<Scalar>,
    _maker: PhantomData<D>,
}

impl<D: DistanceFamily> Quantization for ScalarQuantization<D> {
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

    fn distance(&self, lhs: &[u8], rhs: &[u8]) -> Scalar {
        let dims = self.dims;
        assert!(dims as usize == lhs.len());
        assert!(dims as usize == rhs.len());
        let mut result = D::QUANTIZATION_INITIAL_STATE;
        for i in 0..dims as usize {
            let lhs = Scalar(lhs[i] as Float) * (self.max[i] - self.min[i]) + self.min[i];
            let rhs = Scalar(rhs[i] as Float) * (self.max[i] - self.min[i]) + self.min[i];
            result = D::quantization_merge(result, D::quantization_new(&[lhs], &[rhs]));
        }
        D::quantization_finish(result)
    }

    fn width_dims(dims: u16) -> usize {
        dims as usize
    }

    fn width(&self) -> usize {
        self.dims as usize
    }
}

const DIV: u16 = 2;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ProductQuantization<D: DistanceFamily> {
    dims: u16,
    centroids: Vec<Vec2 /* n = 256, 1 <= dims <= 16 */>,
    matrixs: Vec<ProductQuantizationMatrix<D::QuantizationState>>,
}

impl<D: DistanceFamily> Quantization for ProductQuantization<D> {
    fn build(samples: Vec2) -> Self {
        let n = samples.len();
        let dims = samples.dims();
        let width = dims.div_ceil(DIV);
        let mut centroids = Vec::with_capacity(width as usize);
        let mut matrixs = Vec::with_capacity(width as usize);
        for i in 0..width {
            let subdims = std::cmp::min(DIV, dims - DIV * i);
            let mut subsamples = Vec2::new(subdims, n);
            for j in 0..n {
                let src = &samples[j][(i * DIV) as usize..][..subdims as usize];
                subsamples[j].copy_from_slice(src);
            }
            let mut k_means = ElkanKMeans::<L2>::new(256, subsamples);
            for _ in 0..200 {
                if k_means.iterate() {
                    break;
                }
            }
            let centroid = k_means.finish();
            let mut matrix = ProductQuantizationMatrix::new(D::QUANTIZATION_INITIAL_STATE);
            for i in 0u8..=255 {
                for j in i..=255 {
                    let state = D::quantization_new(&centroid[i as usize], &centroid[j as usize]);
                    matrix[(i, j)] = state;
                    matrix[(j, i)] = state;
                }
            }
            centroids.push(centroid);
            matrixs.push(matrix);
        }
        Self {
            dims,
            centroids,
            matrixs,
        }
    }

    fn process(&self, vector: &[Scalar]) -> Vec<u8> {
        let dims = self.dims;
        assert!(dims as usize == vector.len());
        let width = dims.div_ceil(DIV);
        let mut result = Vec::with_capacity(width as usize);
        for i in 0..width {
            let subdims = std::cmp::min(DIV, dims - DIV * i);
            let mut minimal = Scalar::INFINITY;
            let mut target = 0u8;
            for j in 0u8..=255 {
                let left = &vector[(i * DIV) as usize..][..subdims as usize];
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

    fn distance(&self, lhs: &[u8], rhs: &[u8]) -> Scalar {
        let dims = self.dims;
        let width = dims.div_ceil(DIV);
        assert!(lhs.len() == width as usize);
        assert!(rhs.len() == width as usize);
        let mut result = D::QUANTIZATION_INITIAL_STATE;
        for i in 0..width {
            let delta = self.matrixs[i as usize][(lhs[i as usize], rhs[i as usize])];
            result = D::quantization_merge(result, delta);
        }
        D::quantization_finish(result)
    }

    fn width_dims(dims: u16) -> usize {
        dims.div_ceil(DIV) as usize
    }

    fn width(&self) -> usize {
        self.dims.div_ceil(DIV) as usize
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ProductQuantizationMatrix<T>(Box<[T]>);

impl<T: Copy> ProductQuantizationMatrix<T> {
    pub fn new(initial: T) -> Self {
        let mut inner = Box::<[T]>::new_uninit_slice(65536);
        unsafe {
            let ptr = inner.as_mut_ptr() as *mut T;
            for i in 0..65536 {
                ptr.add(i).write(initial);
            }
        }
        let inner = unsafe { inner.assume_init() };
        Self(inner)
    }
}

impl<T> Index<(u8, u8)> for ProductQuantizationMatrix<T> {
    type Output = T;

    fn index(&self, (x, y): (u8, u8)) -> &Self::Output {
        &self.0[x as usize * 256 + y as usize]
    }
}

impl<T> IndexMut<(u8, u8)> for ProductQuantizationMatrix<T> {
    fn index_mut(&mut self, (x, y): (u8, u8)) -> &mut Self::Output {
        &mut self.0[x as usize * 256 + y as usize]
    }
}

unsafe fn assume_immutable_init<T>(slice: &[UnsafeCell<MaybeUninit<T>>]) -> &[T] {
    let p = slice.as_ptr().cast::<UnsafeCell<T>>() as *const T;
    std::slice::from_raw_parts(p, slice.len())
}
