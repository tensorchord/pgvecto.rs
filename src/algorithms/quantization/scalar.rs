use crate::algorithms::quantization::Quan;
use crate::algorithms::quantization::QuantizationError;
use crate::algorithms::quantization::QuantizationOptions;
use crate::bgworker::index::IndexOptions;
use crate::bgworker::storage::Storage;
use crate::bgworker::storage::StoragePreallocator;
use crate::bgworker::storage_mmap::MmapBox;
use crate::bgworker::vectors::Vectors;
use crate::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalarQuantizationOptions {
    #[serde(default = "ScalarQuantizationOptions::default_memmap")]
    pub memmap: Memmap,
}

impl ScalarQuantizationOptions {
    fn default_memmap() -> Memmap {
        Memmap::Ram
    }
}

#[derive(Debug)]
pub struct ScalarQuantization {
    dims: u16,
    max: MmapBox<[Scalar]>,
    min: MmapBox<[Scalar]>,
    data: MmapBox<[u8]>,
}

impl ScalarQuantization {
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

impl Quan for ScalarQuantization {
    fn prebuild(
        storage: &mut StoragePreallocator,
        index_options: IndexOptions,
        quantization_options: QuantizationOptions,
    ) where
        Self: Sized,
    {
        let quantization_options = quantization_options.unwrap_scalar_quantization();
        let dims = index_options.dims;
        storage.palloc_mmap_slice::<Scalar>(quantization_options.memmap, dims as usize);
        storage.palloc_mmap_slice::<Scalar>(quantization_options.memmap, dims as usize);
        storage.palloc_mmap_slice::<u8>(
            quantization_options.memmap,
            dims as usize * index_options.capacity,
        );
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
        let mut max = unsafe {
            storage
                .alloc_mmap_slice::<Scalar>(quantization_options.memmap, dims as usize)
                .assume_init()
        };
        let mut min = unsafe {
            storage
                .alloc_mmap_slice::<Scalar>(quantization_options.memmap, dims as usize)
                .assume_init()
        };
        max.fill(Scalar::NEG_INFINITY);
        min.fill(Scalar::INFINITY);
        for i in 0..n {
            for j in 0..dims as usize {
                max[j] = std::cmp::max(max[j], vectors.get_vector(i)[j]);
                min[j] = std::cmp::max(min[j], vectors.get_vector(i)[j]);
            }
        }
        let data = unsafe {
            storage
                .alloc_mmap_slice::<u8>(
                    quantization_options.memmap,
                    dims as usize * index_options.capacity,
                )
                .assume_init()
        };
        Self {
            dims,
            max,
            min,
            data,
        }
    }

    fn load(
        storage: &mut Storage,
        index_options: IndexOptions,
        quantization_options: QuantizationOptions,
        _vectors: Arc<Vectors>,
    ) -> Self
    where
        Self: Sized,
    {
        let quantization_options = quantization_options.unwrap_product_quantization();
        let dims = index_options.dims;
        let max = unsafe {
            storage
                .alloc_mmap_slice::<Scalar>(quantization_options.memmap, dims as usize)
                .assume_init()
        };
        let min = unsafe {
            storage
                .alloc_mmap_slice::<Scalar>(quantization_options.memmap, dims as usize)
                .assume_init()
        };
        let data = unsafe {
            storage
                .alloc_mmap_slice::<u8>(
                    quantization_options.memmap,
                    dims as usize * index_options.capacity,
                )
                .assume_init()
        };
        Self {
            dims,
            max,
            min,
            data,
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

    fn distance(&self, d: Distance, lhs: &[Scalar], rhs: usize) -> Scalar {
        let dims = self.dims;
        assert!(dims as usize == lhs.len());
        let rhs = &self.data[rhs * dims as usize..][..dims as usize];
        d.scalar_quantization_distance(dims, &self.max, &self.min, lhs, rhs)
    }

    fn distance2(&self, d: Distance, lhs: usize, rhs: usize) -> Scalar {
        let dims = self.dims;
        let lhs = &self.data[lhs * dims as usize..][..dims as usize];
        let rhs = &self.data[rhs * dims as usize..][..dims as usize];
        d.scalar_quantization_distance2(dims, &self.max, &self.min, lhs, rhs)
    }
}
