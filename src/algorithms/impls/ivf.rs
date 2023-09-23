use super::elkan_k_means::ElkanKMeans;
use super::quantization::{
    ProductQuantization, Quantization, QuantizationOptions, ScalarQuantization, TrivialQuantization,
};
use crate::algorithms::ivf::IvfError;
use crate::algorithms::utils::filtered_fixed_heap::FilteredFixedHeap;
use crate::algorithms::utils::fixed_heap::FixedHeap;
use crate::algorithms::utils::mmap_vec2::MmapVec2;
use crate::algorithms::utils::vec2::Vec2;
use crate::bgworker::index::IndexOptions;
use crate::bgworker::storage::{Storage, StoragePreallocator};
use crate::bgworker::storage_mmap::MmapBox;
use crate::bgworker::vectors::Vectors;
use crate::prelude::*;
use crossbeam::atomic::AtomicCell;
use rand::seq::index::sample;
use rand::thread_rng;
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::sync::Arc;

type Vertex = Option<usize>;

pub struct IvfImpl<D: DistanceFamily> {
    centroids: MmapVec2,
    heads: MmapBox<[AtomicCell<Option<usize>>]>,
    vertexs: MmapBox<[UnsafeCell<Vertex>]>,
    //
    vectors: Arc<Vectors>,
    nprobe: usize,
    nlist: usize,
    //
    quantization: Box<dyn Quantization>,
    _maker: PhantomData<D>,
}

unsafe impl<D: DistanceFamily> Send for IvfImpl<D> {}
unsafe impl<D: DistanceFamily> Sync for IvfImpl<D> {}

impl<D: DistanceFamily> IvfImpl<D> {
    pub fn prebuild(
        storage: &mut StoragePreallocator,
        dims: u16,
        nlist: usize,
        capacity: usize,
        memmap: Memmap,
        index_options: IndexOptions,
        quantization_options: QuantizationOptions,
    ) -> Result<(), IvfError> {
        MmapVec2::prebuild(storage, dims, nlist);
        storage.palloc_mmap_slice::<AtomicCell<Option<usize>>>(memmap, nlist);
        storage.palloc_mmap_slice::<UnsafeCell<Vertex>>(memmap, capacity);
        match quantization_options {
            quantization_options @ QuantizationOptions::Trivial(_) => {
                TrivialQuantization::<D>::prebuild(storage, index_options, quantization_options);
            }
            quantization_options @ QuantizationOptions::Scalar(_) => {
                ScalarQuantization::<D>::prebuild(storage, index_options, quantization_options);
            }
            quantization_options @ QuantizationOptions::Product(_) => {
                ProductQuantization::<D>::prebuild(storage, index_options, quantization_options);
            }
        };
        Ok(())
    }
    pub fn new(
        storage: &mut Storage,
        vectors: Arc<Vectors>,
        dims: u16,
        n: usize,
        nlist: usize,
        nsample: usize,
        nprobe: usize,
        least_iterations: usize,
        iterations: usize,
        capacity: usize,
        memmap: Memmap,
        index_options: IndexOptions,
        quantization_options: QuantizationOptions,
    ) -> Result<Self, IvfError> {
        let m = std::cmp::min(nsample, n);
        let f = sample(&mut thread_rng(), n, m).into_vec();
        let mut samples = Vec2::new(dims, m);
        for i in 0..m {
            samples[i].copy_from_slice(vectors.get_vector(f[i]));
            D::elkan_k_means_normalize(&mut samples[i]);
        }
        let mut k_means = ElkanKMeans::<D>::new(nlist, samples);
        for _ in 0..least_iterations {
            k_means.iterate();
        }
        for _ in least_iterations..iterations {
            if k_means.iterate() {
                break;
            }
        }
        let k_means = k_means.finish();
        let centroids = {
            let mut centroids = MmapVec2::build(storage, dims, nlist);
            for i in 0..nlist {
                centroids[i].copy_from_slice(&k_means[i]);
            }
            centroids
        };
        let heads = {
            let mut heads = storage.alloc_mmap_slice(memmap, nlist);
            for i in 0..nlist {
                heads[i].write(AtomicCell::new(None));
            }
            unsafe { heads.assume_init() }
        };
        let vertexs = {
            let mut vertexs = storage.alloc_mmap_slice(memmap, capacity);
            for i in 0..capacity {
                vertexs[i].write(UnsafeCell::new(None));
            }
            unsafe { vertexs.assume_init() }
        };
        let quantization: Box<dyn Quantization> = match quantization_options {
            quantization_options @ QuantizationOptions::Trivial(_) => {
                Box::new(TrivialQuantization::<D>::build(
                    storage,
                    index_options,
                    quantization_options,
                    vectors.clone(),
                ))
            }
            quantization_options @ QuantizationOptions::Scalar(_) => {
                Box::new(ScalarQuantization::<D>::build(
                    storage,
                    index_options,
                    quantization_options,
                    vectors.clone(),
                ))
            }
            quantization_options @ QuantizationOptions::Product(_) => {
                Box::new(ProductQuantization::<D>::build(
                    storage,
                    index_options,
                    quantization_options,
                    vectors.clone(),
                ))
            }
        };
        Ok(Self {
            centroids,
            heads,
            vertexs,
            vectors,
            nprobe,
            nlist,
            quantization,
            _maker: PhantomData,
        })
    }
    pub fn load(
        storage: &mut Storage,
        dims: u16,
        vectors: Arc<Vectors>,
        nlist: usize,
        nprobe: usize,
        capacity: usize,
        memmap: Memmap,
        index_options: IndexOptions,
        quantization_options: QuantizationOptions,
    ) -> Result<Self, IvfError> {
        Ok(Self {
            centroids: MmapVec2::load(storage, dims, nlist),
            heads: unsafe { storage.alloc_mmap_slice(memmap, nlist).assume_init() },
            vertexs: unsafe { storage.alloc_mmap_slice(memmap, capacity).assume_init() },
            vectors: vectors.clone(),
            nprobe,
            nlist,
            quantization: match quantization_options {
                quantization_options @ QuantizationOptions::Trivial(_) => {
                    Box::new(TrivialQuantization::<D>::load(
                        storage,
                        index_options,
                        quantization_options,
                        vectors.clone(),
                    ))
                }
                quantization_options @ QuantizationOptions::Scalar(_) => {
                    Box::new(ScalarQuantization::<D>::load(
                        storage,
                        index_options,
                        quantization_options,
                        vectors.clone(),
                    ))
                }
                quantization_options @ QuantizationOptions::Product(_) => {
                    Box::new(ProductQuantization::<D>::load(
                        storage,
                        index_options,
                        quantization_options,
                        vectors.clone(),
                    ))
                }
            },
            _maker: PhantomData,
        })
    }
    pub fn search<F>(
        &self,
        mut target: Box<[Scalar]>,
        k: usize,
        filter: F,
    ) -> Result<Vec<(Scalar, u64)>, IvfError>
    where
        F: FnMut(u64) -> bool,
    {
        let vectors = self.vectors.as_ref();
        D::elkan_k_means_normalize(&mut target);
        let mut lists = FixedHeap::new(self.nprobe);
        for i in 0..self.nlist {
            let centroid = &self.centroids[i];
            let dis = D::elkan_k_means_distance(&target, centroid);
            lists.push((dis, i));
        }
        let mut result = FilteredFixedHeap::new(k, filter);
        for (_, i) in lists.into_vec().into_iter() {
            let mut cursor = self.heads[i].load();
            while let Some(u) = cursor {
                let u_data = vectors.get_data(u);
                let u_dis = self.quantization.distance(&target, u);
                result.push((u_dis, u_data));
                cursor = unsafe { *self.vertexs[u].get() };
            }
        }
        Ok(result.into_sorted_vec())
    }
    pub fn insert(&self, x: usize) -> Result<(), IvfError> {
        self._insert(x)?;
        Ok(())
    }
    pub fn _insert(&self, x: usize) -> Result<(), IvfError> {
        let vertexs = self.vertexs.as_ref();
        let mut target = self.vectors.get_vector(x).to_vec();
        D::elkan_k_means_normalize(&mut target);
        let mut result = (Scalar::INFINITY, 0);
        for i in 0..self.nlist {
            let centroid = &self.centroids[i];
            let dis = D::elkan_k_means_distance(&target, centroid);
            result = std::cmp::min(result, (dis, i));
        }
        loop {
            let next = self.heads[result.1].load();
            unsafe {
                vertexs[x].get().write(next);
            }
            let head = &self.heads[result.1];
            if head.compare_exchange(next, Some(x)).is_ok() {
                break;
            }
        }
        self.quantization.insert(x, &target)?;
        Ok(())
    }
}
