use super::elkan_k_means::ElkanKMeans;
use crate::algorithms::ivf::IvfError;
use crate::algorithms::quantization::{ProductQuantization, Quan, QuantizationOptions};
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
use std::sync::Arc;

type Vertex = Option<usize>;

pub struct IvfPq {
    centroids: MmapVec2,
    heads: MmapBox<[AtomicCell<Option<usize>>]>,
    vertexs: MmapBox<[UnsafeCell<Vertex>]>,
    //
    vectors: Arc<Vectors>,
    nprobe: usize,
    nlist: usize,
    //
    dims: u16,
    quantization: ProductQuantization,
    d: Distance,
}

unsafe impl Send for IvfPq {}
unsafe impl Sync for IvfPq {}

impl IvfPq {
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
        ProductQuantization::prebuild(storage, index_options, quantization_options);
        Ok(())
    }
    pub fn new(
        storage: &mut Storage,
        vectors: Arc<Vectors>,
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
        let dims = index_options.dims;
        let d = index_options.d;
        let m = std::cmp::min(nsample, n);
        let f = sample(&mut thread_rng(), n, m).into_vec();
        let mut samples = Vec2::new(dims, m);
        for i in 0..m {
            samples[i].copy_from_slice(vectors.get_vector(f[i]));
            index_options.d.elkan_k_means_normalize(&mut samples[i]);
        }
        let mut k_means = ElkanKMeans::new(nlist, samples, index_options.d);
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
        let quantization = ProductQuantization::build_with_normalizer(
            storage,
            index_options,
            quantization_options,
            vectors.clone(),
            |point| {
                let mut target = point.to_vec();
                d.elkan_k_means_normalize(&mut target);
                let mut result = (Scalar::INFINITY, 0);
                for i in 0..nlist {
                    let centroid = &centroids[i];
                    let dis = d.elkan_k_means_distance(&target, centroid);
                    result = std::cmp::min(result, (dis, i));
                }
                let centroid_id = result.1;
                let centroid_point = &centroids[centroid_id];
                for i in 0..dims {
                    point[i as usize] -= centroid_point[i as usize];
                }
            },
        );
        Ok(Self {
            centroids,
            heads,
            vertexs,
            vectors,
            nprobe,
            nlist,
            quantization,
            d,
            dims,
        })
    }
    pub fn load(
        storage: &mut Storage,
        vectors: Arc<Vectors>,
        nlist: usize,
        nprobe: usize,
        capacity: usize,
        memmap: Memmap,
        index_options: IndexOptions,
        quantization_options: QuantizationOptions,
    ) -> Result<Self, IvfError> {
        let dims = index_options.dims;
        let d = index_options.d;
        Ok(Self {
            centroids: MmapVec2::load(storage, dims, nlist),
            heads: unsafe { storage.alloc_mmap_slice(memmap, nlist).assume_init() },
            vertexs: unsafe { storage.alloc_mmap_slice(memmap, capacity).assume_init() },
            vectors: vectors.clone(),
            nprobe,
            nlist,
            quantization: ProductQuantization::load(
                storage,
                index_options,
                quantization_options,
                vectors,
            ),
            d,
            dims,
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
        self.d.elkan_k_means_normalize(&mut target);
        let mut lists = FixedHeap::new(self.nprobe);
        for i in 0..self.nlist {
            let centroid = &self.centroids[i];
            let dis = self.d.elkan_k_means_distance(&target, centroid);
            lists.push((dis, i));
        }
        let mut result = FilteredFixedHeap::new(k, filter);
        for (_, i) in lists.into_vec().into_iter() {
            let mut cursor = self.heads[i].load();
            while let Some(u) = cursor {
                let u_data = vectors.get_data(u);
                let u_dis =
                    self.quantization
                        .distance_with_delta(self.d, &target, u, &self.centroids[i]);
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
        let mut point = self.vectors.get_vector(x).to_vec();
        let mut target = point.clone();
        self.d.elkan_k_means_normalize(&mut target);
        let mut result = (Scalar::INFINITY, 0);
        for i in 0..self.nlist {
            let centroid = &self.centroids[i];
            let dis = self.d.elkan_k_means_distance(&target, centroid);
            result = std::cmp::min(result, (dis, i));
        }
        let centroid_id = result.1;
        let centroid_point = &self.centroids[result.1];
        for i in 0..self.dims {
            point[i as usize] -= centroid_point[i as usize];
        }
        self.quantization.insert(x, &point)?;
        loop {
            let next = self.heads[centroid_id].load();
            unsafe {
                vertexs[x].get().write(next);
            }
            let head = &self.heads[centroid_id];
            if head.compare_exchange(next, Some(x)).is_ok() {
                break;
            }
        }
        Ok(())
    }
}
