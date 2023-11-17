use crate::algorithms::clustering::elkan_k_means::ElkanKMeans;
use crate::algorithms::quantization::product::ProductQuantization;
use crate::algorithms::quantization::Quan;
use crate::algorithms::raw::Raw;
use crate::index::indexing::ivf::IvfIndexingOptions;
use crate::index::segments::growing::GrowingSegment;
use crate::index::segments::sealed::SealedSegment;
use crate::index::IndexOptions;
use crate::index::VectorOptions;
use crate::prelude::*;
use crate::utils::cells::SyncUnsafeCell;
use crate::utils::dir_ops::sync_dir;
use crate::utils::mmap_array::MmapArray;
use crate::utils::vec2::Vec2;
use rand::seq::index::sample;
use rand::thread_rng;
use std::fs::create_dir;
use std::path::PathBuf;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::Arc;

pub struct IvfPq {
    mmap: IvfMmap,
}

impl IvfPq {
    pub fn create(
        path: PathBuf,
        options: IndexOptions,
        sealed: Vec<Arc<SealedSegment>>,
        growing: Vec<Arc<GrowingSegment>>,
    ) -> Self {
        create_dir(&path).unwrap();
        let ram = make(path.clone(), sealed, growing, options);
        let mmap = save(ram, path.clone());
        sync_dir(&path);
        Self { mmap }
    }

    pub fn open(path: PathBuf, options: IndexOptions) -> Self {
        let mmap = load(path.clone(), options);
        Self { mmap }
    }

    pub fn len(&self) -> u32 {
        self.mmap.raw.len() as u32
    }

    pub fn vector(&self, i: u32) -> &[Scalar] {
        &self.mmap.raw.vector(i)
    }

    pub fn data(&self, i: u32) -> u64 {
        self.mmap.raw.data(i)
    }

    pub fn search<F: FnMut(u64) -> bool>(&self, k: usize, vector: &[Scalar], filter: F) -> Heap {
        search(&self.mmap, k, vector, filter)
    }
}

unsafe impl Send for IvfPq {}
unsafe impl Sync for IvfPq {}

pub struct IvfRam {
    raw: Arc<Raw>,
    quantization: ProductQuantization,
    // ----------------------
    dims: u16,
    d: Distance,
    // ----------------------
    nlist: u32,
    nprobe: u32,
    // ----------------------
    centroids: Vec2,
    heads: Vec<AtomicU32>,
    nexts: Vec<SyncUnsafeCell<u32>>,
}

unsafe impl Send for IvfRam {}
unsafe impl Sync for IvfRam {}

pub struct IvfMmap {
    raw: Arc<Raw>,
    quantization: ProductQuantization,
    // ----------------------
    dims: u16,
    d: Distance,
    // ----------------------
    nlist: u32,
    nprobe: u32,
    // ----------------------
    centroids: MmapArray<Scalar>,
    heads: MmapArray<u32>,
    nexts: MmapArray<u32>,
}

unsafe impl Send for IvfMmap {}
unsafe impl Sync for IvfMmap {}

impl IvfMmap {
    fn centroids(&self, i: u32) -> &[Scalar] {
        let s = i as usize * self.dims as usize;
        let e = (i + 1) as usize * self.dims as usize;
        &self.centroids[s..e]
    }
}

pub fn make(
    path: PathBuf,
    sealed: Vec<Arc<SealedSegment>>,
    growing: Vec<Arc<GrowingSegment>>,
    options: IndexOptions,
) -> IvfRam {
    let VectorOptions { dims, d } = options.vector;
    let IvfIndexingOptions {
        least_iterations,
        iterations,
        nlist,
        nprobe,
        nsample,
        quantization: quantization_opts,
    } = options.indexing.clone().unwrap_ivf();
    let raw = Arc::new(Raw::create(
        path.join("raw"),
        options.clone(),
        sealed,
        growing,
    ));
    let n = raw.len();
    let m = std::cmp::min(nsample, n);
    let f = sample(&mut thread_rng(), n as usize, m as usize).into_vec();
    let mut samples = Vec2::new(dims, m as usize);
    for i in 0..m {
        samples[i as usize].copy_from_slice(raw.vector(f[i as usize] as u32));
        d.elkan_k_means_normalize(&mut samples[i as usize]);
    }
    let mut k_means = ElkanKMeans::new(nlist as usize, samples, d);
    for _ in 0..least_iterations {
        k_means.iterate();
    }
    for _ in least_iterations..iterations {
        if k_means.iterate() {
            break;
        }
    }
    let centroids = k_means.finish();
    let heads = {
        let mut heads = Vec::with_capacity(nlist as usize);
        heads.resize_with(nlist as usize, || AtomicU32::new(u32::MAX));
        heads
    };
    let nexts = {
        let mut nexts = Vec::with_capacity(nlist as usize);
        nexts.resize_with(n as usize, || SyncUnsafeCell::new(u32::MAX));
        nexts
    };
    let quantization = ProductQuantization::with_normalizer(
        path.join("quantization"),
        options.clone(),
        quantization_opts,
        &raw,
        |i, target| {
            let mut vector = target.to_vec();
            d.elkan_k_means_normalize(&mut vector);
            let mut result = (Scalar::INFINITY, 0);
            for i in 0..nlist {
                let dis = d.elkan_k_means_distance(&vector, &centroids[i as usize]);
                result = std::cmp::min(result, (dis, i));
            }
            let centroid_id = result.1;
            loop {
                let next = heads[centroid_id as usize].load(Acquire);
                unsafe {
                    nexts[i as usize].get().write(next);
                }
                let o = &heads[centroid_id as usize];
                if o.compare_exchange(next, i, Release, Relaxed).is_ok() {
                    break;
                }
            }
            for i in 0..dims {
                target[i as usize] -= centroids[centroid_id as usize][i as usize];
            }
        },
    );
    IvfRam {
        raw,
        quantization,
        centroids,
        heads,
        nexts,
        nprobe,
        nlist,
        dims,
        d,
    }
}

pub fn save(mut ram: IvfRam, path: PathBuf) -> IvfMmap {
    let centroids = MmapArray::create(
        path.join("centroids"),
        (0..ram.nlist)
            .flat_map(|i| &ram.centroids[i as usize])
            .copied(),
    );
    let heads = MmapArray::create(
        path.join("heads"),
        ram.heads.iter_mut().map(|x| *x.get_mut()),
    );
    let nexts = MmapArray::create(
        path.join("nexts"),
        ram.nexts.iter_mut().map(|x| *x.get_mut()),
    );
    IvfMmap {
        raw: ram.raw,
        quantization: ram.quantization,
        dims: ram.dims,
        d: ram.d,
        nlist: ram.nlist,
        nprobe: ram.nprobe,
        centroids,
        heads,
        nexts,
    }
}

pub fn load(path: PathBuf, options: IndexOptions) -> IvfMmap {
    let raw = Arc::new(Raw::open(path.join("raw"), options.clone()));
    let quantization = ProductQuantization::open(
        path.join("quantization"),
        options.clone(),
        options.indexing.clone().unwrap_ivf().quantization,
        &raw,
    );
    let centroids = MmapArray::open(path.join("centroids"));
    let heads = MmapArray::open(path.join("heads"));
    let nexts = MmapArray::open(path.join("nexts"));
    let IvfIndexingOptions { nlist, nprobe, .. } = options.indexing.unwrap_ivf();
    IvfMmap {
        raw,
        quantization,
        dims: options.vector.dims,
        d: options.vector.d,
        nlist,
        nprobe,
        centroids,
        heads,
        nexts,
    }
}

pub fn search<F: FnMut(u64) -> bool>(
    mmap: &IvfMmap,
    k: usize,
    vector: &[Scalar],
    mut filter: F,
) -> Heap {
    let mut target = vector.to_vec();
    mmap.d.elkan_k_means_normalize(&mut target);
    let mut lists = Heap::new(mmap.nprobe as usize);
    for i in 0..mmap.nlist {
        let centroid = mmap.centroids(i);
        let distance = mmap.d.elkan_k_means_distance(&target, centroid);
        if lists.check(distance) {
            lists.push(HeapElement {
                distance,
                data: i as u64,
            });
        }
    }
    let lists = lists.into_sorted_vec();
    let mut result = Heap::new(k);
    for i in lists.iter().map(|e| e.data as u32) {
        let mut j = mmap.heads[i as usize];
        while u32::MAX != j {
            let distance =
                mmap.quantization
                    .distance_with_delta(mmap.d, &vector, j, mmap.centroids(i));
            let data = mmap.raw.data(j);
            if result.check(distance) {
                if filter(data) {
                    result.push(HeapElement { distance, data });
                }
            }
            j = mmap.nexts[j as usize];
        }
    }
    result
}
