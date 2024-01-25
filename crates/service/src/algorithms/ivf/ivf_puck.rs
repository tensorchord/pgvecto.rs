use crate::algorithms::clustering::elkan_k_means::ElkanKMeans;
use crate::algorithms::quantization::product::ProductQuantization;
use crate::algorithms::quantization::product::ProductQuantizationOptions;
use crate::algorithms::quantization::Quan;
use crate::algorithms::quantization::QuantizationOptions;
use crate::algorithms::raw::Raw;
use crate::index::indexing::ivf::IvfIndexingOptions;
use crate::index::segments::growing::GrowingSegment;
use crate::index::segments::sealed::SealedSegment;
use crate::index::IndexOptions;
use crate::index::SearchOptions;
use crate::index::VectorOptions;
use crate::prelude::*;
use crate::utils::cells::SyncUnsafeCell;
use crate::utils::dir_ops::sync_dir;
use crate::utils::element_heap::ElementHeap;
use crate::utils::mmap_array::MmapArray;
use crate::utils::vec2::Vec2;
use rand::seq::index::sample;
use rand::thread_rng;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::create_dir;
use std::path::PathBuf;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::Arc;

pub struct IvfPuck<S: G> {
    mmap: IvfMmap<S>,
}

impl<S: G> IvfPuck<S> {
    pub fn create(
        path: PathBuf,
        options: IndexOptions,
        sealed: Vec<Arc<SealedSegment<S>>>,
        growing: Vec<Arc<GrowingSegment<S>>>,
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
        self.mmap.raw.len()
    }

    pub fn vector(&self, i: u32) -> &[S::Scalar] {
        self.mmap.raw.vector(i)
    }

    pub fn payload(&self, i: u32) -> Payload {
        self.mmap.raw.payload(i)
    }

    pub fn basic(
        &self,
        vector: &[S::Scalar],
        opts: &SearchOptions,
        filter: impl Filter,
    ) -> BinaryHeap<Reverse<Element>> {
        basic(&self.mmap, vector, opts.ivf_nprobe, filter)
    }

    pub fn vbase<'a>(
        &'a self,
        vector: &'a [S::Scalar],
        opts: &'a SearchOptions,
        filter: impl Filter + 'a,
    ) -> (Vec<Element>, Box<(dyn Iterator<Item = Element> + 'a)>) {
        vbase(&self.mmap, vector, opts.ivf_nprobe, filter)
    }
}

unsafe impl<S: G> Send for IvfPuck<S> {}
unsafe impl<S: G> Sync for IvfPuck<S> {}

pub struct IvfRam<S: G> {
    raw: Arc<Raw<S>>,
    quantization1: ProductQuantization<S>,
    quantization2: ProductQuantization<S>,
    // ----------------------
    dims: u16,
    // ----------------------
    nlist: u32,
    // ----------------------
    coarse_centroids: Vec2<S>,
    fine_centroids: Vec2<S>,
    heads: Vec<AtomicU32>,
    nexts: Vec<SyncUnsafeCell<u32>>,
}

unsafe impl<S: G> Send for IvfRam<S> {}
unsafe impl<S: G> Sync for IvfRam<S> {}

pub struct IvfMmap<S: G> {
    raw: Arc<Raw<S>>,
    quantization1: ProductQuantization<S>,
    quantization2: ProductQuantization<S>,
    // ----------------------
    dims: u16,
    // ----------------------
    nlist: u32,
    // ----------------------
    coarse_centroids: MmapArray<S::Scalar>,
    fine_centroids: MmapArray<S::Scalar>,
    heads: MmapArray<u32>,
    nexts: MmapArray<u32>,
}

unsafe impl<S: G> Send for IvfMmap<S> {}
unsafe impl<S: G> Sync for IvfMmap<S> {}

impl<S: G> IvfMmap<S> {
    fn coarse_centroids(&self, i: u32) -> &[S::Scalar] {
        let s = i as usize * self.dims as usize;
        let e = (i + 1) as usize * self.dims as usize;
        &self.coarse_centroids[s..e]
    }
    fn fine_centroids(&self, i: u32) -> &[S::Scalar] {
        let s = i as usize * self.dims as usize;
        let e = (i + 1) as usize * self.dims as usize;
        &self.fine_centroids[s..e]
    }
}

pub fn make<S: G>(
    path: PathBuf,
    sealed: Vec<Arc<SealedSegment<S>>>,
    growing: Vec<Arc<GrowingSegment<S>>>,
    options: IndexOptions,
) -> IvfRam<S> {
    let coarse_search_count = 8; // TODO: put this parameter in a proper location
    let VectorOptions { dims, .. } = options.vector;
    let IvfIndexingOptions {
        least_iterations,
        iterations,
        nlist,
        nsample,
        is_puck: _,
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
        S::elkan_k_means_normalize(&mut samples[i as usize]);
    }
    let mut k_means = ElkanKMeans::new(nlist as usize, samples.clone());
    for _ in 0..least_iterations {
        k_means.iterate();
    }
    for _ in least_iterations..iterations {
        if k_means.iterate() {
            break;
        }
    }
    let coarse_centroids = k_means.finish();
    for i in 0..m {
        let mut vector = samples[i as usize].to_vec();
        S::elkan_k_means_normalize(&mut vector);
        let mut result = (F32::infinity(), 0);
        for i in 0..nlist {
            let dis = S::elkan_k_means_distance(&vector, &coarse_centroids[i as usize]);
            result = std::cmp::min(result, (dis, i));
        }
        let centroid_id = result.1;
        for j in 0..dims {
            samples[i as usize][j as usize] -= coarse_centroids[centroid_id as usize][j as usize];
        }
    }
    let mut k_means = ElkanKMeans::new(nlist as usize, samples);
    for _ in 0..least_iterations {
        k_means.iterate();
    }
    for _ in least_iterations..iterations {
        if k_means.iterate() {
            break;
        }
    }
    let fine_centroids = k_means.finish();
    let heads = {
        let mut heads = Vec::with_capacity((nlist * nlist) as usize);
        heads.resize_with((nlist * nlist) as usize, || AtomicU32::new(u32::MAX));
        heads
    };
    let nexts = {
        let mut nexts = Vec::with_capacity((nlist * nlist) as usize);
        nexts.resize_with(n as usize, || SyncUnsafeCell::new(u32::MAX));
        nexts
    };
    let assigner = |i| {
        let mut vector = raw.vector(i).to_vec();
        S::elkan_k_means_normalize(&mut vector);
        let mut coarse_result = BinaryHeap::new();
        let mut result = BinaryHeap::new();
        for i in 0..nlist {
            let dis = S::elkan_k_means_distance(&vector, &coarse_centroids[i as usize]);
            coarse_result.push(Reverse((dis, i)));
        }
        for _ in 0..coarse_search_count {
            let coarse_id = coarse_result.pop().unwrap().0 .1;
            for j in 0..nlist {
                let mut centroid = coarse_centroids[coarse_id as usize].to_vec();
                for k in 0..dims {
                    centroid[k as usize] += fine_centroids[j as usize][k as usize];
                }
                let dis = S::elkan_k_means_distance(&vector, &centroid);
                result.push(Reverse((dis, (coarse_id, j))));
            }
        }
        result.peek().unwrap().0 .1
    };
    for i in 0..n {
        let (coarse_id, fine_id) = assigner(i);
        let bucket_id = coarse_id * nlist + fine_id;
        loop {
            let next = heads[bucket_id as usize].load(Acquire);
            unsafe {
                nexts[i as usize].get().write(next);
            }
            let o = &heads[bucket_id as usize];
            if o.compare_exchange(next, i, Release, Relaxed).is_ok() {
                break;
            }
        }
    }
    let quantization1 = ProductQuantization::with_normalizer(
        path.join("quantization1"),
        options.clone(),
        quantization_opts,
        &raw,
        |i, target| {
            let (coarse_id, fine_id) = assigner(i);
            for i in 0..dims {
                target[i as usize] -= coarse_centroids[coarse_id as usize][i as usize];
                target[i as usize] -= fine_centroids[fine_id as usize][i as usize];
            }
        },
    );
    let quantization2 = ProductQuantization::with_normalizer(
        path.join("quantization2"),
        options.clone(),
        QuantizationOptions::Product(ProductQuantizationOptions::default()),
        &raw,
        |i, target| {
            let (coarse_id, fine_id) = assigner(i);
            for i in 0..dims {
                target[i as usize] -= coarse_centroids[coarse_id as usize][i as usize];
                target[i as usize] -= fine_centroids[fine_id as usize][i as usize];
            }
        },
    );
    IvfRam {
        raw,
        quantization1,
        quantization2,
        coarse_centroids,
        fine_centroids,
        heads,
        nexts,
        nlist,
        dims,
    }
}

pub fn save<S: G>(mut ram: IvfRam<S>, path: PathBuf) -> IvfMmap<S> {
    let coarse_centroids = MmapArray::create(
        path.join("coarse_centroids"),
        (0..ram.nlist)
            .flat_map(|i| &ram.coarse_centroids[i as usize])
            .copied(),
    );
    let fine_centroids = MmapArray::create(
        path.join("fine_centroids"),
        (0..ram.nlist)
            .flat_map(|i| &ram.fine_centroids[i as usize])
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
        quantization1: ram.quantization1,
        quantization2: ram.quantization2,
        dims: ram.dims,
        nlist: ram.nlist,
        coarse_centroids,
        fine_centroids,
        heads,
        nexts,
    }
}

pub fn load<S: G>(path: PathBuf, options: IndexOptions) -> IvfMmap<S> {
    let raw = Arc::new(Raw::open(path.join("raw"), options.clone()));
    let quantization1 = ProductQuantization::open(
        path.join("quantization1"),
        options.clone(),
        options.indexing.clone().unwrap_ivf().quantization,
        &raw,
    );
    let quantization2 = ProductQuantization::open(
        path.join("quantization2"),
        options.clone(),
        QuantizationOptions::Product(ProductQuantizationOptions::default()),
        &raw,
    );
    let coarse_centroids = MmapArray::open(path.join("coarse_centroids"));
    let fine_centroids = MmapArray::open(path.join("fine_centroids"));
    let heads = MmapArray::open(path.join("heads"));
    let nexts = MmapArray::open(path.join("nexts"));
    let IvfIndexingOptions { nlist, .. } = options.indexing.unwrap_ivf();
    IvfMmap {
        raw,
        quantization1,
        quantization2,
        dims: options.vector.dims,
        nlist,
        coarse_centroids,
        fine_centroids,
        heads,
        nexts,
    }
}

pub fn basic<S: G>(
    mmap: &IvfMmap<S>,
    vector: &[S::Scalar],
    nprobe: u32,
    mut filter: impl Filter,
) -> BinaryHeap<Reverse<Element>> {
    let coarse_search_count = 8; // TODO: put this parameter in a proper location
    let over_sample_size = std::cmp::min(1000, mmap.raw.len()); // TODO: put this parameter in a proper location
    let mut target = vector.to_vec();
    S::elkan_k_means_normalize(&mut target);
    let mut coarse_result = ElementHeap::new(coarse_search_count as usize);
    for i in 0..mmap.nlist {
        let distance = S::elkan_k_means_distance(&target, mmap.coarse_centroids(i));
        if coarse_result.check(distance) {
            coarse_result.push(Element {
                distance,
                payload: i as Payload,
            });
        }
    }
    let coarse_result = coarse_result.into_sorted_vec();
    let mut lists = ElementHeap::new(nprobe as usize);
    for coarse_id in coarse_result.iter().map(|e| e.payload as u32) {
        for j in 0..mmap.nlist {
            let mut centroid = mmap.coarse_centroids(coarse_id).to_vec();
            for k in 0..mmap.dims {
                centroid[k as usize] += mmap.fine_centroids(j)[k as usize];
            }
            let distance = S::elkan_k_means_distance(&target, &centroid);
            if lists.check(distance) {
                lists.push(Element {
                    distance,
                    payload: (coarse_id * mmap.nlist + j) as Payload,
                });
            }
        }
    }
    let lists = lists.into_sorted_vec();
    let mut result = BinaryHeap::new();
    for i in lists.iter().map(|e| e.payload as u32) {
        let coarse_id = i / mmap.nlist;
        let fine_id = i % mmap.nlist;
        let mut delta = mmap.coarse_centroids(coarse_id).to_vec();
        for k in 0..mmap.dims {
            delta[k as usize] += mmap.fine_centroids(fine_id)[k as usize];
        }
        let mut j = mmap.heads[i as usize];
        while u32::MAX != j {
            let payload = mmap.raw.payload(j);
            if filter.check(payload) {
                let distance = mmap.quantization1.distance_with_delta(vector, j, &delta);
                result.push((distance, (j, delta.clone())));
                if result.len() > over_sample_size as usize {
                    result.pop();
                }
            }
            j = mmap.nexts[j as usize];
        }
    }
    let mut rerank_result = BinaryHeap::new();
    while !result.is_empty() {
        let (_, (id, delta)) = result.pop().unwrap();
        let distance = mmap.quantization2.distance_with_delta(vector, id, &delta);
        rerank_result.push(Reverse(Element {
            distance,
            payload: id as Payload,
        }));
    }
    rerank_result
}

pub fn vbase<'a, S: G>(
    mmap: &'a IvfMmap<S>,
    vector: &'a [S::Scalar],
    nprobe: u32,
    mut filter: impl Filter + 'a,
) -> (Vec<Element>, Box<(dyn Iterator<Item = Element> + 'a)>) {
    let coarse_search_count = 8; // TODO: put this parameter in a proper location
    let over_sample_size = std::cmp::min(1000, mmap.raw.len()); // TODO: put this parameter in a proper location
    let mut target = vector.to_vec();
    S::elkan_k_means_normalize(&mut target);
    let mut coarse_result = ElementHeap::new(coarse_search_count as usize);
    for i in 0..mmap.nlist {
        let distance = S::elkan_k_means_distance(&target, mmap.coarse_centroids(i));
        if coarse_result.check(distance) {
            coarse_result.push(Element {
                distance,
                payload: i as Payload,
            });
        }
    }
    let coarse_result = coarse_result.into_sorted_vec();
    let mut lists = ElementHeap::new(nprobe as usize);
    for coarse_id in coarse_result.iter().map(|e| e.payload as u32) {
        for j in 0..mmap.nlist {
            let mut centroid = mmap.coarse_centroids(coarse_id).to_vec();
            for k in 0..mmap.dims {
                centroid[k as usize] += mmap.fine_centroids(j)[k as usize];
            }
            let distance = S::elkan_k_means_distance(&target, &centroid);
            if lists.check(distance) {
                lists.push(Element {
                    distance,
                    payload: (coarse_id * mmap.nlist + j) as Payload,
                });
            }
        }
    }
    let lists = lists.into_sorted_vec();
    let mut result = BinaryHeap::new();
    for i in lists.iter().map(|e| e.payload as u32) {
        let coarse_id = i / mmap.nlist;
        let fine_id = i % mmap.nlist;
        let mut delta = mmap.coarse_centroids(coarse_id).to_vec();
        for k in 0..mmap.dims {
            delta[k as usize] += mmap.fine_centroids(fine_id)[k as usize];
        }
        let mut j = mmap.heads[i as usize];
        while u32::MAX != j {
            let payload = mmap.raw.payload(j);
            if filter.check(payload) {
                let distance = mmap.quantization1.distance_with_delta(vector, j, &delta);
                result.push((distance, (j, delta.clone())));
                if result.len() > over_sample_size as usize {
                    result.pop();
                }
            }
            j = mmap.nexts[j as usize];
        }
    }
    let mut rerank_result = Vec::new();
    while !result.is_empty() {
        let (_, (id, delta)) = result.pop().unwrap();
        let distance = mmap.quantization2.distance_with_delta(vector, id, &delta);
        rerank_result.push(Element {
            distance,
            payload: id as Payload,
        });
    }
    (rerank_result, Box::new(std::iter::empty()))
}
