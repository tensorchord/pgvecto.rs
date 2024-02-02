use crate::algorithms::clustering::elkan_k_means::ElkanKMeans;
use crate::algorithms::quantization::product::ProductQuantization;
use crate::algorithms::quantization::Quan;
use crate::algorithms::raw::Raw;
use crate::index::indexing::ivf::IvfIndexingOptions;
use crate::index::segments::growing::GrowingSegment;
use crate::index::segments::sealed::SealedSegment;
use crate::index::IndexOptions;
use crate::index::SearchOptions;
use crate::index::VectorOptions;
use crate::prelude::*;
use crate::utils::dir_ops::sync_dir;
use crate::utils::element_heap::ElementHeap;
use crate::utils::mmap_array::MmapArray;
use crate::utils::vec2::Vec2;
use rand::seq::index::sample;
use rand::thread_rng;
use rayon::iter::IntoParallelRefMutIterator;
use rayon::iter::{IndexedParallelIterator, ParallelIterator};
use rayon::slice::ParallelSliceMut;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::create_dir;
use std::path::PathBuf;
use std::sync::Arc;

pub struct IvfPq<S: G> {
    mmap: IvfMmap<S>,
}

impl<S: G> IvfPq<S> {
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

unsafe impl<S: G> Send for IvfPq<S> {}
unsafe impl<S: G> Sync for IvfPq<S> {}

pub struct IvfRam<S: G> {
    raw: Arc<Raw<S>>,
    quantization: ProductQuantization<S>,
    // ----------------------
    dims: u16,
    // ----------------------
    nlist: u32,
    // ----------------------
    centroids: Vec2<S>,
}

unsafe impl<S: G> Send for IvfRam<S> {}
unsafe impl<S: G> Sync for IvfRam<S> {}

pub struct IvfMmap<S: G> {
    raw: Arc<Raw<S>>,
    quantization: ProductQuantization<S>,
    // ----------------------
    dims: u16,
    // ----------------------
    nlist: u32,
    // ----------------------
    centroids: MmapArray<S::Scalar>,
    ptr: MmapArray<usize>,
    payloads: MmapArray<Payload>,
}

unsafe impl<S: G> Send for IvfMmap<S> {}
unsafe impl<S: G> Sync for IvfMmap<S> {}

impl<S: G> IvfMmap<S> {
    fn centroids(&self, i: u32) -> &[S::Scalar] {
        let s = i as usize * self.dims as usize;
        let e = (i + 1) as usize * self.dims as usize;
        &self.centroids[s..e]
    }
}

pub fn make<S: G>(
    path: PathBuf,
    sealed: Vec<Arc<SealedSegment<S>>>,
    growing: Vec<Arc<GrowingSegment<S>>>,
    options: IndexOptions,
) -> IvfRam<S> {
    let VectorOptions { dims, .. } = options.vector;
    let IvfIndexingOptions {
        least_iterations,
        iterations,
        nlist,
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
        S::elkan_k_means_normalize(&mut samples[i as usize]);
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
    let centroids = k_means.finish();
    let mut idx = vec![0usize; n as usize];
    idx.par_iter_mut().enumerate().for_each(|(i, x)| {
        let mut vector = raw.vector(i as u32).to_vec();
        S::elkan_k_means_normalize(&mut vector);
        let mut result = (F32::infinity(), 0);
        for i in 0..nlist as usize {
            let dis = S::elkan_k_means_distance(&vector, &centroids[i]);
            result = std::cmp::min(result, (dis, i));
        }
        *x = result.1;
    });
    let mut invlists_ids = vec![Vec::new(); nlist as usize];
    let mut invlists_payloads = vec![Vec::new(); nlist as usize];
    for i in 0..n {
        invlists_ids[idx[i as usize]].push(i);
        invlists_payloads[idx[i as usize]].push(raw.payload(i));
    }
    let mut ptr = vec![0usize; nlist as usize + 1];
    for i in 0..nlist {
        ptr[i as usize + 1] = ptr[i as usize] + invlists_ids[i as usize].len();
    }
    let ids = Vec::from_iter((0..nlist).flat_map(|i| &invlists_ids[i as usize]).copied());
    let payloads = Vec::from_iter(
        (0..nlist)
            .flat_map(|i| &invlists_payloads[i as usize])
            .copied(),
    );
    MmapArray::create(path.join("ptr"), ptr.iter().copied());
    MmapArray::create(path.join("payload"), payloads.iter().copied());
    sync_dir(&path);
    let residuals = {
        let mut residuals = Vec2::<S>::new(options.vector.dims, n as usize);
        residuals
            .par_chunks_mut(dims as usize)
            .enumerate()
            .for_each(|(i, v)| {
                for j in 0..dims {
                    v[j as usize] = raw.vector(ids[i])[j as usize]
                        - centroids[idx[ids[i] as usize]][j as usize];
                }
            });
        residuals
    };
    let quantization = ProductQuantization::encode(
        path.join("quantization"),
        options.clone(),
        quantization_opts,
        &residuals,
    );
    IvfRam {
        raw,
        quantization,
        centroids,
        nlist,
        dims,
    }
}

pub fn save<S: G>(ram: IvfRam<S>, path: PathBuf) -> IvfMmap<S> {
    let centroids = MmapArray::create(
        path.join("centroids"),
        (0..ram.nlist)
            .flat_map(|i| &ram.centroids[i as usize])
            .copied(),
    );
    let ptr = MmapArray::open(path.join("ptr"));
    let payloads = MmapArray::open(path.join("payload"));
    IvfMmap {
        raw: ram.raw,
        quantization: ram.quantization,
        dims: ram.dims,
        nlist: ram.nlist,
        centroids,
        ptr,
        payloads,
    }
}

pub fn load<S: G>(path: PathBuf, options: IndexOptions) -> IvfMmap<S> {
    let raw = Arc::new(Raw::open(path.join("raw"), options.clone()));
    let quantization = ProductQuantization::open(
        path.join("quantization"),
        options.clone(),
        options.indexing.clone().unwrap_ivf().quantization,
        &raw,
    );
    let centroids = MmapArray::open(path.join("centroids"));
    let ptr = MmapArray::open(path.join("ptr"));
    let payloads = MmapArray::open(path.join("payload"));
    let IvfIndexingOptions { nlist, .. } = options.indexing.unwrap_ivf();
    IvfMmap {
        raw,
        quantization,
        dims: options.vector.dims,
        nlist,
        centroids,
        ptr,
        payloads,
    }
}

pub fn basic<S: G>(
    mmap: &IvfMmap<S>,
    vector: &[S::Scalar],
    nprobe: u32,
    mut filter: impl Filter,
) -> BinaryHeap<Reverse<Element>> {
    let mut target = vector.to_vec();
    S::elkan_k_means_normalize(&mut target);
    let mut lists = ElementHeap::new(nprobe as usize);
    for i in 0..mmap.nlist {
        let centroid = mmap.centroids(i);
        let distance = S::elkan_k_means_distance(&target, centroid);
        if lists.check(distance) {
            lists.push(Element {
                distance,
                payload: i as Payload,
            });
        }
    }
    let lists = lists.into_sorted_vec();
    let mut result = BinaryHeap::new();
    for i in lists.iter().map(|e| e.payload as usize) {
        let start = mmap.ptr[i];
        let end = mmap.ptr[i + 1];
        for j in start..end {
            let payload = mmap.payloads[j];
            if filter.check(payload) {
                let distance = mmap.quantization.distance_with_delta(
                    vector,
                    j as u32,
                    mmap.centroids(i as u32),
                );
                result.push(Reverse(Element { distance, payload }));
            }
        }
    }
    result
}

pub fn vbase<'a, S: G>(
    mmap: &'a IvfMmap<S>,
    vector: &'a [S::Scalar],
    nprobe: u32,
    mut filter: impl Filter + 'a,
) -> (Vec<Element>, Box<(dyn Iterator<Item = Element> + 'a)>) {
    let mut target = vector.to_vec();
    S::elkan_k_means_normalize(&mut target);
    let mut lists = ElementHeap::new(nprobe as usize);
    for i in 0..mmap.nlist {
        let centroid = mmap.centroids(i);
        let distance = S::elkan_k_means_distance(&target, centroid);
        if lists.check(distance) {
            lists.push(Element {
                distance,
                payload: i as Payload,
            });
        }
    }
    let lists = lists.into_sorted_vec();
    let mut result = Vec::new();
    for i in lists.iter().map(|e| e.payload as usize) {
        let start = mmap.ptr[i];
        let end = mmap.ptr[i + 1];
        for j in start..end {
            let payload = mmap.payloads[j];
            if filter.check(payload) {
                let distance = mmap.quantization.distance_with_delta(
                    vector,
                    j as u32,
                    mmap.centroids(i as u32),
                );
                result.push(Element { distance, payload });
            }
        }
    }
    (result, Box::new(std::iter::empty()))
}
