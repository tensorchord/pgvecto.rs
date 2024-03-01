use crate::algorithms::clustering::elkan_k_means::ElkanKMeans;
use crate::algorithms::quantization::Quantization;
use crate::algorithms::raw::Raw;
use crate::index::segments::growing::GrowingSegment;
use crate::index::segments::sealed::SealedSegment;
use crate::prelude::*;
use crate::utils::dir_ops::sync_dir;
use crate::utils::element_heap::ElementHeap;
use crate::utils::mmap_array::MmapArray;
use crate::utils::vec2::Vec2;
use rand::seq::index::sample;
use rand::thread_rng;
use rayon::iter::{IndexedParallelIterator, IntoParallelRefMutIterator};
use rayon::prelude::ParallelIterator;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::create_dir;
use std::path::Path;
use std::sync::Arc;

pub struct IvfNaive<S: G> {
    mmap: IvfMmap<S>,
}

impl<S: G> IvfNaive<S> {
    pub fn create(
        path: &Path,
        options: IndexOptions,
        sealed: Vec<Arc<SealedSegment<S>>>,
        growing: Vec<Arc<GrowingSegment<S>>>,
    ) -> Self {
        create_dir(path).unwrap();
        let ram = make(path, sealed, growing, options);
        let mmap = save(ram, path);
        sync_dir(path);
        Self { mmap }
    }

    pub fn open(path: &Path, options: IndexOptions) -> Self {
        let mmap = open(path, options);
        Self { mmap }
    }

    pub fn len(&self) -> u32 {
        self.mmap.raw.len()
    }

    pub fn vector(&self, i: u32) -> Borrowed<'_, S> {
        self.mmap.raw.vector(i)
    }

    pub fn payload(&self, i: u32) -> Payload {
        self.mmap.raw.payload(i)
    }

    pub fn basic(
        &self,
        vector: Borrowed<'_, S>,
        opts: &SearchOptions,
        filter: impl Filter,
    ) -> BinaryHeap<Reverse<Element>> {
        basic(&self.mmap, vector, opts.ivf_nprobe, filter)
    }

    pub fn vbase<'a>(
        &'a self,
        vector: Borrowed<'a, S>,
        opts: &'a SearchOptions,
        filter: impl Filter + 'a,
    ) -> (Vec<Element>, Box<(dyn Iterator<Item = Element> + 'a)>) {
        vbase(&self.mmap, vector, opts.ivf_nprobe, filter)
    }
}

unsafe impl<S: G> Send for IvfNaive<S> {}
unsafe impl<S: G> Sync for IvfNaive<S> {}

pub struct IvfRam<S: G> {
    raw: Arc<Raw<S>>,
    quantization: Quantization<S>,
    // ----------------------
    dims: u32,
    // ----------------------
    nlist: u32,
    // ----------------------
    centroids: Vec2<Scalar<S>>,
    ptr: Vec<usize>,
    payloads: Vec<Payload>,
}

unsafe impl<S: G> Send for IvfRam<S> {}
unsafe impl<S: G> Sync for IvfRam<S> {}

pub struct IvfMmap<S: G> {
    raw: Arc<Raw<S>>,
    quantization: Quantization<S>,
    // ----------------------
    dims: u32,
    // ----------------------
    nlist: u32,
    // ----------------------
    centroids: MmapArray<Scalar<S>>,
    ptr: MmapArray<usize>,
    payloads: MmapArray<Payload>,
}

unsafe impl<S: G> Send for IvfMmap<S> {}
unsafe impl<S: G> Sync for IvfMmap<S> {}

impl<S: G> IvfMmap<S> {
    fn centroids(&self, i: u32) -> &[Scalar<S>] {
        let s = i as usize * self.dims as usize;
        let e = (i + 1) as usize * self.dims as usize;
        &self.centroids[s..e]
    }
}

pub fn make<S: G>(
    path: &Path,
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
    let raw = Arc::new(Raw::<S>::create(
        &path.join("raw"),
        options.clone(),
        sealed,
        growing,
    ));
    let n = raw.len();
    let m = std::cmp::min(nsample, n);
    let f = sample(&mut thread_rng(), n as usize, m as usize).into_vec();
    let mut samples = Vec2::new(dims, m as usize);
    for i in 0..m {
        samples[i as usize].copy_from_slice(raw.vector(f[i as usize] as u32).to_vec().as_ref());
        S::elkan_k_means_normalize(&mut samples[i as usize]);
    }
    let mut k_means = ElkanKMeans::<S>::new(nlist as usize, samples);
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
        let vector = raw.vector(i as u32);
        let vector = S::elkan_k_means_normalize2(vector);
        let mut result = (F32::infinity(), 0);
        for i in 0..nlist as usize {
            let dis = S::elkan_k_means_distance2(vector.for_borrow(), &centroids[i]);
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
    let permutation = Vec::from_iter((0..nlist).flat_map(|i| &invlists_ids[i as usize]).copied());
    let payloads = Vec::from_iter(
        (0..nlist)
            .flat_map(|i| &invlists_payloads[i as usize])
            .copied(),
    );
    let quantization = Quantization::create(
        &path.join("quantization"),
        options.clone(),
        quantization_opts,
        &raw,
        permutation,
    );
    let mut ptr = vec![0usize; nlist as usize + 1];
    for i in 0..nlist {
        ptr[i as usize + 1] = ptr[i as usize] + invlists_ids[i as usize].len();
    }
    IvfRam {
        raw,
        quantization,
        centroids,
        nlist,
        dims,
        ptr,
        payloads,
    }
}

pub fn save<S: G>(ram: IvfRam<S>, path: &Path) -> IvfMmap<S> {
    let centroids = MmapArray::create(
        &path.join("centroids"),
        (0..ram.nlist)
            .flat_map(|i| &ram.centroids[i as usize])
            .copied(),
    );
    let ptr = MmapArray::create(&path.join("ptr"), ram.ptr.iter().copied());
    let payloads = MmapArray::create(&path.join("payload"), ram.payloads.iter().copied());
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

pub fn open<S: G>(path: &Path, options: IndexOptions) -> IvfMmap<S> {
    let raw = Arc::new(Raw::open(&path.join("raw"), options.clone()));
    let quantization = Quantization::open(
        &path.join("quantization"),
        options.clone(),
        options.indexing.clone().unwrap_ivf().quantization,
        &raw,
    );
    let centroids = MmapArray::open(&path.join("centroids"));
    let ptr = MmapArray::open(&path.join("ptr"));
    let payloads = MmapArray::open(&path.join("payload"));
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
    vector: Borrowed<'_, S>,
    nprobe: u32,
    mut filter: impl Filter,
) -> BinaryHeap<Reverse<Element>> {
    let target = S::elkan_k_means_normalize2(vector);
    let mut lists = ElementHeap::new(nprobe as usize);
    for i in 0..mmap.nlist {
        let centroid = mmap.centroids(i);
        let distance = S::elkan_k_means_distance2(target.for_borrow(), centroid);
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
                let distance = mmap.quantization.distance(vector, j as u32);
                result.push(Reverse(Element { distance, payload }));
            }
        }
    }
    result
}

pub fn vbase<'a, S: G>(
    mmap: &'a IvfMmap<S>,
    vector: Borrowed<'a, S>,
    nprobe: u32,
    mut filter: impl Filter + 'a,
) -> (Vec<Element>, Box<(dyn Iterator<Item = Element> + 'a)>) {
    let target = S::elkan_k_means_normalize2(vector);
    let mut lists = ElementHeap::new(nprobe as usize);
    for i in 0..mmap.nlist {
        let centroid = mmap.centroids(i);
        let distance = S::elkan_k_means_distance2(target.for_borrow(), centroid);
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
                let distance = mmap.quantization.distance(vector, j as u32);
                result.push(Element { distance, payload });
            }
        }
    }
    (result, Box::new(std::iter::empty()))
}
