use crate::algorithms::clustering::elkan_k_means::ElkanKMeans;
use crate::algorithms::quantization::Quantization;
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
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::create_dir;
use std::path::Path;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
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

    pub fn load(path: &Path, options: IndexOptions) -> Self {
        let mmap = load(path, options);
        Self { mmap }
    }

    pub fn dims(&self) -> u16 {
        self.mmap.raw.dims()
    }

    pub fn len(&self) -> u32 {
        self.mmap.raw.len()
    }

    pub fn content(&self, i: u32) -> &[S::Element] {
        self.mmap.raw.content(i)
    }

    pub fn payload(&self, i: u32) -> Payload {
        self.mmap.raw.payload(i)
    }

    pub fn basic(
        &self,
        vector: &[S::Element],
        opts: &SearchOptions,
        filter: impl Filter,
    ) -> BinaryHeap<Reverse<Element>> {
        basic(&self.mmap, vector, opts.ivf_nprobe, filter)
    }

    pub fn vbase<'a>(
        &'a self,
        vector: &'a [S::Element],
        opts: &'a SearchOptions,
        filter: impl Filter + 'a,
    ) -> (Vec<Element>, Box<(dyn Iterator<Item = Element> + 'a)>) {
        vbase(&self.mmap, vector, opts.ivf_nprobe, filter)
    }
}

unsafe impl<S: G> Send for IvfNaive<S> {}
unsafe impl<S: G> Sync for IvfNaive<S> {}

pub struct IvfRam<S: G> {
    raw: Arc<Raw<S::Storage>>,
    quantization: Quantization<S>,
    // ----------------------
    dims: u16,
    // ----------------------
    nlist: u32,
    // ----------------------
    centroids: Vec2<S::Scalar>,
    heads: Vec<AtomicU32>,
    nexts: Vec<SyncUnsafeCell<u32>>,
}

unsafe impl<S: G> Send for IvfRam<S> {}
unsafe impl<S: G> Sync for IvfRam<S> {}

pub struct IvfMmap<S: G> {
    raw: Arc<Raw<S::Storage>>,
    quantization: Quantization<S>,
    // ----------------------
    dims: u16,
    // ----------------------
    nlist: u32,
    // ----------------------
    centroids: MmapArray<S::Scalar>,
    heads: MmapArray<u32>,
    nexts: MmapArray<u32>,
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
    let raw = Arc::new(Raw::create(
        &path.join("raw"),
        options.clone(),
        sealed,
        growing,
    ));
    let quantization = Quantization::open(
        &path.join("quantization"),
        options.clone(),
        quantization_opts,
        &raw,
    );
    let n = raw.len();
    let m = std::cmp::min(nsample, n);
    let f = sample(&mut thread_rng(), n as usize, m as usize).into_vec();
    let mut samples = Vec2::new(dims, m as usize);
    for i in 0..m {
        samples[i as usize]
            .copy_from_slice(S::Storage::vector(dims, raw.content(f[i as usize] as u32)).as_ref());
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
    (0..n).into_par_iter().for_each(|i| {
        let mut vector = S::Storage::vector(dims, raw.content(i)).to_vec();
        S::elkan_k_means_normalize(&mut vector);
        let mut result = (F32::infinity(), 0);
        for i in 0..nlist {
            let dis = S::elkan_k_means_distance(&vector, &centroids[i as usize]);
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
    });
    IvfRam {
        raw,
        quantization,
        centroids,
        heads,
        nexts,
        nlist,
        dims,
    }
}

pub fn save<S: G>(mut ram: IvfRam<S>, path: &Path) -> IvfMmap<S> {
    let centroids = MmapArray::create(
        &path.join("centroids"),
        (0..ram.nlist)
            .flat_map(|i| &ram.centroids[i as usize])
            .copied(),
    );
    let heads = MmapArray::create(
        &path.join("heads"),
        ram.heads.iter_mut().map(|x| *x.get_mut()),
    );
    let nexts = MmapArray::create(
        &path.join("nexts"),
        ram.nexts.iter_mut().map(|x| *x.get_mut()),
    );
    IvfMmap {
        raw: ram.raw,
        quantization: ram.quantization,
        dims: ram.dims,
        nlist: ram.nlist,
        centroids,
        heads,
        nexts,
    }
}

pub fn load<S: G>(path: &Path, options: IndexOptions) -> IvfMmap<S> {
    let raw = Arc::new(Raw::load(&path.join("raw"), options.clone()));
    let quantization = Quantization::open(
        &path.join("quantization"),
        options.clone(),
        options.indexing.clone().unwrap_ivf().quantization,
        &raw,
    );
    let centroids = MmapArray::open(&path.join("centroids"));
    let heads = MmapArray::open(&path.join("heads"));
    let nexts = MmapArray::open(&path.join("nexts"));
    let IvfIndexingOptions { nlist, .. } = options.indexing.unwrap_ivf();
    IvfMmap {
        raw,
        quantization,
        dims: options.vector.dims,
        nlist,
        centroids,
        heads,
        nexts,
    }
}

pub fn basic<S: G>(
    mmap: &IvfMmap<S>,
    vector: &[S::Element],
    nprobe: u32,
    mut filter: impl Filter,
) -> BinaryHeap<Reverse<Element>> {
    let dims = mmap.raw.dims();
    let mut target = S::Storage::vector(dims, vector).to_vec();
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
        let mut j = mmap.heads[i];
        while u32::MAX != j {
            let payload = mmap.raw.payload(j);
            if filter.check(payload) {
                let distance = mmap.quantization.distance(vector, j);
                result.push(Reverse(Element { distance, payload }));
            }
            j = mmap.nexts[j as usize];
        }
    }
    result
}

pub fn vbase<'a, S: G>(
    mmap: &'a IvfMmap<S>,
    vector: &'a [S::Element],
    nprobe: u32,
    mut filter: impl Filter + 'a,
) -> (Vec<Element>, Box<(dyn Iterator<Item = Element> + 'a)>) {
    let dims = mmap.raw.dims();
    let mut target = S::Storage::vector(dims, vector).to_vec();
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
    for i in lists.iter().map(|e| e.payload as u32) {
        let mut j = mmap.heads[i as usize];
        while u32::MAX != j {
            let payload = mmap.raw.payload(j);
            if filter.check(payload) {
                let distance = mmap.quantization.distance(vector, j);
                result.push(Element { distance, payload });
            }
            j = mmap.nexts[j as usize];
        }
    }
    (result, Box::new(std::iter::empty()))
}
