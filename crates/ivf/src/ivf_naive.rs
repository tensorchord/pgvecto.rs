use super::OperatorIvf as Op;
use base::index::*;
use base::operator::*;
use base::scalar::F32;
use base::search::*;
use base::vector::*;
use common::dir_ops::sync_dir;
use common::mmap_array::MmapArray;
use common::vec2::Vec2;
use elkan_k_means::ElkanKMeans;
use num_traits::Float;
use quantization::Quantization;
use rand::seq::index::sample;
use rand::thread_rng;
use rayon::iter::{IndexedParallelIterator, IntoParallelRefMutIterator, ParallelIterator};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::create_dir;
use std::path::Path;
use std::sync::Arc;
use storage::StorageCollection;

pub struct IvfNaive<O: Op> {
    mmap: IvfMmap<O>,
}

impl<O: Op> IvfNaive<O> {
    pub fn create<S: Source<O>>(path: &Path, options: IndexOptions, source: &S) -> Self {
        create_dir(path).unwrap();
        let ram = make(path, options, source);
        let mmap = save(ram, path);
        sync_dir(path);
        Self { mmap }
    }

    pub fn open(path: &Path, options: IndexOptions) -> Self {
        let mmap = open(path, options);
        Self { mmap }
    }

    pub fn len(&self) -> u32 {
        self.mmap.storage.len()
    }

    pub fn vector(&self, i: u32) -> Borrowed<'_, O> {
        self.mmap.storage.vector(i)
    }

    pub fn payload(&self, i: u32) -> Payload {
        self.mmap.storage.payload(i)
    }

    pub fn basic(
        &self,
        vector: Borrowed<'_, O>,
        opts: &SearchOptions,
        filter: impl Filter,
    ) -> BinaryHeap<Reverse<Element>> {
        basic(&self.mmap, vector, opts.ivf_nprobe, filter)
    }

    pub fn vbase<'a>(
        &'a self,
        vector: Borrowed<'a, O>,
        opts: &'a SearchOptions,
        filter: impl Filter + 'a,
    ) -> (Vec<Element>, Box<(dyn Iterator<Item = Element> + 'a)>) {
        vbase(&self.mmap, vector, opts.ivf_nprobe, filter)
    }
}

unsafe impl<O: Op> Send for IvfNaive<O> {}
unsafe impl<O: Op> Sync for IvfNaive<O> {}

pub struct IvfRam<O: Op> {
    storage: Arc<StorageCollection<O>>,
    quantization: Quantization<O, StorageCollection<O>>,
    // ----------------------
    dims: u32,
    // ----------------------
    nlist: u32,
    // ----------------------
    centroids: Vec2<Scalar<O>>,
    ptr: Vec<usize>,
    payloads: Vec<Payload>,
}

unsafe impl<O: Op> Send for IvfRam<O> {}
unsafe impl<O: Op> Sync for IvfRam<O> {}

pub struct IvfMmap<O: Op> {
    storage: Arc<StorageCollection<O>>,
    quantization: Quantization<O, StorageCollection<O>>,
    // ----------------------
    dims: u32,
    // ----------------------
    nlist: u32,
    // ----------------------
    centroids: MmapArray<Scalar<O>>,
    ptr: MmapArray<usize>,
    payloads: MmapArray<Payload>,
}

unsafe impl<O: Op> Send for IvfMmap<O> {}
unsafe impl<O: Op> Sync for IvfMmap<O> {}

impl<O: Op> IvfMmap<O> {
    fn centroids(&self, i: u32) -> &[Scalar<O>] {
        let s = i as usize * self.dims as usize;
        let e = (i + 1) as usize * self.dims as usize;
        &self.centroids[s..e]
    }
}

pub fn make<O: Op, S: Source<O>>(path: &Path, options: IndexOptions, source: &S) -> IvfRam<O> {
    let VectorOptions { dims, .. } = options.vector;
    let IvfIndexingOptions {
        least_iterations,
        iterations,
        nlist,
        nsample,
        quantization: quantization_opts,
    } = options.indexing.clone().unwrap_ivf();
    let storage = Arc::new(StorageCollection::<O>::create(&path.join("raw"), source));
    let n = storage.len();
    let m = std::cmp::min(nsample, n);
    let f = sample(&mut thread_rng(), n as usize, m as usize).into_vec();
    let mut samples = Vec2::new(dims, m as usize);
    for i in 0..m {
        samples[i as usize].copy_from_slice(storage.vector(f[i as usize] as u32).to_vec().as_ref());
        O::elkan_k_means_normalize(&mut samples[i as usize]);
    }
    rayon::check();
    let mut k_means = ElkanKMeans::<O>::new(nlist as usize, samples);
    for _ in 0..least_iterations {
        rayon::check();
        k_means.iterate();
    }
    for _ in least_iterations..iterations {
        rayon::check();
        if k_means.iterate() {
            break;
        }
    }
    let centroids = k_means.finish();
    let mut idx = vec![0usize; n as usize];
    idx.par_iter_mut().enumerate().for_each(|(i, x)| {
        rayon::check();
        let vector = storage.vector(i as u32);
        let vector = O::elkan_k_means_normalize2(vector);
        let mut result = (F32::infinity(), 0);
        for i in 0..nlist as usize {
            let dis = O::elkan_k_means_distance2(vector.for_borrow(), &centroids[i]);
            result = std::cmp::min(result, (dis, i));
        }
        *x = result.1;
    });
    let mut invlists_ids = vec![Vec::new(); nlist as usize];
    let mut invlists_payloads = vec![Vec::new(); nlist as usize];
    for i in 0..n {
        invlists_ids[idx[i as usize]].push(i);
        invlists_payloads[idx[i as usize]].push(storage.payload(i));
    }
    rayon::check();
    let permutation = Vec::from_iter((0..nlist).flat_map(|i| &invlists_ids[i as usize]).copied());
    rayon::check();
    let payloads = Vec::from_iter(
        (0..nlist)
            .flat_map(|i| &invlists_payloads[i as usize])
            .copied(),
    );
    rayon::check();
    let quantization = Quantization::create(
        &path.join("quantization"),
        options.clone(),
        quantization_opts,
        &storage,
        permutation,
    );
    rayon::check();
    let mut ptr = vec![0usize; nlist as usize + 1];
    for i in 0..nlist {
        ptr[i as usize + 1] = ptr[i as usize] + invlists_ids[i as usize].len();
    }
    IvfRam {
        storage,
        quantization,
        centroids,
        nlist,
        dims,
        ptr,
        payloads,
    }
}

pub fn save<O: Op>(ram: IvfRam<O>, path: &Path) -> IvfMmap<O> {
    let centroids = MmapArray::create(
        &path.join("centroids"),
        (0..ram.nlist)
            .flat_map(|i| &ram.centroids[i as usize])
            .copied(),
    );
    let ptr = MmapArray::create(&path.join("ptr"), ram.ptr.iter().copied());
    let payloads = MmapArray::create(&path.join("payload"), ram.payloads.iter().copied());
    IvfMmap {
        storage: ram.storage,
        quantization: ram.quantization,
        dims: ram.dims,
        nlist: ram.nlist,
        centroids,
        ptr,
        payloads,
    }
}

pub fn open<O: Op>(path: &Path, options: IndexOptions) -> IvfMmap<O> {
    let storage = Arc::new(StorageCollection::open(&path.join("raw"), options.clone()));
    let quantization = Quantization::open(
        &path.join("quantization"),
        options.clone(),
        options.indexing.clone().unwrap_ivf().quantization,
        &storage,
    );
    let centroids = MmapArray::open(&path.join("centroids"));
    let ptr = MmapArray::open(&path.join("ptr"));
    let payloads = MmapArray::open(&path.join("payload"));
    let IvfIndexingOptions { nlist, .. } = options.indexing.unwrap_ivf();
    IvfMmap {
        storage,
        quantization,
        dims: options.vector.dims,
        nlist,
        centroids,
        ptr,
        payloads,
    }
}

pub fn basic<O: Op>(
    mmap: &IvfMmap<O>,
    vector: Borrowed<'_, O>,
    nprobe: u32,
    mut filter: impl Filter,
) -> BinaryHeap<Reverse<Element>> {
    let target = O::elkan_k_means_normalize2(vector);
    let mut lists = Vec::with_capacity(mmap.nlist as usize);
    for i in 0..mmap.nlist {
        let centroid = mmap.centroids(i);
        let distance = O::elkan_k_means_distance2(target.for_borrow(), centroid);
        lists.push((distance, i));
    }
    if nprobe < mmap.nlist {
        lists.select_nth_unstable(nprobe as usize);
        lists.truncate(nprobe as usize);
    }
    let mut result = BinaryHeap::new();
    for i in lists.iter().map(|(_, i)| *i as usize) {
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

pub fn vbase<'a, O: Op>(
    mmap: &'a IvfMmap<O>,
    vector: Borrowed<'a, O>,
    nprobe: u32,
    mut filter: impl Filter + 'a,
) -> (Vec<Element>, Box<(dyn Iterator<Item = Element> + 'a)>) {
    let target = O::elkan_k_means_normalize2(vector);
    let mut lists = Vec::with_capacity(mmap.nlist as usize);
    for i in 0..mmap.nlist {
        let centroid = mmap.centroids(i);
        let distance = O::elkan_k_means_distance2(target.for_borrow(), centroid);
        lists.push((distance, i));
    }
    if nprobe < mmap.nlist {
        lists.select_nth_unstable(nprobe as usize);
        lists.truncate(nprobe as usize);
    }
    let mut result = Vec::new();
    for i in lists.iter().map(|(_, i)| *i as usize) {
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
