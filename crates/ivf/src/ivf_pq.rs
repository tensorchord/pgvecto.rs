use super::OperatorIvf as Op;
use base::distance::*;
use base::index::*;
use base::operator::*;
use base::scalar::*;
use base::search::*;
use base::vector::*;
use common::dir_ops::sync_dir;
use common::mmap_array::MmapArray;
use common::vec2::Vec2;
use elkan_k_means::ElkanKMeans;
use num_traits::{Float, Zero};
use quantization::product::operator::OperatorProductQuantization;
use rand::seq::index::sample;
use rand::thread_rng;
use rayon::iter::IndexedParallelIterator;
use rayon::iter::IntoParallelRefMutIterator;
use rayon::iter::ParallelIterator;
use rayon::slice::ParallelSliceMut;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::create_dir;
use std::path::Path;
use std::sync::Arc;
use storage::StorageCollection;

pub struct IvfPq<O: Op> {
    mmap: IvfMmap<O>,
}

impl<O: Op> IvfPq<O> {
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

unsafe impl<O: Op> Send for IvfPq<O> {}
unsafe impl<O: Op> Sync for IvfPq<O> {}

pub struct IvfRam<O: Op> {
    storage: Arc<StorageCollection<O>>,
    quantization: ProductQuantization<O>,
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
    quantization: ProductQuantization<O>,
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
    rayon::check();
    let residuals = {
        let mut residuals = Vec2::new(options.vector.dims, n as usize);
        residuals
            .par_chunks_mut(dims as usize)
            .enumerate()
            .for_each(|(i, v)| {
                for j in 0..dims {
                    v[j as usize] = storage.vector(ids[i]).to_vec()[j as usize]
                        - centroids[idx[ids[i] as usize]][j as usize];
                }
            });
        residuals
    };
    let quantization = ProductQuantization::create(
        &path.join("quantization"),
        options.clone(),
        quantization_opts,
        &residuals,
        &centroids,
    );
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
    let quantization = ProductQuantization::open(
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
    let dense = vector.to_vec();
    let mut lists = Vec::with_capacity(mmap.nlist as usize);
    for i in 0..mmap.nlist {
        let centroid = mmap.centroids(i);
        let distance = O::product_quantization_dense_distance(&dense, centroid);
        lists.push((distance, i));
    }
    if nprobe < mmap.nlist {
        lists.select_nth_unstable(nprobe as usize);
        lists.truncate(nprobe as usize);
    }
    let runtime_table = mmap.quantization.init_query(vector.to_vec().as_ref());
    let mut result = BinaryHeap::new();
    for &(coarse_dis, key) in lists.iter() {
        let start = mmap.ptr[key as usize];
        let end = mmap.ptr[key as usize + 1];
        for j in start..end {
            let payload = mmap.payloads[j];
            if filter.check(payload) {
                let distance = mmap.quantization.distance_with_codes(
                    vector,
                    j as u32,
                    mmap.centroids(key),
                    key as usize,
                    coarse_dis,
                    &runtime_table,
                );
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
    let dense = vector.to_vec();
    let mut lists = Vec::with_capacity(mmap.nlist as usize);
    for i in 0..mmap.nlist {
        let centroid = mmap.centroids(i);
        let distance = O::product_quantization_dense_distance(&dense, centroid);
        lists.push((distance, i));
    }
    if nprobe < mmap.nlist {
        lists.select_nth_unstable(nprobe as usize);
        lists.truncate(nprobe as usize);
    }
    let runtime_table = mmap.quantization.init_query(vector.to_vec().as_ref());
    let mut result = Vec::new();
    for &(coarse_dis, key) in lists.iter() {
        let start = mmap.ptr[key as usize];
        let end = mmap.ptr[key as usize + 1];
        for j in start..end {
            let payload = mmap.payloads[j];
            if filter.check(payload) {
                let distance = mmap.quantization.distance_with_codes(
                    vector,
                    j as u32,
                    mmap.centroids(key),
                    key as usize,
                    coarse_dis,
                    &runtime_table,
                );
                result.push(Element { distance, payload });
            }
        }
    }
    (result, Box::new(std::iter::empty()))
}

pub struct ProductQuantization<O: Op> {
    dims: u32,
    ratio: u32,
    centroids: Vec<Scalar<O>>,
    codes: MmapArray<u8>,
    precomputed_table: Vec<F32>,
}

unsafe impl<O: Op> Send for ProductQuantization<O> {}
unsafe impl<O: Op> Sync for ProductQuantization<O> {}

impl<O: Op> ProductQuantization<O> {
    pub fn codes(&self, i: u32) -> &[u8] {
        let width = self.dims.div_ceil(self.ratio);
        let s = i as usize * width as usize;
        let e = (i + 1) as usize * width as usize;
        &self.codes[s..e]
    }
    pub fn open(
        path: &Path,
        options: IndexOptions,
        quantization_options: QuantizationOptions,
        _: &Arc<StorageCollection<O>>,
    ) -> Self {
        let QuantizationOptions::Product(quantization_options) = quantization_options else {
            unreachable!()
        };
        let centroids =
            serde_json::from_slice(&std::fs::read(path.join("centroids")).unwrap()).unwrap();
        let codes = MmapArray::open(&path.join("codes"));
        let precomputed_table =
            serde_json::from_slice(&std::fs::read(path.join("table")).unwrap()).unwrap();
        Self {
            dims: options.vector.dims,
            ratio: quantization_options.ratio as _,
            centroids,
            codes,
            precomputed_table,
        }
    }
    pub fn create(
        path: &Path,
        options: IndexOptions,
        quantization_options: QuantizationOptions,
        v2: &Vec2<Scalar<O>>,
        coarse_centroids: &Vec2<Scalar<O>>,
    ) -> Self {
        create_dir(path).unwrap();
        let QuantizationOptions::Product(quantization_options) = quantization_options else {
            unreachable!()
        };
        let dims = options.vector.dims;
        let ratio = quantization_options.ratio as u32;
        let n = v2.len();
        let m = std::cmp::min(n, quantization_options.sample as usize);
        let samples = {
            let f = sample(&mut thread_rng(), n, m).into_vec();
            let mut samples = Vec2::new(dims, m);
            for i in 0..m {
                samples[i].copy_from_slice(&v2[f[i]]);
            }
            samples
        };
        let width = dims.div_ceil(ratio);
        // a temp layout (width * 256 * subdims) for par_chunks_mut
        let mut tmp_centroids = vec![Scalar::<O>::zero(); 256 * dims as usize];
        // this par_for parallelizes over sub quantizers
        tmp_centroids
            .par_chunks_mut(256 * ratio as usize)
            .enumerate()
            .for_each(|(i, v)| {
                // i is the index of subquantizer
                let subdims = std::cmp::min(ratio, dims - ratio * i as u32) as usize;
                let mut subsamples = Vec2::new(subdims as u32, m);
                for j in 0..m {
                    let src = &samples[j][i * ratio as usize..][..subdims];
                    subsamples[j].copy_from_slice(src);
                }
                let mut k_means = ElkanKMeans::<O::ProductQuantizationL2>::new(256, subsamples);
                for _ in 0..25 {
                    if k_means.iterate() {
                        break;
                    }
                }
                let centroid = k_means.finish();
                for j in 0usize..=255 {
                    v[j * subdims..][..subdims].copy_from_slice(&centroid[j]);
                }
            });
        // transform back to normal layout (256 * width * subdims)
        let mut centroids = vec![Scalar::<O>::zero(); 256 * dims as usize];
        centroids
            .par_chunks_mut(dims as usize)
            .enumerate()
            .for_each(|(i, v)| {
                for j in 0..width {
                    let subdims = std::cmp::min(ratio, dims - ratio * j) as usize;
                    v[(j * ratio) as usize..][..subdims].copy_from_slice(
                        &tmp_centroids[(j * ratio) as usize * 256..][i * subdims..][..subdims],
                    );
                }
            });
        let mut codes = vec![0u8; n * width as usize];
        codes
            .par_chunks_mut(width as usize)
            .enumerate()
            .for_each(|(id, v)| {
                let vector = v2[id].to_vec();
                let width = dims.div_ceil(ratio);
                for i in 0..width {
                    let subdims = std::cmp::min(ratio, dims - ratio * i);
                    let mut minimal = F32::infinity();
                    let mut target = 0u8;
                    let left = &vector[(i * ratio) as usize..][..subdims as usize];
                    for j in 0u8..=255 {
                        let right = &centroids[j as usize * dims as usize..]
                            [(i * ratio) as usize..][..subdims as usize];
                        let dis = O::ProductQuantizationL2::product_quantization_dense_distance(
                            left, right,
                        );
                        if dis < minimal {
                            minimal = dis;
                            target = j;
                        }
                    }
                    v[i as usize] = target;
                }
            });
        sync_dir(path);
        std::fs::write(
            path.join("centroids"),
            serde_json::to_string(&centroids).unwrap(),
        )
        .unwrap();
        let codes = MmapArray::create(&path.join("codes"), codes.into_iter());
        // precompute_table
        let nlist = coarse_centroids.len();
        let width = dims.div_ceil(ratio);
        let mut precomputed_table = Vec::new();
        precomputed_table.resize(nlist * width as usize * 256, F32::zero());
        precomputed_table
            .par_chunks_mut(width as usize * 256)
            .enumerate()
            .for_each(|(i, v)| {
                let x_c = &coarse_centroids[i];
                for j in 0..width {
                    let subdims = std::cmp::min(ratio, dims - ratio * j);
                    let sub_x_c = &x_c[(j * ratio) as usize..][..subdims as usize];
                    for k in 0usize..256 {
                        let sub_x_r = &centroids[k * dims as usize..][(j * ratio) as usize..]
                            [..subdims as usize];
                        v[j as usize * 256 + k] = squared_norm::<O>(subdims, sub_x_r)
                            + F32(2.0) * inner_product::<O>(subdims, sub_x_c, sub_x_r);
                    }
                }
            });
        std::fs::write(
            path.join("table"),
            serde_json::to_string(&precomputed_table).unwrap(),
        )
        .unwrap();
        Self {
            dims,
            ratio,
            centroids,
            codes,
            precomputed_table,
        }
    }

    // compute term2 at query time
    pub fn init_query(&self, query: &[Scalar<O>]) -> Vec<F32> {
        match O::DISTANCE_KIND {
            DistanceKind::Cos => Vec::new(),
            DistanceKind::L2 | DistanceKind::Dot | DistanceKind::Jaccard => {
                let dims = self.dims;
                let ratio = self.ratio;
                let width = dims.div_ceil(ratio);
                let mut runtime_table = vec![F32::zero(); width as usize * 256];
                for i in 0..256 {
                    for j in 0..width {
                        let subdims = std::cmp::min(ratio, dims - ratio * j);
                        let sub_query = &query[(j * ratio) as usize..][..subdims as usize];
                        let centroid = &self.centroids[i * dims as usize..][(j * ratio) as usize..]
                            [..subdims as usize];
                        runtime_table[j as usize * 256 + i] =
                            F32(-1.0) * inner_product::<O>(subdims, sub_query, centroid);
                    }
                }
                runtime_table
            }
        }
    }

    // add up all terms given codes
    pub fn distance_with_codes(
        &self,
        lhs: Borrowed<'_, O>,
        rhs: u32,
        delta: &[Scalar<O>],
        key: usize,
        coarse_dis: F32,
        runtime_table: &[F32],
    ) -> F32 {
        let codes = self.codes(rhs);
        let width = self.dims.div_ceil(self.ratio);
        let precomputed_table = &self.precomputed_table[key * width as usize * 256..];
        match O::DISTANCE_KIND {
            DistanceKind::Cos => self.distance_with_delta(lhs, rhs, delta),
            DistanceKind::L2 => {
                let mut result = coarse_dis;
                for i in 0..width {
                    result += precomputed_table[i as usize * 256 + codes[i as usize] as usize]
                        + F32(2.0) * runtime_table[i as usize * 256 + codes[i as usize] as usize];
                }
                result
            }
            DistanceKind::Dot => {
                let mut result = coarse_dis;
                for i in 0..width {
                    result += runtime_table[i as usize * 256 + codes[i as usize] as usize];
                }
                result
            }
            DistanceKind::Jaccard => {
                unimplemented!()
            }
        }
    }

    pub fn distance_with_delta(&self, lhs: Borrowed<'_, O>, rhs: u32, delta: &[Scalar<O>]) -> F32 {
        let dims = self.dims;
        let ratio = self.ratio;
        let rhs = self.codes(rhs);
        O::product_quantization_distance_with_delta(dims, ratio, &self.centroids, lhs, rhs, delta)
    }
}

pub fn squared_norm<O: Op>(dims: u32, vec: &[Scalar<O>]) -> F32 {
    let mut result = F32::zero();
    for i in 0..dims as usize {
        result += F32((vec[i] * vec[i]).to_f32());
    }
    result
}

pub fn inner_product<O: Op>(dims: u32, lhs: &[Scalar<O>], rhs: &[Scalar<O>]) -> F32 {
    let mut result = F32::zero();
    for i in 0..dims as usize {
        result += F32((lhs[i] * rhs[i]).to_f32());
    }
    result
}
