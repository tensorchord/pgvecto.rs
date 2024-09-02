#![allow(clippy::len_without_is_empty)]
#![allow(clippy::needless_range_loop)]

pub mod operator;

use base::always_equal::AlwaysEqual;
use base::index::*;
use base::operator::*;
use base::search::*;
use base::vector::VectorBorrowed;
use base::vector::VectorOwned;
use common::json::Json;
use common::mmap_array::MmapArray;
use common::remap::RemappedCollection;
use common::vec2::Vec2;
use k_means::k_means;
use k_means::k_means_lookup;
use k_means::k_means_lookup_many;
use operator::OperatorIvf as Op;
use quantization::Quantization;
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use std::fs::create_dir;
use std::path::Path;
use stoppable_rayon as rayon;
use storage::Storage;

pub struct Ivf<O: Op> {
    storage: O::Storage,
    quantization: Quantization<O>,
    payloads: MmapArray<Payload>,
    offsets: Json<Vec<u32>>,
    centroids: Json<Vec2<<O as Op>::Scalar>>,
    is_residual: Json<bool>,
}

impl<O: Op> Ivf<O> {
    pub fn create(
        path: impl AsRef<Path>,
        options: IndexOptions,
        source: &(impl Vectors<Owned<O>> + Collection + Source + Sync),
    ) -> Self {
        let remapped = RemappedCollection::from_source(source);
        from_nothing(path, options, &remapped)
    }

    pub fn open(path: impl AsRef<Path>) -> Self {
        open(path)
    }

    pub fn dims(&self) -> u32 {
        self.storage.dims()
    }

    pub fn len(&self) -> u32 {
        self.storage.len()
    }

    pub fn vector(&self, i: u32) -> Borrowed<'_, O> {
        self.storage.vector(i)
    }

    pub fn payload(&self, i: u32) -> Payload {
        self.payloads[i as usize]
    }

    pub fn vbase<'a>(
        &'a self,
        vector: Borrowed<'a, O>,
        opts: &'a SearchOptions,
    ) -> Box<dyn Iterator<Item = Element> + 'a> {
        let lists = select(
            k_means_lookup_many(O::interpret(vector), &self.centroids),
            opts.ivf_nprobe as usize,
        );
        let mut heap = Vec::new();
        let mut preprocessed = self.quantization.preprocess(vector);
        for i in lists.iter().map(|(_, i)| *i) {
            if *self.is_residual {
                let vector = O::residual(vector, &self.centroids[(i,)]);
                preprocessed = self.quantization.preprocess(vector.as_borrowed());
            }
            let start = self.offsets[i];
            let end = self.offsets[i + 1];
            self.quantization.push_batch(
                &preprocessed,
                start..end,
                &mut heap,
                opts.ivf_sq_fast_scan,
                opts.ivf_pq_fast_scan,
            );
        }
        let mut reranker = self.quantization.flat_rerank(
            heap,
            move |u| (O::distance(vector, self.storage.vector(u)), ()),
            opts.ivf_sq_rerank_size,
            opts.ivf_pq_rerank_size,
        );
        Box::new(std::iter::from_fn(move || {
            reranker.pop().map(|(dis_u, u, ())| Element {
                distance: dis_u,
                payload: AlwaysEqual(self.payload(u)),
            })
        }))
    }
}

fn from_nothing<O: Op>(
    path: impl AsRef<Path>,
    options: IndexOptions,
    collection: &(impl Vectors<Owned<O>> + Collection + Sync),
) -> Ivf<O> {
    create_dir(path.as_ref()).unwrap();
    let IvfIndexingOptions {
        nlist,
        spherical_centroids,
        residual_quantization,
        quantization: quantization_options,
    } = options.indexing.clone().unwrap_ivf();
    let samples = O::sample(collection, nlist);
    rayon::check();
    let centroids = k_means(nlist as usize, samples, true, spherical_centroids, false);
    rayon::check();
    let ls = (0..collection.len())
        .into_par_iter()
        .fold(
            || vec![Vec::new(); nlist as usize],
            |mut state, i| {
                state[k_means_lookup(O::interpret(collection.vector(i)), &centroids)].push(i);
                state
            },
        )
        .reduce(
            || vec![Vec::new(); nlist as usize],
            |lhs, rhs| {
                std::iter::zip(lhs, rhs)
                    .map(|(lhs, rhs)| {
                        let mut x = lhs;
                        x.extend(rhs);
                        x
                    })
                    .collect()
            },
        );
    let mut offsets = vec![0u32; nlist as usize + 1];
    for i in 0..nlist {
        offsets[i as usize + 1] = offsets[i as usize] + ls[i as usize].len() as u32;
    }
    let remap = ls
        .into_iter()
        .flat_map(|x| x.into_iter())
        .collect::<Vec<_>>();
    let collection = RemappedCollection::from_collection(collection, remap);
    let is_residual = residual_quantization && O::SUPPORT_RESIDUAL;
    rayon::check();
    let storage = O::Storage::create(path.as_ref().join("storage"), &collection);
    let quantization = Quantization::<O>::create(
        path.as_ref().join("quantization"),
        options.vector,
        quantization_options,
        &collection,
        |vector| {
            if is_residual {
                let target = k_means_lookup(O::interpret(vector), &centroids);
                O::residual(vector, &centroids[(target,)])
            } else {
                vector.own()
            }
        },
    );
    let payloads = MmapArray::create(
        path.as_ref().join("payloads"),
        (0..collection.len()).map(|i| collection.payload(i)),
    );
    let offsets = Json::create(path.as_ref().join("offsets"), offsets);
    let centroids = Json::create(path.as_ref().join("centroids"), centroids);
    let is_residual = Json::create(path.as_ref().join("is_residual"), is_residual);
    Ivf {
        storage,
        quantization,
        payloads,
        offsets,
        centroids,
        is_residual,
    }
}

fn open<O: Op>(path: impl AsRef<Path>) -> Ivf<O> {
    let storage = O::Storage::open(path.as_ref().join("storage"));
    let quantization = Quantization::open(path.as_ref().join("quantization"));
    let payloads = MmapArray::open(path.as_ref().join("payloads"));
    let offsets = Json::open(path.as_ref().join("offsets"));
    let centroids = Json::open(path.as_ref().join("centroids"));
    let is_residual = Json::open(path.as_ref().join("is_residual"));
    Ivf {
        storage,
        quantization,
        payloads,
        offsets,
        centroids,
        is_residual,
    }
}

fn select(mut lists: Vec<(f32, usize)>, n: usize) -> Vec<(f32, usize)> {
    if lists.is_empty() || n == 0 {
        return Vec::new();
    }
    let n = n.min(lists.len());
    lists.select_nth_unstable_by(n - 1, |(x, _), (y, _)| f32::total_cmp(x, y));
    lists.truncate(n);
    lists.sort_by(|(x, _), (y, _)| f32::total_cmp(x, y));
    lists
}
