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
use quantization::quantizer::Quantizer;
use quantization::Quantization;
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use std::fs::create_dir;
use std::path::Path;
use stoppable_rayon as rayon;
use storage::Storage;

pub struct Ivf<O: Op, Q: Quantizer<O>> {
    storage: O::Storage,
    quantization: Quantization<O, Q>,
    payloads: MmapArray<Payload>,
    offsets: Json<Vec<u32>>,
    projected_centroids: Json<Vec2<<O as Op>::Scalar>>,
    is_residual: Json<bool>,
}

impl<O: Op, Q: Quantizer<O>> Ivf<O, Q> {
    pub fn create(
        path: impl AsRef<Path>,
        options: IndexOptions,
        source: &(impl Vectors<O::Vector> + Collection + Source + Sync),
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
        let projected_vector = self.quantization.project(vector);
        let lists = select(
            k_means_lookup_many(
                O::interpret(projected_vector.as_borrowed()),
                &self.projected_centroids,
            ),
            opts.ivf_nprobe as usize,
        );
        let mut heap = Q::flat_rerank_start();
        let lut = if *self.is_residual {
            None
        } else {
            Some(
                self.quantization
                    .flat_rerank_preprocess(projected_vector.as_borrowed(), opts),
            )
        };
        for i in lists.iter().map(|(_, i)| *i) {
            let lut = if let Some(lut) = lut.as_ref() {
                lut
            } else {
                &self.quantization.flat_rerank_preprocess(
                    O::residual(
                        projected_vector.as_borrowed(),
                        &self.projected_centroids[(i,)],
                    )
                    .as_borrowed(),
                    opts,
                )
            };
            let start = self.offsets[i];
            let end = self.offsets[i + 1];
            self.quantization
                .flat_rerank_continue(lut, start..end, &mut heap);
        }
        let mut reranker = self.quantization.flat_rerank_break(
            heap,
            move |u| (O::distance(vector, self.storage.vector(u)), ()),
            opts,
        );
        Box::new(std::iter::from_fn(move || {
            reranker.pop().map(|(dis_u, u, ())| Element {
                distance: dis_u,
                payload: AlwaysEqual(self.payload(u)),
            })
        }))
    }
}

fn from_nothing<O: Op, Q: Quantizer<O>>(
    path: impl AsRef<Path>,
    options: IndexOptions,
    collection: &(impl Vectors<O::Vector> + Collection + Sync),
) -> Ivf<O, Q> {
    create_dir(path.as_ref()).unwrap();
    let IvfIndexingOptions {
        nlist,
        spherical_centroids,
        residual_quantization,
        quantization: quantization_options,
    } = options.indexing.clone().unwrap_ivf();
    let samples = O::sample(collection, nlist);
    rayon::check();
    let centroids = k_means(nlist as usize, samples, spherical_centroids, 10, false);
    rayon::check();
    let fa = (0..collection.len())
        .into_par_iter()
        .map(|i| k_means_lookup(O::interpret(collection.vector(i)), &centroids))
        .collect::<Vec<_>>();
    let ls = (0..collection.len())
        .into_par_iter()
        .fold(
            || vec![Vec::new(); nlist as usize],
            |mut state, i| {
                state[fa[i as usize]].push(i);
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
    let quantization = Quantization::<O, Q>::create(
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
    let projected_centroids = Json::create(path.as_ref().join("projected_centroids"), {
        let mut projected_centroids = Vec2::zeros(centroids.shape());
        for i in 0..centroids.shape_0() {
            projected_centroids[(i,)]
                .copy_from_slice(&O::project(quantization.quantizer(), &centroids[(i,)]));
        }
        projected_centroids
    });
    let is_residual = Json::create(path.as_ref().join("is_residual"), is_residual);
    Ivf {
        storage,
        quantization,
        payloads,
        offsets,
        projected_centroids,
        is_residual,
    }
}

fn open<O: Op, Q: Quantizer<O>>(path: impl AsRef<Path>) -> Ivf<O, Q> {
    let storage = O::Storage::open(path.as_ref().join("storage"));
    let quantization = Quantization::open(path.as_ref().join("quantization"));
    let payloads = MmapArray::open(path.as_ref().join("payloads"));
    let offsets = Json::open(path.as_ref().join("offsets"));
    let projected_centroids = Json::open(path.as_ref().join("projected_centroids"));
    let is_residual = Json::open(path.as_ref().join("is_residual"));
    Ivf {
        storage,
        quantization,
        payloads,
        offsets,
        projected_centroids,
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
