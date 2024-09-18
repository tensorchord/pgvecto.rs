#![allow(clippy::needless_range_loop)]
#![allow(clippy::type_complexity)]
#![allow(clippy::identity_op)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::len_without_is_empty)]

pub mod operator;
pub mod quant;

use crate::operator::OperatorRabitq as Op;
use crate::quant::quantization::Quantization;
use base::always_equal::AlwaysEqual;
use base::index::{IndexOptions, RabitqIndexingOptions, SearchOptions};
use base::operator::{Borrowed, Owned};
use base::search::{Collection, Element, Payload, RerankerPop, Source, Vectors};
use common::json::Json;
use common::mmap_array::MmapArray;
use common::remap::RemappedCollection;
use common::vec2::Vec2;
use k_means::{k_means, k_means_lookup, k_means_lookup_many};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::fs::create_dir;
use std::path::Path;
use stoppable_rayon as rayon;
use storage::Storage;

pub struct Rabitq<O: Op> {
    storage: O::Storage,
    quantization: Quantization<O>,
    payloads: MmapArray<Payload>,
    offsets: Json<Vec<u32>>,
    projected_centroids: Json<Vec2<f32>>,
    projection: Json<Vec<Vec<f32>>>,
    is_residual: Json<bool>,
}

impl<O: Op> Rabitq<O> {
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
        let projected_query = O::proj(&self.projection, O::cast(vector));
        let lists = select(
            k_means_lookup_many(&projected_query, &self.projected_centroids),
            opts.rabitq_nprobe as usize,
        );
        let mut heap = Vec::new();
        for &(dis_v2, i) in lists.iter() {
            let trans_vector = if *self.is_residual {
                &O::residual(&projected_query, &self.projected_centroids[(i,)])
            } else {
                &projected_query
            };
            let preprocessed = if opts.rabitq_fast_scan {
                self.quantization.fscan_preprocess(trans_vector, dis_v2)
            } else {
                self.quantization.preprocess(trans_vector, dis_v2)
            };
            let start = self.offsets[i];
            let end = self.offsets[i + 1];
            self.quantization
                .push_batch(&preprocessed, start..end, &mut heap, opts.rabitq_epsilon);
        }
        let mut reranker = self.quantization.rerank(heap, move |u| {
            (O::distance(vector, self.storage.vector(u)), ())
        });
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
) -> Rabitq<O> {
    create_dir(path.as_ref()).unwrap();
    let RabitqIndexingOptions {
        nlist,
        spherical_centroids,
        residual_quantization,
    } = options.indexing.clone().unwrap_rabitq();
    let projection = {
        use nalgebra::{DMatrix, QR};
        use rand::Rng;
        use rand_distr::StandardNormal;
        let mut rng = rand::thread_rng();
        let dims = options.vector.dims;
        let matrix: Vec<f32> = (0..dims as usize * dims as usize)
            .map(|_| rng.sample(StandardNormal))
            .collect();
        let matrix = DMatrix::from_vec(dims as usize, dims as usize, matrix);
        let qr = QR::new(matrix);
        let q = qr.q();
        let mut projection = vec![Vec::with_capacity(dims as usize); dims as usize];
        for (i, v) in q.row_iter().enumerate() {
            for &val in v.iter() {
                projection[i].push(val);
            }
        }
        projection
    };
    let is_residual = residual_quantization && O::SUPPORT_RESIDUAL;
    rayon::check();
    let samples = O::sample(collection, nlist);
    rayon::check();
    let centroids: Vec2<f32> = k_means(nlist as usize, samples, true, spherical_centroids, false);
    rayon::check();
    let ls = (0..collection.len())
        .into_par_iter()
        .fold(
            || vec![Vec::new(); nlist as usize],
            |mut state, i| {
                state[k_means_lookup(O::cast(collection.vector(i)), &centroids)].push(i);
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
    rayon::check();
    let storage = O::Storage::create(path.as_ref().join("storage"), &collection);

    let quantization = if is_residual {
        Quantization::create(
            path.as_ref().join("quantization"),
            options.vector,
            collection.len(),
            |vector| {
                let vector = O::cast(collection.vector(vector));
                let target = k_means_lookup(vector, &centroids);
                O::proj(&projection, &O::residual(vector, &centroids[(target,)]))
            },
        )
    } else {
        Quantization::create(
            path.as_ref().join("quantization"),
            options.vector,
            collection.len(),
            |vector| {
                let vector = O::cast(collection.vector(vector));
                O::proj(&projection, vector)
            },
        )
    };

    let projected_centroids = Vec2::from_vec(
        (centroids.shape_0(), centroids.shape_1()),
        (0..centroids.shape_0())
            .flat_map(|x| O::proj(&projection, &centroids[(x,)]))
            .collect(),
    );
    let payloads = MmapArray::create(
        path.as_ref().join("payloads"),
        (0..collection.len()).map(|i| collection.payload(i)),
    );
    let offsets = Json::create(path.as_ref().join("offsets"), offsets);
    let projected_centroids = Json::create(
        path.as_ref().join("projected_centroids"),
        projected_centroids,
    );
    let projection = Json::create(path.as_ref().join("projection"), projection);
    let is_residual = Json::create(path.as_ref().join("is_residual"), is_residual);
    Rabitq {
        storage,
        payloads,
        offsets,
        projected_centroids,
        quantization,
        projection,
        is_residual,
    }
}

fn open<O: Op>(path: impl AsRef<Path>) -> Rabitq<O> {
    let storage = O::Storage::open(path.as_ref().join("storage"));
    let quantization = Quantization::open(path.as_ref().join("quantization"));
    let payloads = MmapArray::open(path.as_ref().join("payloads"));
    let offsets = Json::open(path.as_ref().join("offsets"));
    let projected_centroids = Json::open(path.as_ref().join("projected_centroids"));
    let projection = Json::open(path.as_ref().join("projection"));
    let is_residual = Json::open(path.as_ref().join("is_residual"));
    Rabitq {
        storage,
        quantization,
        payloads,
        offsets,
        projected_centroids,
        projection,
        is_residual,
    }
}

fn select<T>(mut lists: Vec<(f32, T)>, n: usize) -> Vec<(f32, T)> {
    if lists.is_empty() || n == 0 {
        return Vec::new();
    }
    let n = n.min(lists.len());
    lists.select_nth_unstable_by(n - 1, |(x, _), (y, _)| f32::total_cmp(x, y));
    lists.truncate(n);
    lists.sort_by(|(x, _), (y, _)| f32::total_cmp(x, y));
    lists
}
