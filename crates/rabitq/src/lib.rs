#![allow(clippy::needless_range_loop)]
#![allow(clippy::type_complexity)]
#![allow(clippy::identity_op)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::len_without_is_empty)]

pub mod operator;
pub mod quant;

use crate::operator::OperatorRabitq as Op;
use crate::quant::quantization::Quantization;
use base::index::{IndexOptions, RabitqIndexingOptions, SearchOptions};
use base::operator::Borrowed;
use base::scalar::F32;
use base::search::{Collection, Element, Payload, Source, Vectors};
use common::json::Json;
use common::mmap_array::MmapArray;
use common::remap::RemappedCollection;
use common::vec2::Vec2;
use k_means::{k_means, k_means_lookup, k_means_lookup_many};
use std::fs::create_dir;
use std::path::Path;
use stoppable_rayon as rayon;
use storage::Storage;

pub struct Rabitq<O: Op> {
    storage: O::Storage,
    quantization: Quantization<O>,
    payloads: MmapArray<Payload>,
    offsets: Json<Vec<u32>>,
    centroids: Json<Vec2<F32>>,
    projection: Json<Vec<Vec<F32>>>,
}

impl<O: Op> Rabitq<O> {
    pub fn create(path: impl AsRef<Path>, options: IndexOptions, source: &impl Source<O>) -> Self {
        let remapped = RemappedCollection::from_source(source);
        from_nothing(path, options, &remapped)
    }

    pub fn open(path: impl AsRef<Path>) -> Self {
        open(path)
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
    ) -> (Vec<Element>, Box<dyn Iterator<Item = Element> + 'a>) {
        let projected_query = O::proj(&self.projection, O::cast(vector));
        let lists = select(
            k_means_lookup_many(&projected_query, &self.centroids),
            opts.rabitq_nprobe as usize,
        );
        let mut heap = Vec::new();
        for &(_, i) in lists.iter() {
            let preprocessed = self
                .quantization
                .preprocess(&O::residual(&projected_query, &self.centroids[(i,)]));
            let start = self.offsets[i];
            let end = self.offsets[i + 1];
            self.quantization.push_batch(
                &preprocessed,
                start..end,
                &mut heap,
                F32(1.9),
                opts.rabitq_fast_scan,
            );
        }
        let mut reranker = self.quantization.rerank(heap, move |u| {
            (
                O::distance(vector, self.storage.vector(u)),
                self.payloads[u as usize],
            )
        });
        (
            Vec::new(),
            Box::new(std::iter::from_fn(move || {
                reranker.pop().map(|(dis_u, _, payload_u)| Element {
                    distance: dis_u,
                    payload: payload_u,
                })
            })),
        )
    }
}

fn from_nothing<O: Op>(
    path: impl AsRef<Path>,
    options: IndexOptions,
    collection: &impl Collection<O>,
) -> Rabitq<O> {
    create_dir(path.as_ref()).unwrap();
    let RabitqIndexingOptions { nlist } = options.indexing.clone().unwrap_rabitq();
    let projection = {
        use nalgebra::debug::RandomOrthogonal;
        use nalgebra::{Dim, Dyn};
        use rand::Rng;
        let dims = options.vector.dims as usize;
        let mut rng = rand::thread_rng();
        let random_orth: RandomOrthogonal<f32, Dyn> =
            RandomOrthogonal::new(Dim::from_usize(dims), || rng.gen());
        let random_matrix = random_orth.unwrap();
        let mut projection = vec![Vec::with_capacity(dims); dims];
        // use the transpose of the random matrix as the inverse of the orthogonal matrix,
        // but we need to transpose it again to make it efficient for the dot production
        for (i, vec) in random_matrix.row_iter().enumerate() {
            for &val in vec.iter() {
                projection[i].push(F32(val));
            }
        }
        projection
    };
    let samples = common::sample::sample_cast(collection);
    rayon::check();
    let centroids: Vec2<F32> = k_means(nlist as usize, samples, false);
    rayon::check();
    let mut ls = vec![Vec::new(); nlist as usize];
    for i in 0..collection.len() {
        ls[k_means_lookup(O::cast(collection.vector(i)), &centroids)].push(i);
    }
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
    let quantization = Quantization::create(
        path.as_ref().join("quantization"),
        options.vector,
        collection.len(),
        |vector| {
            let vector = O::cast(collection.vector(vector));
            let target = k_means_lookup(vector, &centroids);
            O::proj(&projection, &O::residual(vector, &centroids[(target,)]))
        },
    );
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
    let centroids = Json::create(path.as_ref().join("centroids"), projected_centroids);
    let projection = Json::create(path.as_ref().join("projection"), projection);
    Rabitq {
        storage,
        payloads,
        offsets,
        centroids,
        quantization,
        projection,
    }
}

fn open<O: Op>(path: impl AsRef<Path>) -> Rabitq<O> {
    let storage = O::Storage::open(path.as_ref().join("storage"));
    let quantization = Quantization::open(path.as_ref().join("quantization"));
    let payloads = MmapArray::open(path.as_ref().join("payloads"));
    let offsets = Json::open(path.as_ref().join("offsets"));
    let centroids = Json::open(path.as_ref().join("centroids"));
    let projection = Json::open(path.as_ref().join("projection"));
    Rabitq {
        storage,
        quantization,
        payloads,
        offsets,
        centroids,
        projection,
    }
}

fn select<T: Ord>(mut lists: Vec<T>, n: usize) -> Vec<T> {
    if lists.is_empty() || n == 0 {
        return Vec::new();
    }
    let n = n.min(lists.len());
    lists.select_nth_unstable(n - 1);
    lists.truncate(n);
    lists
}
