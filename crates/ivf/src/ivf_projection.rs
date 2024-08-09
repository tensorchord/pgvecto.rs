use super::OperatorIvf as Op;
use base::index::{IndexOptions, IvfIndexingOptions, SearchOptions};
use base::operator::{Borrowed, Scalar};
use base::search::{Collection, Element, Payload, Source, Vectors};
use base::vector::{VectorBorrowed, VectorOwned};
use common::json::Json;
use common::mmap_array::MmapArray;
use common::remap::RemappedCollection;
use common::vec2::Vec2;
use k_means::{k_means, k_means_lookup, k_means_lookup_many};
use quantization::Quantization;
use stoppable_rayon as rayon;
use storage::Storage;

use std::fs::create_dir;
use std::path::Path;

pub struct IvfProjection<O: Op> {
    storage: O::Storage,
    quantization: Quantization<O>,
    payloads: MmapArray<Payload>,
    offsets: Json<Vec<u32>>,
    centroids: Json<Vec2<Scalar<O>>>,
}

impl<O: Op> IvfProjection<O> {
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
        let projected_query = self.quantization.project(&vector.to_vec());
        let lists = select(
            k_means_lookup_many(&projected_query, &self.centroids),
            opts.ivf_nprobe as usize,
        );
        let preprocessed_centroids = lists
            .iter()
            .map(|&(distance, i)| {
                self.quantization.projection_preprocess(
                    O::residual_dense(&projected_query, &self.centroids[(i,)]).as_borrowed(),
                    distance,
                )
            })
            .collect::<Vec<_>>();
        let mut rough_distances = Vec::new();
        for (code, i) in lists.iter().map(|(_, i)| *i).enumerate() {
            let start = self.offsets[i];
            let end = self.offsets[i + 1];
            for u in start..end {
                rough_distances.push((
                    self.quantization
                        .process(&self.storage, &preprocessed_centroids[code], u),
                    u,
                ));
            }
        }
        let mut reranker = self
            .quantization
            .ivf_projection_rerank(rough_distances, move |u| {
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
) -> IvfProjection<O> {
    create_dir(path.as_ref()).unwrap();
    let IvfIndexingOptions {
        nlist,
        quantization: quantization_options,
        spherical_centroids: _,
        residual_quantization: _,
    } = options.indexing.clone().unwrap_ivf();
    let samples = common::sample::sample(collection);
    rayon::check();
    let centroids = k_means(nlist as usize, samples, false);
    rayon::check();
    let mut ls = vec![Vec::new(); nlist as usize];
    for i in 0..collection.len() {
        ls[k_means_lookup(&collection.vector(i).to_vec(), &centroids)].push(i);
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
        quantization_options,
        &collection,
        |vector| {
            let target = k_means_lookup(&vector.to_vec(), &centroids);
            O::residual(vector, &centroids[(target,)])
        },
    );
    let projected_centroids = Vec2::from_vec(
        (centroids.shape_0(), centroids.shape_1()),
        (0..centroids.shape_0())
            .flat_map(|x| quantization.project(&centroids[(x,)]))
            .collect(),
    );
    let payloads = MmapArray::create(
        path.as_ref().join("payloads"),
        (0..collection.len()).map(|i| collection.payload(i)),
    );
    let offsets = Json::create(path.as_ref().join("offsets"), offsets);
    let centroids = Json::create(path.as_ref().join("centroids"), projected_centroids);
    IvfProjection {
        storage,
        payloads,
        offsets,
        centroids,
        quantization,
    }
}

fn open<O: Op>(path: impl AsRef<Path>) -> IvfProjection<O> {
    let storage = O::Storage::open(path.as_ref().join("storage"));
    let quantization = Quantization::open(path.as_ref().join("quantization"));
    let payloads = MmapArray::open(path.as_ref().join("payloads"));
    let offsets = Json::open(path.as_ref().join("offsets"));
    let centroids = Json::open(path.as_ref().join("centroids"));
    IvfProjection {
        storage,
        quantization,
        payloads,
        offsets,
        centroids,
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
