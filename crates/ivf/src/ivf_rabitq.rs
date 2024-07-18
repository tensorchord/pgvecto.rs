use std::{fs::create_dir, path::Path, vec};

use base::{
    distance::DistanceKind,
    index::{IndexOptions, IvfIndexingOptions, SearchOptions},
    operator::{Borrowed, Scalar},
    search::{Collection, Element, Payload, Source, Vectors},
    vector::{VectorBorrowed, VectorKind},
};
use common::{json::Json, mmap_array::MmapArray, remap::RemappedCollection, vec2::Vec2};
use k_means::{k_means, k_means_lookup};
use quantization::Quantization;
use stoppable_rayon as rayon;
use storage::Storage;

use super::OperatorIvf as Op;

pub struct IvfRaBitQ<O: Op> {
    storage: O::Storage,
    quantization: Quantization<O>,
    payloads: MmapArray<Payload>,
    offsets: Json<Vec<u32>>,
    centroids: Json<Vec2<Scalar<O>>>,
}

impl<O: Op> IvfRaBitQ<O> {
    pub fn create(path: impl AsRef<Path>, options: IndexOptions, source: &impl Source<O>) -> Self {
        if options.vector.d != DistanceKind::L2 {
            panic!("RaBitQ only supports L2 distance");
        }
        if options.vector.v != VectorKind::Vecf32 {
            panic!("RaBitQ only supports Vecf32 vectors");
        }
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
        unimplemented!()
    }
}

fn from_nothing<O: Op>(
    path: impl AsRef<Path>,
    options: IndexOptions,
    collection: &impl Collection<O>,
) -> IvfRaBitQ<O> {
    create_dir(path.as_ref()).expect("failed to create dir for IvfRaBitQ");
    let IvfIndexingOptions {
        nlist,
        quantization: quantization_options,
    } = options.indexing.clone().unwrap_ivf();
    let samples = common::sample::sample(collection);
    rayon::check();
    let centroids = {
        let mut samples = samples;
        for i in 0..samples.len() {
            O::elkan_k_means_normalize(&mut samples[i]);
        }
        k_means(nlist as usize, samples)
    };
    rayon::check();
    let mut ls = vec![Vec::new(); nlist as usize];
    for i in 0..collection.len() {
        ls[{
            let mut vector = collection.vector(i).to_vec();
            O::elkan_k_means_normalize(&mut vector);
            k_means_lookup(&vector, &centroids)
        }]
        .push(i);
    }
    let mut offsets = vec![0u32; nlist as usize + 1];
    for i in 0..(nlist as usize) {
        offsets[i + 1] = offsets[i] + ls[i].len() as u32;
    }
    let remap = ls
        .into_iter()
        .flat_map(|x| x.into_iter())
        .collect::<Vec<_>>();
    let collection = RemappedCollection::from_collection(collection, remap);
    rayon::check();

    let storage = O::Storage::create(path.as_ref().join("storage"), &collection);
    let payloads = MmapArray::create(
        path.as_ref().join("payloads"),
        (0..collection.len()).map(|i| collection.payload(i)),
    );
    let offsets_json = Json::create(path.as_ref().join("offsets"), offsets);
    let quantization = Quantization::create(
        path.as_ref().join("quantization"),
        options.vector,
        quantization_options,
        &collection,
        |vector| {
            let target = {
                let mut vector = vector.to_vec();
                O::elkan_k_means_normalize(&mut vector);
                k_means_lookup(&vector, &centroids)
            };
            O::vector_sub(vector, &centroids[target])
        },
    );
    let centroids_json = Json::create(path.as_ref().join("centroids"), centroids);

    IvfRaBitQ {
        storage,
        quantization,
        payloads,
        offsets: offsets_json,
        centroids: centroids_json,
    }
}

fn open<O: Op>(path: impl AsRef<Path>) -> IvfRaBitQ<O> {
    let storage = O::Storage::open(path.as_ref().join("storage"));
    let quantization = Quantization::open(path.as_ref().join("quantization"));
    let payloads = MmapArray::open(path.as_ref().join("payloads"));
    let offsets = Json::open(path.as_ref().join("offsets"));
    let centroids = Json::open(path.as_ref().join("centroids"));
    IvfRaBitQ {
        storage,
        quantization,
        payloads,
        offsets,
        centroids,
    }
}
