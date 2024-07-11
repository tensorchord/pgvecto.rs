use super::OperatorIvf as Op;
use base::index::*;
use base::operator::*;
use base::search::*;
use common::json::Json;
use common::mmap_array::MmapArray;
use common::remap::RemappedCollection;
use common::vec2::Vec2;
use elkan_k_means::elkan_k_means;
use elkan_k_means::elkan_k_means_caluate;
use elkan_k_means::elkan_k_means_lookup;
use quantization::Quantization;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::create_dir;
use std::path::Path;
use stoppable_rayon as rayon;
use storage::Storage;

pub struct IvfNaive<O: Op> {
    storage: O::Storage,
    quantization: Quantization<O>,
    payloads: MmapArray<Payload>,
    offsets: Json<Vec<u32>>,
    centroids: Json<Vec2<Scalar<O>>>,
}

impl<O: Op> IvfNaive<O> {
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

    pub fn basic(
        &self,
        vector: Borrowed<'_, O>,
        opts: &SearchOptions,
    ) -> BinaryHeap<Reverse<Element>> {
        let mut lists = elkan_k_means_caluate::<O>(vector, &self.centroids);
        lists.select_nth_unstable(opts.ivf_nprobe as usize);
        lists.truncate(opts.ivf_nprobe as usize);
        let mut result = BinaryHeap::new();
        for i in lists.iter().map(|(_, i)| *i) {
            let start = self.offsets[i];
            let end = self.offsets[i + 1];
            for j in start..end {
                let payload = self.payloads[j as usize];
                let distance = self.quantization.distance(&self.storage, vector, j);
                result.push(Reverse(Element { distance, payload }));
            }
        }
        result
    }

    pub fn vbase<'a>(
        &'a self,
        vector: Borrowed<'a, O>,
        opts: &'a SearchOptions,
    ) -> (Vec<Element>, Box<(dyn Iterator<Item = Element> + 'a)>) {
        let mut lists = elkan_k_means_caluate::<O>(vector, &self.centroids);
        lists.select_nth_unstable(opts.ivf_nprobe as usize);
        lists.truncate(opts.ivf_nprobe as usize);
        let mut result = Vec::new();
        for i in lists.iter().map(|(_, i)| *i) {
            let start = self.offsets[i];
            let end = self.offsets[i + 1];
            for j in start..end {
                let payload = self.payloads[j as usize];
                let distance = self.quantization.distance(&self.storage, vector, j);
                result.push(Element { distance, payload });
            }
        }
        (result, Box::new(std::iter::empty()))
    }
}

fn from_nothing<O: Op>(
    path: impl AsRef<Path>,
    options: IndexOptions,
    collection: &impl Collection<O>,
) -> IvfNaive<O> {
    create_dir(path.as_ref()).unwrap();
    let IvfIndexingOptions {
        nlist,
        quantization: quantization_options,
    } = options.indexing.clone().unwrap_ivf();
    let samples = common::sample::sample(collection);
    rayon::check();
    let centroids = elkan_k_means::<O>(nlist as usize, samples);
    rayon::check();
    let mut ls = vec![Vec::new(); nlist as usize];
    for i in 0..collection.len() {
        ls[elkan_k_means_lookup::<O>(collection.vector(i), &centroids)].push(i);
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
        options.clone(),
        quantization_options,
        &collection,
    );
    let payloads = MmapArray::create(
        path.as_ref().join("payloads"),
        (0..collection.len()).map(|i| collection.payload(i)),
    );
    let offsets = Json::create(path.as_ref().join("offsets"), offsets);
    let centroids = Json::create(path.as_ref().join("centroids"), centroids);
    IvfNaive {
        storage,
        quantization,
        payloads,
        offsets,
        centroids,
    }
}

fn open<O: Op>(path: impl AsRef<Path>) -> IvfNaive<O> {
    let storage = O::Storage::open(path.as_ref().join("storage"));
    let quantization = Quantization::open(path.as_ref().join("quantization"));
    let payloads = MmapArray::open(path.as_ref().join("payloads"));
    let offsets = Json::open(path.as_ref().join("offsets"));
    let centroids = Json::open(path.as_ref().join("centroids"));
    IvfNaive {
        storage,
        quantization,
        payloads,
        offsets,
        centroids,
    }
}
