#![allow(clippy::len_without_is_empty)]

pub mod operator;
mod quantized_summary;

use base::index::*;
use base::operator::*;
use base::scalar::F32;
use base::search::*;
use base::vector::SVecf32Borrowed;
use base::vector::SVecf32Owned;
use common::mmap_array::MmapArray;
use common::remap::RemappedCollection;
use common::visited::VisitedGuardChecker;
use common::visited::VisitedPool;
use operator::OperatorSeismic;
use quantized_summary::QuantizedSummary;
use rand::seq::IteratorRandom;
use serde::Deserialize;
use serde::Serialize;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::fs::create_dir;
use std::path::Path;
use stoppable_rayon as rayon;
use stoppable_rayon::iter::IntoParallelIterator;
use stoppable_rayon::iter::IntoParallelRefMutIterator;
use stoppable_rayon::iter::ParallelIterator;
use storage::Storage;

const MIN_CLUSTER_SIZE: usize = 2;

#[derive(Debug, Serialize, Deserialize)]
struct PostingList {
    postings: Box<[u32]>,
    block_offsets: Box<[usize]>,
    summaries: QuantizedSummary,
}
pub struct Seismic<O: OperatorSeismic> {
    storage: O::Storage,
    payloads: MmapArray<Payload>,
    // ----------------------
    posting_lists: Box<[PostingList]>,
    // ----------------------
    visited: VisitedPool,
}

impl<O: OperatorSeismic> Seismic<O> {
    pub fn create(path: impl AsRef<Path>, options: IndexOptions, source: &impl Source<O>) -> Self {
        let remapped = RemappedCollection::from_source(source);
        from_nothing(path.as_ref(), options, &remapped)
    }

    pub fn open(path: impl AsRef<Path>) -> Self {
        open(path.as_ref())
    }

    pub fn vbase<'a>(
        &'a self,
        vector: Borrowed<'a, O>,
        opts: &'a SearchOptions,
    ) -> (Vec<Element>, Box<(dyn Iterator<Item = Element> + 'a)>) {
        vbase(
            self,
            vector,
            opts.hnsw_ef_search,
            opts.seismic_q_cut,
            opts.seismic_heap_factor,
        )
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
}

fn from_nothing<O: OperatorSeismic>(
    path: &Path,
    options: IndexOptions,
    collection: &(impl Collection<O> + Sync),
) -> Seismic<O> {
    create_dir(path).unwrap();
    let SeismicIndexingOptions {
        n_postings,
        centroid_fraction,
        summary_energy,
    } = options.indexing.clone().unwrap_seismic();
    let n_postings = n_postings as usize;
    let centroids = (n_postings as f32 * centroid_fraction) as usize;
    let dims = collection.dims() as usize;
    rayon::check();

    // 1. Static Pruning
    let mut postings = Vec::new();
    for i in 0..collection.len() {
        let vector = O::cast_svec(collection.vector(i));
        for (&index, &value) in vector.indexes().iter().zip(vector.values()) {
            postings.push((value, i, index));
        }
        for v in vector.values() {
            assert!(v.0 > 0., "Seismic index doesn't support negative values");
        }
    }
    let tot_postings = std::cmp::min(dims * n_postings, postings.len() - 1);
    postings.select_nth_unstable_by(tot_postings, |a, b| b.0.cmp(&a.0));
    let mut inverted_lists: Vec<Vec<(F32, u32)>> = vec![Vec::new(); dims];
    for (val, id, index) in postings.into_iter().take(tot_postings) {
        inverted_lists[index as usize].push((val, id));
    }
    inverted_lists.par_iter_mut().for_each(|vec| {
        vec.sort_by(|a, b| b.0.cmp(&a.0));
        vec.truncate(n_postings * 3 / 2);
        vec.shrink_to_fit();
    });

    let posting_lists = inverted_lists
        .into_par_iter()
        .map(|postings| build_posting_list(&postings, centroids, summary_energy, collection))
        .collect();

    rayon::check();
    let storage = O::Storage::create(path.join("storage"), collection);
    rayon::check();
    let payloads = MmapArray::create(
        path.join("payloads"),
        (0..collection.len()).map(|i| collection.payload(i)),
    );
    rayon::check();
    let posting_lists_file = std::fs::File::create_new(path.join("posting_lists")).unwrap();
    let posting_lists_writer = std::io::BufWriter::new(posting_lists_file);
    bincode::serialize_into(posting_lists_writer, &posting_lists).unwrap();
    rayon::check();
    let visited = VisitedPool::new(storage.len());

    Seismic {
        storage,
        payloads,
        posting_lists,
        visited,
    }
}

fn build_posting_list<O: OperatorSeismic>(
    postings: &[(F32, u32)],
    centroids: usize,
    summary_energy: f32,
    collection: &(impl Collection<O> + Sync),
) -> PostingList {
    // 2. Blocking of Inverted Lists
    let mut postings = postings.iter().map(|&(_, i)| i).collect::<Box<[_]>>();
    let mut block_offsets = Vec::new();
    if !postings.is_empty() {
        let mut reordered_postings = Vec::with_capacity(postings.len());
        block_offsets = Vec::with_capacity(centroids + 1);
        let clustering_results = random_kmeans(&postings, centroids, collection);
        block_offsets.push(0);
        for cluster in clustering_results {
            if cluster.is_empty() {
                continue;
            }
            reordered_postings.extend(cluster);
            block_offsets.push(reordered_postings.len());
        }
        postings.copy_from_slice(&reordered_postings);
    }

    // 3. Per-block Summary Vectors
    let summary_vectors = block_offsets
        .windows(2)
        .map(|w| {
            let mut map = HashMap::new();
            for &id in &postings[w[0]..w[1]] {
                let vector = O::cast_svec(collection.vector(id));
                for i in 0..vector.len() {
                    let index = vector.indexes()[i as usize];
                    let value = vector.values()[i as usize];
                    map.entry(index)
                        .and_modify(|v| *v = std::cmp::max(*v, value))
                        .or_insert(value);
                }
            }

            // alpha mass
            let min_l1 = map.iter().map(|x| x.1 .0).sum::<f32>() * summary_energy;
            let mut indexes = map.keys().copied().collect::<Vec<_>>();
            indexes.sort_unstable_by_key(|&i| Reverse(map[&i]));
            let mut l1 = 0.;
            let mut j = 0;
            while j < indexes.len() {
                let val = map[&indexes[j]].0;
                l1 += val;
                j += 1;
                if l1 >= min_l1 {
                    break;
                }
            }
            indexes.truncate(j);
            indexes.sort_unstable();
            let values = indexes.iter().map(|i| map[i]).collect::<Vec<_>>();
            SVecf32Owned::new(collection.dims(), indexes, values)
        })
        .collect::<Vec<_>>();
    let summaries = QuantizedSummary::create(collection.dims(), &summary_vectors);

    PostingList {
        postings,
        block_offsets: block_offsets.into_boxed_slice(),
        summaries,
    }
}

fn random_kmeans<O: OperatorSeismic>(
    postings: &[u32],
    centroids: usize,
    collection: &(impl Collection<O> + Sync),
) -> Vec<Vec<u32>> {
    let mut rng = rand::thread_rng();
    let centroid_ids = postings
        .iter()
        .copied()
        .choose_multiple(&mut rng, centroids);

    let mut inverted_lists = vec![Vec::new(); centroid_ids.len()];
    for &id in postings {
        let vector = O::cast_svec(collection.vector(id));
        // Because `SVecf32Dot::distance` calculates (-1 * dot), we use `min_by_key` instead of `max_by_key` here
        let argmax = (0..centroid_ids.len())
            .min_by_key(|&j| {
                let centroid = O::cast_svec(collection.vector(centroid_ids[j]));
                SVecf32Dot::distance(centroid, vector)
            })
            .unwrap();
        inverted_lists[argmax].push(id);
    }

    let mut to_be_replaced = Vec::new(); // ids that belong to too small clusters
    for inverted_list in inverted_lists.iter_mut() {
        if !inverted_list.is_empty() && inverted_list.len() <= MIN_CLUSTER_SIZE {
            to_be_replaced.extend(inverted_list.iter().copied());
            inverted_list.clear();
        }
    }

    for id in to_be_replaced {
        let vector = O::cast_svec(collection.vector(id));
        let argmax = (0..centroid_ids.len())
            .min_by_key(|&j| {
                if inverted_lists[j].len() < MIN_CLUSTER_SIZE {
                    return F32(0.);
                }
                let centroid = O::cast_svec(collection.vector(centroid_ids[j]));
                SVecf32Dot::distance(centroid, vector)
            })
            .unwrap();
        inverted_lists[argmax].push(id);
    }

    inverted_lists
}

fn open<O: OperatorSeismic>(path: &Path) -> Seismic<O> {
    let storage = O::Storage::open(path.join("storage"));
    let payloads = MmapArray::open(path.join("payloads"));
    let posting_lists_file = std::fs::File::open(path.join("posting_lists")).unwrap();
    let posting_lists_reader = std::io::BufReader::new(posting_lists_file);
    let posting_lists = bincode::deserialize_from(posting_lists_reader).unwrap();
    let visited = VisitedPool::new(storage.len());
    Seismic {
        storage,
        payloads,
        posting_lists,
        visited,
    }
}

fn vbase<'a, O: OperatorSeismic>(
    s: &'a Seismic<O>,
    vector: Borrowed<'a, O>,
    k: u32,
    q_cut: u32,
    heap_factor: f32,
) -> (Vec<Element>, Box<(dyn Iterator<Item = Element> + 'a)>) {
    let mut visited = s.visited.fetch_guard_checker();
    let vector = O::cast_svec(vector);
    let mut perm = (0..vector.len()).collect::<Vec<_>>();
    perm.sort_unstable_by_key(|&i| Reverse(vector.values()[i as usize]));
    perm.truncate(q_cut as usize);
    let top_cut = perm.into_iter().map(|i| vector.indexes()[i as usize]);
    let mut heap = ElementHeap::new(k as usize);
    for i in top_cut {
        let posting_list = &s.posting_lists[i as usize];
        let mut blocks_to_evaluate = None;
        let summary_dots = posting_list.summaries.matmul(vector);

        for (j, &dot) in summary_dots.iter().enumerate() {
            if !heap.check(-dot / heap_factor) {
                continue;
            }
            let offset1 = posting_list.block_offsets[j];
            let offset2 = posting_list.block_offsets[j + 1];
            let posting_block = &posting_list.postings[offset1..offset2];

            if let Some(block) = std::mem::replace(&mut blocks_to_evaluate, Some(posting_block)) {
                vbase_block(s, block, vector, &mut heap, &mut visited);
            }

            for i in (0..posting_block.len()).step_by(8) {
                let ptr = posting_block.as_ptr().wrapping_add(i);
                common::prefetch::prefetch_read_NTA(ptr as *const i8);
            }
        }

        if let Some(block) = blocks_to_evaluate {
            vbase_block(s, block, vector, &mut heap, &mut visited);
        }
    }

    (heap.into_vec(), Box::new(std::iter::empty()))
}

#[inline]
fn vbase_block<O: OperatorSeismic>(
    s: &Seismic<O>,
    block: &[u32],
    vector: SVecf32Borrowed,
    heap: &mut ElementHeap,
    visited: &mut VisitedGuardChecker<'_>,
) {
    let mut prev_id = block[0];
    for &id in block.iter().skip(1) {
        O::prefetch(&s.storage, id);

        if visited.check(prev_id) {
            let distance = SVecf32Dot::distance(vector, O::cast_svec(s.storage.vector(prev_id)));
            if heap.check(distance) {
                let payload = s.payload(prev_id);
                heap.push(Element { distance, payload });
            }
            visited.mark(prev_id);
        }

        prev_id = id;
    }

    if visited.check(prev_id) {
        let distance = SVecf32Dot::distance(vector, O::cast_svec(s.storage.vector(prev_id)));
        if heap.check(distance) {
            let payload = s.payload(prev_id);
            heap.push(Element { distance, payload });
        }
        visited.mark(prev_id);
    }
}

pub struct ElementHeap {
    binary_heap: BinaryHeap<Element>,
    k: usize,
}

impl ElementHeap {
    pub fn new(k: usize) -> Self {
        assert!(k != 0);
        Self {
            binary_heap: BinaryHeap::new(),
            k,
        }
    }
    pub fn check(&self, distance: F32) -> bool {
        self.binary_heap.len() < self.k || distance < self.binary_heap.peek().unwrap().distance
    }
    pub fn push(&mut self, element: Element) -> Option<Element> {
        self.binary_heap.push(element);
        if self.binary_heap.len() == self.k + 1 {
            self.binary_heap.pop()
        } else {
            None
        }
    }
    pub fn into_reversed_heap(self) -> BinaryHeap<Reverse<Element>> {
        self.binary_heap.into_iter().map(Reverse).collect()
    }

    pub fn into_vec(self) -> Vec<Element> {
        self.binary_heap.into_sorted_vec()
    }
}
