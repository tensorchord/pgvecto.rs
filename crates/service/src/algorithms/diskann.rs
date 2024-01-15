use super::raw::Raw;
use crate::index::segments::growing::GrowingSegment;
use crate::index::segments::sealed::SealedSegment;
use crate::index::{IndexOptions, SearchOptions, VectorOptions};
use crate::prelude::*;
use crate::utils::dir_ops::sync_dir;
use crate::utils::mmap_array::MmapArray;
use crate::utils::element_heap::ElementHeap;
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use rand::distributions::Uniform;
use rand::Rng;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::create_dir;
use std::collections::{BTreeMap, HashSet};
use rand::prelude::SliceRandom;
use parking_lot::{RwLock, RwLockWriteGuard};
use std::path::PathBuf;
use std::sync::Arc;

pub struct DiskANN<S: G> {
    mmap: DiskANNMmap<S>,
}

impl<S: G> DiskANN<S> {
    pub fn create(
        path: PathBuf,
        options: IndexOptions,
        sealed: Vec<Arc<SealedSegment<S>>>,
        growing: Vec<Arc<GrowingSegment<S>>>,
    ) -> Self {
        create_dir(&path).unwrap();
        let ram = make(path.clone(), sealed, growing, options.clone());
        let mmap = save(ram, path.clone());
        sync_dir(&path);
        Self { mmap }
    }
    pub fn open(path: PathBuf, options: IndexOptions) -> Self {
        let mmap = load(path, options.clone());
        Self { mmap }
    }

    pub fn len(&self) -> u32 {
        self.mmap.raw.len()
    }

    pub fn vector(&self, i: u32) -> &[S::Scalar] {
        self.mmap.raw.vector(i)
    }

    pub fn payload(&self, i: u32) -> Payload {
        self.mmap.raw.payload(i)
    }

    pub fn basic(
        &self,
        vector: &[S::Scalar],
        opts: &SearchOptions,
        filter: impl Filter,
    ) -> BinaryHeap<Reverse<Element>> {
        basic(&self.mmap, vector, opts.disk_ann_k, filter)
    }
}

unsafe impl<S: G> Send for DiskANN<S> {}
unsafe impl<S: G> Sync for DiskANN<S> {}

pub struct VertexWithDistance {
    pub id: u32,
    pub distance: F32,
}

impl VertexWithDistance {
    pub fn new(id: u32, distance: F32) -> Self {
        Self { id, distance }
    }
}

impl PartialEq for VertexWithDistance {
    fn eq(&self, other: &Self) -> bool {
        self.distance.eq(&other.distance)
    }
}

impl Eq for VertexWithDistance {}

impl PartialOrd for VertexWithDistance {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.distance.cmp(&other.distance))
    }
}

impl Ord for VertexWithDistance {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.distance.cmp(&other.distance)
    }
}

pub struct SearchState {
    pub visited: HashSet<u32>,
    candidates: BTreeMap<F32, u32>,
    heap: BinaryHeap<Reverse<VertexWithDistance>>,
    heap_visited: HashSet<u32>,
    l: usize,
    k: usize,
}

impl SearchState {
    /// Creates a new search state.
    pub(crate) fn new(k: usize, l: usize) -> Self {
        Self {
            visited: HashSet::new(),
            candidates: BTreeMap::new(),
            heap: BinaryHeap::new(),
            heap_visited: HashSet::new(),
            k,
            l,
        }
    }

    /// Return the next unvisited vertex.
    fn pop(&mut self) -> Option<u32> {
        while let Some(vertex) = self.heap.pop() {
            if !self.candidates.contains_key(&vertex.0.distance) {
                // The vertex has been removed from the candidate lists,
                // from [`push()`].
                continue;
            }

            self.visited.insert(vertex.0.id);
            return Some(vertex.0.id);
        }

        None
    }

    /// Push a new (unvisited) vertex into the search state.
    fn push(&mut self, vertex_id: u32, distance: F32) {
        assert!(!self.visited.contains(&vertex_id));
        self.heap_visited.insert(vertex_id);
        self.heap
            .push(Reverse(VertexWithDistance::new(vertex_id, distance)));
        self.candidates.insert(distance, vertex_id);
        if self.candidates.len() > self.l {
            self.candidates.pop_last();
        }
    }

    /// Mark a vertex as visited.
    fn visit(&mut self, vertex_id: u32) {
        self.visited.insert(vertex_id);
    }

    // Returns true if the vertex has been visited.
    fn is_visited(&self, vertex_id: u32) -> bool {
        self.visited.contains(&vertex_id) || self.heap_visited.contains(&vertex_id)
    }
}

struct VertexNeighbor{
    neighbors: Vec<u32>,
}

// DiskANNRam is for constructing the index
// it stores the intermediate structure when constructing 
// the index and these data are stored in memory
pub struct DiskANNRam<S: G> {
    raw: Arc<Raw<S>>,
    // quantization: Quantization<S>,
    vertexs: Vec<RwLock<VertexNeighbor>>,
    /// the entry for the entire graph, the closet vector to centroid
    medoid: u32,
    dims: u16,
    max_degree: u32,
    alpha: f32,
    l_build: u32,
}

pub struct DiskANNMmap<S: G> {
    raw: Arc<Raw<S>>,
    neighbors: MmapArray<u32>,
    neighbor_offset: MmapArray<usize>,
    medoid: MmapArray<u32>,
    r: u32,
    alpha: f32,
    l: u32,
}

impl<S:G> DiskANNRam<S>{
    fn _init_graph(&self, n: u32, mut rng: impl Rng) {
        let distribution = Uniform::new(0, n);
        for i in 0..n {
            let mut neighbor_ids: HashSet<u32> = HashSet::new();
            if self.max_degree < n {
                while neighbor_ids.len() < self.max_degree as usize {
                    let neighbor_id = rng.sample(distribution);
                    if neighbor_id != i {
                        neighbor_ids.insert(neighbor_id);
                    }
                }
            } else {
                neighbor_ids = (0..n).collect();
            }

            self._set_neighbors(i, &neighbor_ids);
        }
    }

    fn _set_neighbors(
        &self,
        vertex_index: u32,
        neighbor_ids: &HashSet<u32>,
    ) {
        assert!(neighbor_ids.len() <= self.max_degree as usize);
        assert!((vertex_index as usize) < self.vertexs.len());

        let mut vertex = self.vertexs[vertex_index as usize].write();
        vertex.neighbors.clear();
        for item in neighbor_ids.iter() {
            vertex.neighbors.push(*item);
        }
    }

    fn _set_neighbors_with_write_guard(
        &self,
        neighbor_ids: &HashSet<u32>,
        guard: &RwLockWriteGuard<VertexNeighbor>,
    ) {
        assert!(neighbor_ids.len() <= self.max_degree as usize);
        (*guard).neighbors.clear();
        for item in neighbor_ids.iter() {
            (*guard).neighbors.push(*item);
        }
    }

    fn _get_neighbors(
        &self,
        vertex_index: u32,
    ) -> VertexNeighbor {
        let vertex = self.vertexs[vertex_index as usize].read();
        *vertex
    }

    fn _find_medoid(&self, n: u32) -> u32 {
        let centroid = self._compute_centroid(n);
        let centroid_arr: &[S::Scalar] = &centroid;

        let mut medoid_index = 0;
        let mut min_dis = F32::infinity();
        for i in 0..n {
            let dis = S::distance(centroid_arr, self.raw.vector(i));
            if dis < min_dis {
                min_dis = dis;
                medoid_index = i;
            }
        }
        medoid_index
    }

    fn _compute_centroid(&self, n: u32) -> Vec<S::Scalar> {
        let dim = self.dims as usize;
        let mut sum = vec![0_f32; dim];
        for i in 0..n {
            let vec = self.raw.vector(i);
            for j in 0..dim {
                sum[j] += vec[j].to_f32();
            }
        }

        let collection: Vec<S::Scalar> = sum
            .iter()
            .map(|v| S::Scalar::from_f32((*v / n as f32) as f32))
            .collect();
        collection
    }

    // r and l leave here for multiple pass extension
    fn _one_pass(&self, n: u32, alpha: f32, r: u32, l: u32, mut rng: impl Rng) {
        let mut ids = (0..n).collect::<Vec<_>>();
        ids.shuffle(&mut rng);

        ids.into_par_iter()
            .for_each(|id| self.search_and_prune_for_one_vertex(id, alpha, r, l));
    }

    fn search_and_prune_for_one_vertex(&self, id: u32, alpha: f32, r: u32, l: u32) {
        let query = self.raw.vector(id);
        let mut state = self._greedy_search(self.medoid, query, 1, l as usize);
        state.visited.remove(&id); // in case visited has id itself
        let mut new_neighbor_ids: HashSet<u32> = HashSet::new();
        {
            let mut guard = self.vertexs[id as usize].write();
            let neighbor_ids : Vec<u32> = (*guard).neighbors;
            state.visited.extend(neighbor_ids.iter().map(|x| *x));
            let neighbor_ids = self._robust_prune(id, state.visited, alpha, r);
            let neighbor_ids: HashSet<u32> = neighbor_ids.into_iter().collect();
            self._set_neighbors_with_write_guard(&neighbor_ids, &mut guard);
            new_neighbor_ids = neighbor_ids;
        }

        for &neighbor_id in new_neighbor_ids.iter() {
            {
                let mut guard = self.vertexs[neighbor_id as usize].write();
                let old_neighbors : Vec<u32> = (*guard).neighbors;
                let mut old_neighbors: HashSet<u32> =
                    old_neighbors.iter().map(|x| *x).collect();
                old_neighbors.insert(id);
                if old_neighbors.len() > r as usize {
                    // need robust prune
                    let new_neighbors = self._robust_prune(neighbor_id, old_neighbors, alpha, r);
                    let new_neighbors: HashSet<u32> = new_neighbors.into_iter().collect();
                    self._set_neighbors_with_write_guard(&new_neighbors, &mut guard);
                } else {
                    self._set_neighbors_with_write_guard(&old_neighbors, &mut guard);
                }
            }
        }
    }

    fn _greedy_search(
        &self,
        start: u32,
        query: &[S::Scalar],
        k: usize,
        search_size: usize,
    ) -> SearchState {
        let mut state = SearchState::new(k, search_size);

        let dist = S::distance(query, self.raw.vector(start));
        state.push(start, dist);
        while let Some(id) = state.pop() {
            // only pop id in the search list but not visited
            state.visit(id);
            {
                let neighbor_ids = self._get_neighbors(id).neighbors;
                for neighbor_id in neighbor_ids {
                    if state.is_visited(neighbor_id) {
                        continue;
                    }

                    let dist = S::distance(query, self.raw.vector(neighbor_id));
                    state.push(neighbor_id, dist); // push and retain closet l nodes
                }
            }
        }

        state
    }

    fn _robust_prune(&self, id: u32, mut visited: HashSet<u32>, alpha: f32, r: u32) -> Vec<u32> {
        let mut heap: BinaryHeap<VertexWithDistance> = visited
            .iter()
            .map(|v| {
                let dist = S::distance(self.raw.vector(id), self.raw.vector(*v));
                VertexWithDistance {
                    id: *v,
                    distance: dist,
                }
            })
            .collect();

        let mut new_neighbor_ids: Vec<u32> = vec![];
        while !visited.is_empty() {
            if let Some(mut p) = heap.pop() {
                while !visited.contains(&p.id) {
                    match heap.pop() {
                        Some(value) => {
                            p = value;
                        }
                        None => {
                            return new_neighbor_ids;
                        }
                    }
                }
                new_neighbor_ids.push(p.id);
                if new_neighbor_ids.len() >= r as usize {
                    break;
                }
                let mut to_remove: HashSet<u32> = HashSet::new();
                for pv in visited.iter() {
                    let dist_prime = S::distance(self.raw.vector(p.id), self.raw.vector(*pv));
                    let dist_query = S::distance(self.raw.vector(id), self.raw.vector(*pv));
                    if F32::from(alpha) * dist_prime <= dist_query {
                        to_remove.insert(*pv);
                    }
                }
                for pv in to_remove.iter() {
                    visited.remove(pv);
                }
            } else {
                return new_neighbor_ids;
            }
        }
        new_neighbor_ids
    }
}

impl<S:G> DiskANNMmap<S>{
    fn _get_neighbors(self, id: u32) -> Vec<u32>{
        let start = self.neighbor_offset[id as usize];
        let end = self.neighbor_offset[id as usize + 1];
        self.neighbors[start..end].to_vec()
    }
}

unsafe impl<S: G> Send for DiskANNMmap<S> {}
unsafe impl<S: G> Sync for DiskANNMmap<S> {}

fn calculate_offsets(iter: impl Iterator<Item = usize>) -> impl Iterator<Item = usize> {
    let mut offset = 0usize;
    let mut iter = std::iter::once(0).chain(iter);
    std::iter::from_fn(move || {
        let x = iter.next()?;
        offset += x;
        Some(offset)
    })
}

pub fn make<S: G>(
    path: PathBuf,
    sealed: Vec<Arc<SealedSegment<S>>>,
    growing: Vec<Arc<GrowingSegment<S>>>,
    options: IndexOptions,
) -> DiskANNRam<S> {
    let idx_opts = options.indexing.clone().unwrap_diskann();
    let raw = Arc::new(Raw::create(
        path.join("raw"),
        options.clone(),
        sealed,
        growing,
    ));

    let n = raw.len();
    let r = idx_opts.max_degree;
    let VectorOptions { dims, .. } = options.vector;

    let vertexs: Vec<RwLock<VertexNeighbor>> = (0..n).map(|_| {
        RwLock::new(VertexNeighbor { neighbors: Vec::new() })
    }).collect();

    let medoid = 0;

    let mut new_vamana = DiskANNRam::<S> {
        raw,
        vertexs,
        medoid,
        dims,
        max_degree: idx_opts.max_degree,
        alpha: idx_opts.alpha,
        l_build: idx_opts.l_build,
    };

    // 1. init graph with r random neighbors for each node
    let rng = rand::thread_rng();
    new_vamana._init_graph(n, rng.clone());

    // 2. find medoid
    new_vamana.medoid = new_vamana._find_medoid(n);

    // 3. iterate pass
    new_vamana._one_pass(n, 1.0, r, idx_opts.l_build, rng.clone());

    new_vamana._one_pass(n, idx_opts.alpha, r, idx_opts.max_degree, rng.clone());
    
    new_vamana
}

pub fn save<S: G>(ram: DiskANNRam<S>, path: PathBuf) -> DiskANNMmap<S> {

    let neighbors_iter = ram.vertexs.iter()
        .flat_map(|vertex| {
            let vertex = vertex.read();
            vertex.neighbors.iter().cloned()
        });

    // Create the neighbors array using MmapArray::create.
    let neighbors = MmapArray::create(path.join("neighbors"), neighbors_iter);

    // Create an iterator for the size of each neighbor list.
    let neighbor_offset_iter = { 
        let iter = ram.vertexs.iter()
        .map(|vertex| {
            let vertex = vertex.read();
            vertex.neighbors.len()
        });
        calculate_offsets(iter)
    };

    // Create the neighbor_size array using MmapArray::create.
    let neighbor_offset = MmapArray::create(path.join("neighbor_offset"), neighbor_offset_iter);

    let medoid_vec = vec![ram.medoid];
    let medoid = MmapArray::create(path.join("medoid"), medoid_vec.into_iter());

    DiskANNMmap{
        raw: ram.raw,
        neighbors,
        neighbor_offset,
        medoid,
        r: ram.max_degree,
        alpha: ram.alpha,
        l: ram.l_build
    }
}

pub fn load<S: G>(path: PathBuf, options: IndexOptions) -> DiskANNMmap<S> {
    let idx_opts = options.indexing.clone().unwrap_diskann();
    let raw = Arc::new(Raw::open(path.join("raw"), options.clone()));
    let neighbors = MmapArray::open(path.join("neighbors"));
    let neighbor_offset = MmapArray::open(path.join("neighbor_offset"));
    let medoid = MmapArray::open(path.join("medoid"));
    assert!(medoid.len() == 1);
    
    DiskANNMmap{
        raw,
        neighbors,
        neighbor_offset,
        medoid,
        r: idx_opts.max_degree,
        alpha: idx_opts.alpha,
        l: idx_opts.l_build
    }
}

pub fn basic<S: G>(
    mmap: &DiskANNMmap<S>,
    vector: &[S::Scalar],
    k: u32,
    mut filter: impl Filter,
) -> BinaryHeap<Reverse<Element>> {
    let mut state = SearchState::new(k as usize, mmap.l as usize);

    let start = mmap.medoid[0];
    let dist = S::distance(vector, mmap.raw.vector(start));
    state.push(start, dist);
    while let Some(id) = state.pop() {
        // only pop id in the search list but not visited
        state.visit(id);
        {
            let neighbor_ids = mmap._get_neighbors(id);
            for neighbor_id in neighbor_ids {
                if state.is_visited(neighbor_id) {
                    continue;
                }

                let payload = mmap.raw.payload(neighbor_id);

                if filter.check(payload) {
                    let dist = S::distance(vector, mmap.raw.vector(neighbor_id));
                    state.push(neighbor_id, dist); // push and retain closet l nodes
                }
            }
        }
    }

    let mut results = ElementHeap::new(k as usize);
    for (distance, id) in state.candidates {
        results.push(Element {
            distance: distance,
            payload: mmap.raw.payload(id),
        });
    }
    results.into_reversed_heap()
}
