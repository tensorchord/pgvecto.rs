#![allow(unused)]

use crate::algorithms::raw::Raw;
use crate::prelude::*;
use crossbeam::atomic::AtomicCell;
use parking_lot::RwLock;
use parking_lot::RwLockReadGuard;
use parking_lot::RwLockWriteGuard;
use rand::distributions::Uniform;
use rand::prelude::SliceRandom;
use rand::Rng;
use rayon::prelude::*;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashSet};
use std::sync::Arc;

pub struct VertexWithDistance {
    pub id: u32,
    pub distance: Scalar,
}

impl VertexWithDistance {
    pub fn new(id: u32, distance: Scalar) -> Self {
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

/// DiskANN search state.
pub struct SearchState {
    pub visited: HashSet<u32>,
    candidates: BTreeMap<Scalar, u32>,
    heap: BinaryHeap<Reverse<VertexWithDistance>>,
    heap_visited: HashSet<u32>,
    l: usize,
    /// Number of results to return.
    //TODO: used during search.
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
    fn push(&mut self, vertex_id: u32, distance: Scalar) {
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

pub struct VamanaImpl {
    raw: Arc<Raw>,

    /// neighbors[vertex_id*r..(vertex_id+1)*r] records r neighbors for each vertex
    neighbors: Vec<AtomicCell<u32>>,

    /// neighbor_size[vertex_id] records the actual number of neighbors for each vertex
    /// the RwLock is for protecting both the data for size and original data
    neighbor_size: Vec<RwLock<u32>>,

    /// the entry for the entire graph, the closet vector to centroid
    medoid: u32,

    dims: u16,
    r: u32,
    alpha: f32,
    l: usize,

    d: Distance,
}

unsafe impl Send for VamanaImpl {}
unsafe impl Sync for VamanaImpl {}

impl VamanaImpl {
    pub fn new(
        raw: Arc<Raw>,
        n: u32,
        dims: u16,
        r: u32,
        alpha: f32,
        l: usize,
        d: Distance,
    ) -> Self {
        let neighbors = {
            let mut result = Vec::new();
            result.resize_with(r as usize * n as usize, || AtomicCell::new(0));
            result
        };
        let neighbor_size = unsafe {
            let mut result = Vec::new();
            result.resize_with(n as usize, || RwLock::new(0));
            result
        };
        let medoid = 0;

        let mut new_vamana = Self {
            raw,
            neighbors,
            neighbor_size,
            medoid,
            dims,
            r,
            alpha,
            l,
            d,
        };

        // 1. init graph with r random neighbors for each node
        let rng = rand::thread_rng();
        new_vamana._init_graph(n, rng.clone());

        // 2. find medoid
        new_vamana.medoid = new_vamana._find_medoid(n);

        // 3. iterate pass
        new_vamana._one_pass(n, 1.0, r, l, rng.clone());

        new_vamana._one_pass(n, alpha, r, l, rng.clone());

        new_vamana
    }

    pub fn search<F>(&self, target: Box<[Scalar]>, k: usize, f: F) -> Vec<(Scalar, Payload)>
    where
        F: FnMut(Payload) -> bool,
    {
        // TODO: filter
        let state = self._greedy_search_with_filter(0, &target, k, k * 2, f);

        let mut results = BinaryHeap::<(Scalar, u32)>::new();
        for (distance, row) in state.candidates {
            if results.len() == k {
                break;
            }

            results.push((distance, row));
        }
        let mut res_vec: Vec<(Scalar, Payload)> = results
            .iter()
            .map(|x| (x.0, self.raw.payload(x.1)))
            .collect();
        res_vec.sort();
        res_vec
    }

    fn _greedy_search_with_filter<F>(
        &self,
        start: u32,
        query: &[Scalar],
        k: usize,
        search_size: usize,
        mut f: F,
    ) -> SearchState
    where
        F: FnMut(Payload) -> bool,
    {
        let mut state = SearchState::new(k, search_size);

        let dist = self.d.distance(query, self.raw.vector(start));
        state.push(start, dist);
        while let Some(id) = state.pop() {
            // only pop id in the search list but not visited
            state.visit(id);
            {
                let guard = self.neighbor_size[id as usize].read();
                let neighbor_ids = self._get_neighbors(id, &guard);
                for neighbor_id in neighbor_ids {
                    let neighbor_id = neighbor_id.load();
                    if state.is_visited(neighbor_id) {
                        continue;
                    }

                    if f(self.raw.payload(neighbor_id)) {
                        let dist = self.d.distance(query, self.raw.vector(neighbor_id));
                        state.push(neighbor_id, dist); // push and retain closet l nodes
                    }
                }
            }
        }

        state
    }

    fn _init_graph(&self, n: u32, mut rng: impl Rng) {
        let distribution = Uniform::new(0, n);
        for i in 0..n {
            let mut neighbor_ids: HashSet<u32> = HashSet::new();
            if self.r < n {
                while neighbor_ids.len() < self.r as usize {
                    let neighbor_id = rng.sample(distribution);
                    if neighbor_id != i {
                        neighbor_ids.insert(neighbor_id);
                    }
                }
            } else {
                neighbor_ids = (0..n).collect();
            }

            {
                let mut guard = self.neighbor_size[i as usize].write();
                self._set_neighbors(i, &neighbor_ids, &mut guard);
            }
        }
    }

    fn _set_neighbors(
        &self,
        vertex_index: u32,
        neighbor_ids: &HashSet<u32>,
        guard: &mut RwLockWriteGuard<u32>,
    ) {
        assert!(neighbor_ids.len() <= self.r as usize);
        for (i, item) in neighbor_ids.iter().enumerate() {
            self.neighbors[vertex_index as usize * self.r as usize + i].store(*item);
        }
        **guard = neighbor_ids.len() as u32;
    }

    fn _get_neighbors(
        &self,
        vertex_index: u32,
        guard: &RwLockReadGuard<u32>,
    ) -> &[AtomicCell<u32>] {
        //TODO: store neighbor length
        let size = **guard;
        &self.neighbors[(vertex_index as usize * self.r as usize)
            ..(vertex_index as usize * self.r as usize + size as usize)]
    }

    fn _get_neighbors_with_write_guard(
        &self,
        vertex_index: u32,
        guard: &RwLockWriteGuard<u32>,
    ) -> &[AtomicCell<u32>] {
        let size = **guard;
        &self.neighbors[(vertex_index as usize * self.r as usize)
            ..(vertex_index as usize * self.r as usize + size as usize)]
    }

    fn _find_medoid(&self, n: u32) -> u32 {
        let centroid = self._compute_centroid(n);
        let centroid_arr: &[Scalar] = &centroid;

        let mut medoid_index = 0;
        let mut min_dis = Scalar::INFINITY;
        for i in 0..n {
            let dis = self.d.distance(centroid_arr, self.raw.vector(i));
            if dis < min_dis {
                min_dis = dis;
                medoid_index = i;
            }
        }
        medoid_index
    }

    fn _compute_centroid(&self, n: u32) -> Vec<Scalar> {
        let dim = self.dims as usize;
        let mut sum = vec![0_f64; dim]; // change to f32 to avoid overflow
        for i in 0..n {
            let vec = self.raw.vector(i);
            for j in 0..dim {
                sum[j] += f32::from(vec[j]) as f64;
            }
        }

        let collection: Vec<Scalar> = sum
            .iter()
            .map(|v| Scalar::from((*v / n as f64) as f32))
            .collect();
        collection
    }

    // r and l leave here for multiple pass extension
    fn _one_pass(&self, n: u32, alpha: f32, r: u32, l: usize, mut rng: impl Rng) {
        let mut ids = (0..n).collect::<Vec<_>>();
        ids.shuffle(&mut rng);

        ids.into_par_iter()
            .for_each(|id| self.search_and_prune_for_one_vertex(id, alpha, r, l));
    }

    fn search_and_prune_for_one_vertex(&self, id: u32, alpha: f32, r: u32, l: usize) {
        let query = self.raw.vector(id);
        let mut state = self._greedy_search(self.medoid, query, 1, l);
        state.visited.remove(&id); // in case visited has id itself
        let mut new_neighbor_ids: HashSet<u32> = HashSet::new();
        {
            let mut guard = self.neighbor_size[id as usize].write();
            let neighbor_ids = self._get_neighbors_with_write_guard(id, &guard);
            state.visited.extend(neighbor_ids.iter().map(|x| x.load()));
            let neighbor_ids = self._robust_prune(id, state.visited, alpha, r);
            let neighbor_ids: HashSet<u32> = neighbor_ids.into_iter().collect();
            self._set_neighbors(id, &neighbor_ids, &mut guard);
            new_neighbor_ids = neighbor_ids;
        }

        for &neighbor_id in new_neighbor_ids.iter() {
            {
                let mut guard = self.neighbor_size[neighbor_id as usize].write();
                let old_neighbors = self._get_neighbors_with_write_guard(neighbor_id, &guard);
                let mut old_neighbors: HashSet<u32> =
                    old_neighbors.iter().map(|x| x.load()).collect();
                old_neighbors.insert(id);
                if old_neighbors.len() > r as usize {
                    // need robust prune
                    let new_neighbors = self._robust_prune(neighbor_id, old_neighbors, alpha, r);
                    let new_neighbors: HashSet<u32> = new_neighbors.into_iter().collect();
                    self._set_neighbors(neighbor_id, &new_neighbors, &mut guard);
                } else {
                    self._set_neighbors(neighbor_id, &old_neighbors, &mut guard);
                }
            }
        }
    }

    fn _greedy_search(
        &self,
        start: u32,
        query: &[Scalar],
        k: usize,
        search_size: usize,
    ) -> SearchState {
        let mut state = SearchState::new(k, search_size);

        let dist = self.d.distance(query, self.raw.vector(start));
        state.push(start, dist);
        while let Some(id) = state.pop() {
            // only pop id in the search list but not visited
            state.visit(id);
            {
                let guard = self.neighbor_size[id as usize].read();
                let neighbor_ids = self._get_neighbors(id, &guard);
                for neighbor_id in neighbor_ids {
                    let neighbor_id = neighbor_id.load();
                    if state.is_visited(neighbor_id) {
                        continue;
                    }

                    let dist = self.d.distance(query, self.raw.vector(neighbor_id));
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
                let dist = self.d.distance(self.raw.vector(id), self.raw.vector(*v));
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
                    let dist_prime = self.d.distance(self.raw.vector(p.id), self.raw.vector(*pv));
                    let dist_query = self.d.distance(self.raw.vector(id), self.raw.vector(*pv));
                    if Scalar::from(alpha) * dist_prime <= dist_query {
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
