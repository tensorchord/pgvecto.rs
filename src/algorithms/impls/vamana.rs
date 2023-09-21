use crate::algorithms::vamana::VamanaError;
use crate::bgworker::storage::Storage;
use crate::bgworker::storage::StoragePreallocator;
use crate::bgworker::storage_mmap::MmapBox;
use crate::bgworker::vectors::Vectors;
use crate::prelude::*;

use parking_lot::RwLock;
use parking_lot::RwLockReadGuard;
use parking_lot::RwLockWriteGuard;
use crossbeam::atomic::AtomicCell;
use rand::distributions::Uniform;
use rand::prelude::SliceRandom;
use rand::Rng;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashSet};
use std::marker::PhantomData;
use std::sync::Arc;

pub struct VertexWithDistance {
    pub id: usize,
    pub distance: Scalar,
}

impl VertexWithDistance {
    pub fn new(id: usize, distance: Scalar) -> Self {
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
    pub visited: HashSet<usize>,
    candidates: BTreeMap<Scalar, usize>,
    heap: BinaryHeap<Reverse<VertexWithDistance>>,
    heap_visited: HashSet<usize>,
    l: usize,
    /// Number of results to return.
    //TODO: used during search.
    #[allow(dead_code)]
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
    fn pop(&mut self) -> Option<usize> {
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
    fn push(&mut self, vertex_id: usize, distance: Scalar) {
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
    fn visit(&mut self, vertex_id: usize) {
        self.visited.insert(vertex_id);
    }

    // Returns true if the vertex has been visited.
    fn is_visited(&self, vertex_id: usize) -> bool {
        self.visited.contains(&vertex_id) || self.heap_visited.contains(&vertex_id)
    }
}

#[allow(unused)]
pub struct VamanaImpl<D: DistanceFamily> {
    /// neighbors[vertex_id*r..(vertex_id+1)*r] records r neighbors for each vertex
    neighbors: MmapBox<[AtomicCell<usize>]>,

    /// neighbor_size[vertex_id] records the actual number of neighbors for each vertex
    /// the RwLock is for protecting both the data for size and original data
    neighbor_size: MmapBox<[RwLock<usize>]>,

    /// the entry for the entire graph, the closet vector to centroid
    medoid: MmapBox<usize>,

    vectors: Arc<Vectors>,
    dims: u16,
    r: usize,
    alpha: f32,
    l: usize,
    build_threads: usize,
    _maker: PhantomData<D>,
}

unsafe impl<D: DistanceFamily> Send for VamanaImpl<D> {}
unsafe impl<D: DistanceFamily> Sync for VamanaImpl<D> {}

impl<D: DistanceFamily> VamanaImpl<D> {
    pub fn prebuild(
        storage: &mut StoragePreallocator,
        capacity: usize,
        r: usize,
        memmap: Memmap,
    ) -> Result<(), VamanaError> {
        let number_of_nodes = capacity;
        storage.palloc_mmap_slice::<AtomicCell<usize>>(memmap, r * number_of_nodes);
        storage.palloc_mmap_slice::<RwLock<usize>>(memmap, number_of_nodes);
        storage.palloc_mmap::<usize>(memmap);
        Ok(())
    }

    pub fn new(
        storage: &mut Storage,
        vectors: Arc<Vectors>,
        n: usize,
        capacity: usize,
        dims: u16,
        r: usize,
        alpha: f32,
        l: usize,
        build_threads: usize,
        memmap: Memmap,
    ) -> Result<Self, VamanaError> {
        let number_of_nodes = capacity;
        let neighbors = unsafe {
            storage
                .alloc_mmap_slice::<AtomicCell<usize>>(memmap, r * number_of_nodes)
                .assume_init()
        };
        let neighbor_size = unsafe {
            storage
                .alloc_mmap_slice::<RwLock<usize>>(memmap, number_of_nodes)
                .assume_init()
        };
        let medoid = unsafe {
            storage
                .alloc_mmap::<usize>(memmap)
                .assume_init()
        };

        let mut new_vamana = Self {
            neighbors,
            neighbor_size,
            medoid,
            vectors: vectors.clone(),
            dims,
            r,
            alpha,
            l,
            build_threads,
            _maker: PhantomData,
        };

        // 1. init graph with r random neighbors for each node
        let rng = rand::thread_rng();
        new_vamana._init_graph(n, rng.clone());

        // 2. find medoid
        *new_vamana.medoid = new_vamana._find_medoid(n);

        // 3. iterate pass
        new_vamana._one_pass(n, 1.0, r, l, rng.clone())?;

        new_vamana._one_pass(n, alpha, r, l, rng.clone())?;

        Ok(new_vamana)
    }

    pub fn load(
        storage: &mut Storage,
        vectors: Arc<Vectors>,
        capacity: usize,
        dims: u16,
        r: usize,
        alpha: f32,
        l: usize,
        build_threads: usize,
        memmap: Memmap,
    ) -> Result<Self, VamanaError> {
        let number_of_nodes = capacity;
        let neighbors = unsafe {
            storage
                .alloc_mmap_slice::<AtomicCell<usize>>(memmap, r * number_of_nodes)
                .assume_init()
        };
        let neighbor_size = unsafe {
            storage
                .alloc_mmap_slice::<RwLock<usize>>(memmap, number_of_nodes)
                .assume_init()
        };
        let medoid = unsafe {
            storage
                .alloc_mmap::<usize>(memmap)
                .assume_init()
        };
        Ok(Self {
            neighbors,
            neighbor_size,
            medoid,
            vectors,
            dims,
            r,
            alpha,
            l,
            build_threads,
            _maker: PhantomData,
        })
    }

    #[allow(unused)]
    pub fn search<F>(
        &self,
        target: Box<[Scalar]>,
        k: usize,
        filter: F,
    ) -> Result<Vec<(Scalar, u64)>, VamanaError>
    where
        F: FnMut(u64) -> bool,
    {
        // TODO: filter
        let state = self._greedy_search(0, &target, k, k * 2)?;

        let mut results = BinaryHeap::<(Scalar, usize)>::new();
        for (distance, row) in state.candidates {
            if results.len() == k {
                break;
            }

            results.push((Scalar::from(distance), row));
        }
        let res_vec: Vec<(Scalar, u64)> = results
            .iter()
            .map(|x| (x.0, self.vectors.get_data(x.1)))
            .collect();
        Ok(res_vec)
    }

    #[allow(unused)]
    pub fn insert(&self, x: usize) -> Result<(), VamanaError> {
        assert!(self.vectors.len()>x);

        // init random edges
        let distribution = Uniform::new(0, self.vectors.len());
        let mut rng = rand::thread_rng();
        let mut neighbor_ids: HashSet<usize> = HashSet::new();
        while neighbor_ids.len() < self.r {
            let neighbor_id = rng.sample(distribution);
            if neighbor_id != x {
                neighbor_ids.insert(neighbor_id);
            }
        }
        {
            let mut guard = self.neighbor_size[x].write();
            self._set_neighbors(x, &neighbor_ids, &mut guard);
        }

        // search and prune
        self.search_and_prune_for_one_vertex(x, self.alpha, self.r, self.l)?;
        
        Ok(())
    }

    fn _init_graph(&self, n: usize, mut rng: impl Rng) {
        let distribution = Uniform::new(0, n);
        for i in 0..n{
            let mut neighbor_ids: HashSet<usize> = HashSet::new();
            while neighbor_ids.len() < self.r {
                let neighbor_id = rng.sample(distribution);
                if neighbor_id != i {
                    neighbor_ids.insert(neighbor_id);
                }
            }

            {
                let mut guard = self.neighbor_size[i].write();
                self._set_neighbors(i, &neighbor_ids, &mut guard);
            }
        }
    }

    fn _set_neighbors(&self, vertex_index: usize, neighbor_ids: &HashSet<usize>, guard: &mut RwLockWriteGuard<usize>) {
        assert!(neighbor_ids.len() <= self.r);
        let mut i = 0;
        for item in neighbor_ids {
            self.neighbors[vertex_index * self.r + i].store(*item);
            i += 1;
        }
        **guard = neighbor_ids.len();
    }

    fn _get_neighbors(&self, vertex_index: usize, guard: &RwLockReadGuard<usize>) -> &[AtomicCell<usize>] {
        //TODO: store neighbor length
        let size = **guard;
        &self.neighbors[(vertex_index * self.r)..(vertex_index * self.r + size)]
    }

    fn _get_neighbors_with_write_guard(&self, vertex_index: usize, guard: &RwLockWriteGuard<usize>) -> &[AtomicCell<usize>] {
        let size = **guard;
        &self.neighbors[(vertex_index * self.r)..(vertex_index * self.r + size)]
    }

    fn _find_medoid(&self, n: usize) -> usize {
        let centroid = self._compute_centroid(n);
        let centroid_arr: &[Scalar] = &centroid;

        let mut medoid_index = 0;
        let mut min_dis = Scalar::INFINITY;
        for i in 0..n {
            let dis = D::distance(centroid_arr, self.vectors.get_vector(i));
            if dis < min_dis {
                min_dis = dis;
                medoid_index = i;
            }
        }
        medoid_index
    }

    fn _compute_centroid(&self, n: usize) -> Vec<Scalar> {
        let dim = self.dims as usize;
        let mut sum = vec![0_f64; dim]; // change to f32 to avoid overflow
        for i in 0..n {
            let vec = self.vectors.get_vector(i);
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
    fn _one_pass(
        &self,
        n: usize,
        alpha: f32,
        r: usize,
        l: usize,
        mut rng: impl Rng,
    ) -> Result<(), VamanaError> {
        let mut ids = (0..n).collect::<Vec<_>>();
        ids.shuffle(&mut rng);

        for &id in ids.iter() {
            self.search_and_prune_for_one_vertex(id, alpha, r, l)?;
        }

        Ok(())
    }

    #[warn(unused_assignments)]
    fn search_and_prune_for_one_vertex(
        &self,
        id: usize,
        alpha: f32,
        r: usize,
        l: usize
    ) -> Result<(), VamanaError> {
        let query = self.vectors.get_vector(id);
        let mut state = self._greedy_search(*self.medoid, query, 1, l)?;
        state.visited.remove(&id); // in case visited has id itself
        let mut new_neighbor_ids: HashSet<usize> = HashSet::new();
        {
            let mut guard = self.neighbor_size[id].write();
            let neighbor_ids = self._get_neighbors_with_write_guard(id, &guard);
            state.visited.extend(neighbor_ids.iter().map(|x| x.load()));
            let neighbor_ids = self._robust_prune(id, state.visited, alpha, l)?;
            let neighbor_ids: HashSet<usize> = neighbor_ids.into_iter().collect();
            self._set_neighbors(id, &neighbor_ids, &mut guard);
            new_neighbor_ids = neighbor_ids;
        }

        for &neighbor_id in new_neighbor_ids.iter() {
            {
                let mut guard = self.neighbor_size[neighbor_id].write();
                let old_neighbors = self._get_neighbors_with_write_guard(neighbor_id, &guard);
                let mut old_neighbors: HashSet<usize> = old_neighbors.into_iter().map(|x| x.load()).collect();
                old_neighbors.insert(id);
                if old_neighbors.len() > r {
                    // need robust prune
                    let new_neighbors = self._robust_prune(neighbor_id, old_neighbors, alpha, r)?;
                    let new_neighbors: HashSet<usize> = new_neighbors.into_iter().collect();
                    self._set_neighbors(neighbor_id, &new_neighbors, &mut guard);
                } else {
                    self._set_neighbors(neighbor_id, &old_neighbors, &mut guard);
                }
            }
        }
        Ok(())
    }

    fn _greedy_search(
        &self,
        start: usize,
        query: &[Scalar],
        k: usize,
        search_size: usize,
    ) -> Result<SearchState, VamanaError> {
        let mut state = SearchState::new(k, search_size);

        let dist = D::distance(query, self.vectors.get_vector(start));
        state.push(start, dist);
        while let Some(id) = state.pop() {
            // only pop id in the search list but not visited
            state.visit(id);
            {
                let guard = self.neighbor_size[id].read();
                let neighbor_ids = self._get_neighbors(id, &guard);
                for neighbor_id in neighbor_ids {
                    let neighbor_id = neighbor_id.load();
                    if state.is_visited(neighbor_id) {
                        continue;
                    }
    
                    let dist = D::distance(query, self.vectors.get_vector(neighbor_id));
                    state.push(neighbor_id, dist); // push and retain closet l nodes
                }
            }
        }

        Ok(state)
    }

    fn _robust_prune(
        &self,
        id: usize,
        mut visited: HashSet<usize>,
        alpha: f32,
        r: usize,
    ) -> Result<Vec<usize>, VamanaError> {
        let mut heap: BinaryHeap<VertexWithDistance> = visited
            .iter()
            .map(|v| {
                let dist = D::distance(self.vectors.get_vector(id), self.vectors.get_vector(*v));
                VertexWithDistance {
                    id: *v,
                    distance: dist,
                }
            })
            .collect();

        let mut new_neighbor_ids: Vec<usize> = vec![];
        while !visited.is_empty() {
            let mut p = heap.pop().unwrap();
            while !visited.contains(&p.id) {
                p = heap.pop().unwrap();
            }

            new_neighbor_ids.push(p.id);
            if new_neighbor_ids.len() >= r {
                break;
            }
            let mut to_remove: HashSet<usize> = HashSet::new();
            for pv in visited.iter() {
                let dist_prime =
                    D::distance(self.vectors.get_vector(p.id), self.vectors.get_vector(*pv));
                let dist_query =
                    D::distance(self.vectors.get_vector(id), self.vectors.get_vector(*pv));
                if Scalar::from(alpha) * dist_prime <= dist_query {
                    to_remove.insert(*pv);
                }
            }
            for pv in to_remove.iter() {
                visited.remove(pv);
            }
        }
        Ok(new_neighbor_ids)
    }
}
