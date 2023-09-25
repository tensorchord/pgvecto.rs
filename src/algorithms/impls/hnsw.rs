use crate::algorithms::hnsw::HnswError;
use crate::algorithms::quantization::{Quan, Quantization};
use crate::algorithms::utils::filtered_fixed_heap::FilteredFixedHeap;
use crate::algorithms::utils::semaphore::Semaphore;
use crate::algorithms::HnswOptions;
use crate::bgworker::index::IndexOptions;
use crate::bgworker::storage::Storage;
use crate::bgworker::storage::StoragePreallocator;
use crate::bgworker::storage_mmap::MmapBox;
use crate::bgworker::vectors::Vectors;
use crate::prelude::*;
use parking_lot::RwLock;
use parking_lot::RwLockReadGuard;
use parking_lot::RwLockWriteGuard;
use rand::Rng;
use std::cell::UnsafeCell;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::ops::RangeInclusive;
use std::sync::Arc;

#[derive(Debug, Clone, Copy)]
struct VertexIndexer {
    offset: usize,
    capacity: usize,
}

#[derive(Debug, Clone, Copy)]
struct EdgesIndexer {
    offset: usize,
    capacity: usize,
    len: usize,
}

pub struct HnswImpl {
    indexers: MmapBox<[VertexIndexer]>,
    vertexs: MmapBox<[RwLock<EdgesIndexer>]>,
    edges: MmapBox<[UnsafeCell<(Scalar, usize)>]>,
    entry: MmapBox<RwLock<Option<usize>>>,
    vectors: Arc<Vectors>,
    dims: u16,
    m: usize,
    ef_construction: usize,
    visited: Semaphore<Visited>,
    quantization: Quantization,
    d: Distance,
}

unsafe impl Send for HnswImpl {}
unsafe impl Sync for HnswImpl {}

impl HnswImpl {
    pub fn prebuild(
        storage: &mut StoragePreallocator,
        capacity: usize,
        m: usize,
        memmap: Memmap,
        index_options: IndexOptions,
        hnsw_options: HnswOptions,
    ) -> Result<(), HnswError> {
        let len_indexers = capacity;
        let len_vertexs = capacity * 2;
        let len_edges = capacity * 2 * (2 * m);
        storage.palloc_mmap_slice::<VertexIndexer>(memmap, len_indexers);
        storage.palloc_mmap_slice::<RwLock<EdgesIndexer>>(memmap, len_vertexs);
        storage.palloc_mmap_slice::<UnsafeCell<(Scalar, usize)>>(memmap, len_edges);
        storage.palloc_mmap::<RwLock<Option<usize>>>(memmap);
        Quantization::prebuild(storage, index_options, hnsw_options.quantization);
        Ok(())
    }
    pub fn new(
        storage: &mut Storage,
        vectors: Arc<Vectors>,
        dims: u16,
        capacity: usize,
        max_threads: usize,
        m: usize,
        ef_construction: usize,
        memmap: Memmap,
        distance: Distance,
        index_options: IndexOptions,
        hnsw_options: HnswOptions,
    ) -> Result<Self, HnswError> {
        let len_indexers = capacity;
        let len_vertexs = capacity * 2;
        let len_edges = capacity * 2 * (2 * m);
        let mut indexers = unsafe {
            storage
                .alloc_mmap_slice::<VertexIndexer>(memmap, len_indexers)
                .assume_init()
        };
        let mut vertexs = unsafe {
            storage
                .alloc_mmap_slice::<RwLock<EdgesIndexer>>(memmap, len_vertexs)
                .assume_init()
        };
        let edges = unsafe {
            storage
                .alloc_mmap_slice::<UnsafeCell<(Scalar, usize)>>(memmap, len_edges)
                .assume_init()
        };
        let entry = unsafe {
            let mut entry = storage.alloc_mmap::<RwLock<Option<usize>>>(memmap);
            entry.write(RwLock::new(None));
            entry.assume_init()
        };
        {
            let mut offset_vertexs = 0usize;
            let mut offset_edges = 0usize;
            for i in 0..capacity {
                let levels = generate_random_levels(m, 63);
                let capacity_vertexs = levels as usize + 1;
                for j in 0..=levels {
                    let capacity_edges = size_of_a_layer(m, j);
                    vertexs[offset_vertexs + j as usize] = RwLock::new(EdgesIndexer {
                        offset: offset_edges,
                        capacity: capacity_edges,
                        len: 0,
                    });
                    offset_edges += capacity_edges;
                }
                indexers[i] = VertexIndexer {
                    offset: offset_vertexs,
                    capacity: capacity_vertexs,
                };
                offset_vertexs += capacity_vertexs;
            }
        }
        let quantization = Quantization::build(
            storage,
            index_options,
            hnsw_options.quantization,
            vectors.clone(),
        );
        Ok(Self {
            indexers,
            vertexs,
            edges,
            entry,
            vectors,
            dims,
            visited: {
                let semaphore = Semaphore::<Visited>::new();
                for _ in 0..max_threads {
                    semaphore.push(Visited::new(capacity));
                }
                semaphore
            },
            m,
            ef_construction,
            quantization,
            d: distance,
        })
    }
    pub fn load(
        storage: &mut Storage,
        vectors: Arc<Vectors>,
        dims: u16,
        capacity: usize,
        max_threads: usize,
        m: usize,
        ef_construction: usize,
        memmap: Memmap,
        distance: Distance,
        index_options: IndexOptions,
        hnsw_options: HnswOptions,
    ) -> Result<Self, HnswError> {
        let len_indexers = capacity;
        let len_vertexs = capacity * 2;
        let len_edges = capacity * 2 * (2 * m);
        let quantization = Quantization::load(
            storage,
            index_options,
            hnsw_options.quantization,
            vectors.clone(),
        );
        Ok(Self {
            indexers: unsafe { storage.alloc_mmap_slice(memmap, len_indexers).assume_init() },
            vertexs: unsafe { storage.alloc_mmap_slice(memmap, len_vertexs).assume_init() },
            edges: unsafe { storage.alloc_mmap_slice(memmap, len_edges).assume_init() },
            entry: unsafe { storage.alloc_mmap(memmap).assume_init() },
            vectors,
            dims,
            m,
            ef_construction,
            visited: {
                let semaphore = Semaphore::<Visited>::new();
                for _ in 0..max_threads {
                    semaphore.push(Visited::new(capacity));
                }
                semaphore
            },
            d: distance,
            quantization,
        })
    }
    pub fn search<F>(
        &self,
        target: Box<[Scalar]>,
        k: usize,
        filter: F,
    ) -> Result<Vec<(Scalar, u64)>, HnswError>
    where
        F: FnMut(u64) -> bool,
    {
        assert!(target.len() == self.dims as usize);
        let entry = *self.entry.read();
        let Some(u) = entry else {
            return Ok(Vec::new());
        };
        let top = self._levels(u);
        let u = self._go(1..=top, u, &target);
        let mut visited = self.visited.acquire();
        let result = self._filtered_search(&mut visited, &target, u, k, 0, filter);
        Ok(result)
    }
    pub fn insert(&self, x: usize) -> Result<(), HnswError> {
        let mut visited = self.visited.acquire();
        self._insert(&mut visited, x)
    }
    fn _vertex(&self, i: usize) -> &[RwLock<EdgesIndexer>] {
        let VertexIndexer { offset, capacity } = self.indexers[i];
        &self.vertexs[offset..][..capacity]
    }
    fn _edges<'a>(&self, guard: &'a RwLockReadGuard<EdgesIndexer>) -> &'a [(Scalar, usize)] {
        unsafe {
            let raw = self.edges[guard.offset..][..guard.len].as_ptr();
            std::slice::from_raw_parts(raw.cast(), guard.len)
        }
    }
    #[allow(clippy::needless_pass_by_ref_mut)]
    fn _edges_mut<'a>(
        &self,
        guard: &'a mut RwLockWriteGuard<EdgesIndexer>,
    ) -> &'a mut [(Scalar, usize)] {
        unsafe {
            let raw = self.edges[guard.offset..][..guard.len].as_ptr();
            std::slice::from_raw_parts_mut(raw.cast_mut().cast(), guard.len)
        }
    }
    fn _edges_clear(&self, guard: &mut RwLockWriteGuard<EdgesIndexer>) {
        guard.len = 0;
    }
    fn _edges_append(&self, guard: &mut RwLockWriteGuard<EdgesIndexer>, data: (Scalar, usize)) {
        if guard.capacity == guard.len {
            panic!("Array is full. The capacity is {}.", guard.capacity);
        }
        unsafe {
            self.edges[guard.offset + guard.len].get().write(data);
        }
        guard.len += 1;
    }
    fn _go(&self, levels: RangeInclusive<u8>, u: usize, target: &[Scalar]) -> usize {
        let mut u = u;
        let mut u_dis = self._dist0(u, target);
        for i in levels.rev() {
            let mut changed = true;
            while changed {
                changed = false;
                let guard = self._vertex(u)[i as usize].read();
                for &(_, v) in self._edges(&guard).iter() {
                    let v_dis = self._dist0(v, target);
                    if v_dis < u_dis {
                        u = v;
                        u_dis = v_dis;
                        changed = true;
                    }
                }
            }
        }
        u
    }
    fn _insert(&self, visited: &mut Visited, id: usize) -> Result<(), HnswError> {
        let target = self.vectors.get_vector(id);
        self.quantization.insert(id, target)?;
        let levels = self._levels(id);
        let entry;
        let lock = {
            let cond = move |global: Option<usize>| {
                if let Some(u) = global {
                    self._levels(u) < levels
                } else {
                    true
                }
            };
            let lock = self.entry.read();
            if cond(*lock) {
                drop(lock);
                let lock = self.entry.write();
                entry = *lock;
                if cond(*lock) {
                    Some(lock)
                } else {
                    None
                }
            } else {
                entry = *lock;
                None
            }
        };
        let Some(mut u) = entry else {
            if let Some(mut lock) = lock {
                *lock = Some(id);
            }
            return Ok(());
        };
        let top = self._levels(u);
        if top > levels {
            u = self._go(levels + 1..=top, u, target);
        }
        let mut layers = Vec::with_capacity(1 + levels as usize);
        for i in (0..=std::cmp::min(levels, top)).rev() {
            let mut edges = self._search(visited, target, u, self.ef_construction, i);
            edges.sort();
            edges = self._select(edges, size_of_a_layer(self.m, i))?;
            u = edges.first().unwrap().1;
            layers.push(edges);
        }
        layers.reverse();
        layers.resize_with(1 + levels as usize, Vec::new);
        let backup = layers.iter().map(|x| x.to_vec()).collect::<Vec<_>>();
        for i in 0..=levels {
            let mut guard = self._vertex(id)[i as usize].write();
            let edges = layers[i as usize].as_slice();
            self._edges_clear(&mut guard);
            for &edge in edges {
                self._edges_append(&mut guard, edge);
            }
        }
        for (i, layer) in backup.into_iter().enumerate() {
            let i = i as u8;
            for (n_dis, n) in layer.iter().copied() {
                let mut guard = self._vertex(n)[i as usize].write();
                let element = (n_dis, id);
                let mut edges = self._edges_mut(&mut guard).to_vec();
                let (Ok(index) | Err(index)) = edges.binary_search(&element);
                edges.insert(index, element);
                edges = self._select(edges, size_of_a_layer(self.m, i))?;
                self._edges_clear(&mut guard);
                for &edge in edges.iter() {
                    self._edges_append(&mut guard, edge);
                }
            }
        }
        if let Some(mut lock) = lock {
            *lock = Some(id);
        }
        Ok(())
    }
    fn _select(
        &self,
        input: Vec<(Scalar, usize)>,
        size: usize,
    ) -> Result<Vec<(Scalar, usize)>, HnswError> {
        if input.len() <= size {
            return Ok(input);
        }
        let mut res = Vec::new();
        for (u_dis, u) in input.iter().copied() {
            if res.len() == size {
                break;
            }
            let check = res
                .iter()
                .map(|&(_, v)| self._dist1(u, v))
                .all(|dist| dist > u_dis);
            if check {
                res.push((u_dis, u));
            }
        }
        Ok(res)
    }
    fn _search(
        &self,
        visited: &mut Visited,
        target: &[Scalar],
        s: usize,
        k: usize,
        i: u8,
    ) -> Vec<(Scalar, usize)> {
        assert!(k > 0);
        let mut bound = Scalar::INFINITY;
        let mut visited = visited.new_version();
        let mut candidates = BinaryHeap::<Reverse<(Scalar, usize)>>::new();
        let mut results = BinaryHeap::<(Scalar, usize)>::new();
        let s_dis = self._dist0(s, target);
        visited.set(s);
        candidates.push(Reverse((s_dis, s)));
        results.push((s_dis, s));
        if results.len() == k + 1 {
            results.pop();
        }
        if results.len() == k {
            bound = results.peek().unwrap().0;
        }
        while let Some(Reverse((u_dis, u))) = candidates.pop() {
            if u_dis > bound {
                break;
            }
            let guard = self._vertex(u)[i as usize].read();
            for &(_, v) in self._edges(&guard).iter() {
                if visited.test(v) {
                    continue;
                }
                visited.set(v);
                let v_dis = self._dist0(v, target);
                if v_dis > bound {
                    continue;
                }
                candidates.push(Reverse((v_dis, v)));
                results.push((v_dis, v));
                if results.len() == k + 1 {
                    results.pop();
                }
                if results.len() == k {
                    bound = results.peek().unwrap().0;
                }
            }
        }
        results.into_vec()
    }
    fn _filtered_search<F>(
        &self,
        visited: &mut Visited,
        target: &[Scalar],
        s: usize,
        k: usize,
        i: u8,
        filter: F,
    ) -> Vec<(Scalar, u64)>
    where
        F: FnMut(u64) -> bool,
    {
        assert!(k > 0);
        let mut visited = visited.new_version();
        let mut candidates = BinaryHeap::<Reverse<(Scalar, usize)>>::new();
        let mut results = FilteredFixedHeap::new(k, filter);
        let s_dis = self._dist0(s, target);
        visited.set(s);
        candidates.push(Reverse((s_dis, s)));
        results.push((s_dis, self.vectors.get_data(s)));
        while let Some(Reverse((u_dis, u))) = candidates.pop() {
            if u_dis > results.bound() {
                break;
            }
            let guard = self._vertex(u)[i as usize].read();
            for &(_, v) in self._edges(&guard).iter() {
                if visited.test(v) {
                    continue;
                }
                visited.set(v);
                let v_dis = self._dist0(v, target);
                if v_dis > results.bound() {
                    continue;
                }
                candidates.push(Reverse((v_dis, v)));
                results.push((v_dis, self.vectors.get_data(v)));
            }
        }
        results.into_sorted_vec()
    }
    fn _dist0(&self, u: usize, target: &[Scalar]) -> Scalar {
        self.quantization.distance(self.d, target, u)
    }
    fn _dist1(&self, u: usize, v: usize) -> Scalar {
        let u = self.vectors.get_vector(u);
        let v = self.vectors.get_vector(v);
        self.d.distance(u, v)
    }
    fn _levels(&self, u: usize) -> u8 {
        self._vertex(u).len() as u8 - 1
    }
}

fn generate_random_levels(m: usize, max_level: usize) -> u8 {
    let factor = 1.0 / (m as f64).ln();
    let mut rng = rand::thread_rng();
    let x = -rng.gen_range(0.0f64..1.0).ln() * factor;
    x.round().min(max_level as f64) as u8
}

fn size_of_a_layer(m: usize, i: u8) -> usize {
    if i == 0 {
        m * 2
    } else {
        m
    }
}

pub struct Visited {
    version: usize,
    data: Box<[usize]>,
}

impl Visited {
    pub fn new(capacity: usize) -> Self {
        Self {
            version: 0,
            data: unsafe { Box::new_zeroed_slice(capacity).assume_init() },
        }
    }
    pub fn new_version(&mut self) -> VisitedVersion<'_> {
        assert_ne!(self.version, usize::MAX);
        self.version += 1;
        VisitedVersion { inner: self }
    }
}

pub struct VisitedVersion<'a> {
    inner: &'a mut Visited,
}

impl<'a> VisitedVersion<'a> {
    pub fn test(&mut self, i: usize) -> bool {
        self.inner.data[i] == self.inner.version
    }
    pub fn set(&mut self, i: usize) {
        self.inner.data[i] = self.inner.version;
    }
}
