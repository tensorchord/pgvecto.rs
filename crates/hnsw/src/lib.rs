#![feature(trait_alias)]
#![allow(clippy::len_without_is_empty)]

pub mod visited;

use base::index::*;
use base::operator::*;
use base::scalar::F32;
use base::search::*;
use bytemuck::{Pod, Zeroable};
use common::dir_ops::sync_dir;
use common::mmap_array::MmapArray;
use parking_lot::{RwLock, RwLockWriteGuard};
use quantization::operator::OperatorQuantization;
use quantization::Quantization;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::create_dir;
use std::ops::RangeInclusive;
use std::path::Path;
use std::sync::Arc;
use storage::operator::OperatorStorage;
use storage::StorageCollection;
use visited::{VisitedGuard, VisitedPool};

pub trait OperatorHnsw = Operator + OperatorQuantization + OperatorStorage;

pub struct Hnsw<O: OperatorHnsw> {
    mmap: HnswMmap<O>,
}

impl<O: OperatorHnsw> Hnsw<O> {
    #[cfg(feature = "stand-alone-test")]
    pub fn new(mmap: HnswMmap<O>) -> Self {
        Self { mmap }
    }

    pub fn create<S: Source<O>>(path: &Path, options: IndexOptions, source: &S) -> Self {
        create_dir(path).unwrap();
        let ram = make(path, options, source);
        let mmap = save(ram, path);
        sync_dir(path);
        Self { mmap }
    }

    pub fn open(path: &Path, options: IndexOptions) -> Self {
        let mmap = open(path, options);
        Self { mmap }
    }

    pub fn basic(
        &self,
        vector: Borrowed<'_, O>,
        opts: &SearchOptions,
        filter: impl Filter,
    ) -> BinaryHeap<Reverse<Element>> {
        basic(&self.mmap, vector, opts.hnsw_ef_search, filter)
    }

    pub fn vbase<'a>(
        &'a self,
        vector: Borrowed<'a, O>,
        opts: &'a SearchOptions,
        filter: impl Filter + 'a,
    ) -> (Vec<Element>, Box<(dyn Iterator<Item = Element> + 'a)>) {
        vbase(&self.mmap, vector, opts.hnsw_ef_search, filter)
    }

    pub fn len(&self) -> u32 {
        self.mmap.storage.len()
    }

    pub fn vector(&self, i: u32) -> Borrowed<'_, O> {
        self.mmap.storage.vector(i)
    }

    pub fn payload(&self, i: u32) -> Payload {
        self.mmap.storage.payload(i)
    }
}

unsafe impl<O: OperatorHnsw> Send for Hnsw<O> {}
unsafe impl<O: OperatorHnsw> Sync for Hnsw<O> {}

pub struct HnswRam<O: OperatorHnsw> {
    storage: Arc<StorageCollection<O>>,
    quantization: Quantization<O, StorageCollection<O>>,
    // ----------------------
    m: u32,
    // ----------------------
    graph: HnswRamGraph,
    // ----------------------
    visited: VisitedPool,
}

impl<O: OperatorHnsw> HnswRam<O> {
    #[cfg(feature = "stand-alone-test")]
    pub fn new(
        storage: Arc<StorageCollection<O>>,
        quantization: Quantization<O, StorageCollection<O>>,
        m: u32,
        graph: HnswRamGraph,
        visited: VisitedPool,
    ) -> Self {
        Self {
            storage,
            quantization,
            m,
            graph,
            visited,
        }
    }
}

pub struct HnswRamGraph {
    pub vertexs: Vec<HnswRamVertex>,
}

pub struct HnswRamVertex {
    pub layers: Vec<RwLock<HnswRamLayer>>,
}

impl HnswRamVertex {
    pub fn levels(&self) -> u8 {
        self.layers.len() as u8 - 1
    }
}

pub struct HnswRamLayer {
    pub edges: Vec<(F32, u32)>,
}

pub struct HnswMmap<O: OperatorHnsw> {
    storage: Arc<StorageCollection<O>>,
    quantization: Quantization<O, StorageCollection<O>>,
    // ----------------------
    m: u32,
    // ----------------------
    edges: MmapArray<HnswMmapEdge>,
    by_layer_id: MmapArray<usize>,
    by_vertex_id: MmapArray<usize>,
    // ----------------------
    visited: VisitedPool,
}

impl<O: OperatorHnsw> HnswMmap<O> {
    #[cfg(feature = "stand-alone-test")]
    pub fn new(
        storage: Arc<StorageCollection<O>>,
        quantization: Quantization<O, StorageCollection<O>>,
        m: u32,
        edges: MmapArray<HnswMmapEdge>,
        by_layer_id: MmapArray<usize>,
        by_vertex_id: MmapArray<usize>,
        visited: VisitedPool,
    ) -> Self {
        Self {
            storage,
            quantization,
            m,
            edges,
            by_layer_id,
            by_vertex_id,
            visited,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct HnswMmapEdge(#[allow(dead_code)] F32, u32);
// we may convert a memory-mapped graph to a memory graph
// so that it speeds merging sealed segments

unsafe impl<O: OperatorHnsw> Send for HnswMmap<O> {}
unsafe impl<O: OperatorHnsw> Sync for HnswMmap<O> {}
unsafe impl Pod for HnswMmapEdge {}
unsafe impl Zeroable for HnswMmapEdge {}

pub fn make<O: OperatorHnsw, S: Source<O>>(
    path: &Path,
    options: IndexOptions,
    source: &S,
) -> HnswRam<O> {
    let HnswIndexingOptions {
        m,
        ef_construction,
        quantization: quantization_opts,
    } = options.indexing.clone().unwrap_hnsw();
    let storage = Arc::new(StorageCollection::create(&path.join("raw"), source));
    rayon::check();
    let quantization = Quantization::create(
        &path.join("quantization"),
        options.clone(),
        quantization_opts,
        &storage,
        (0..storage.len()).collect::<Vec<_>>(),
    );
    rayon::check();
    let n = storage.len();
    let graph = HnswRamGraph {
        vertexs: (0..n)
            .into_par_iter()
            .map(|i| HnswRamVertex {
                layers: (0..count_layers_of_a_vertex(m, i))
                    .map(|_| RwLock::new(HnswRamLayer { edges: Vec::new() }))
                    .collect(),
            })
            .collect(),
    };
    let entry = RwLock::<Option<u32>>::new(None);
    let visited = VisitedPool::new(storage.len());
    (0..n).into_par_iter().for_each(|i| {
        fn fast_search<O: OperatorHnsw>(
            quantization: &Quantization<O, StorageCollection<O>>,
            graph: &HnswRamGraph,
            levels: RangeInclusive<u8>,
            u: u32,
            target: Borrowed<'_, O>,
        ) -> u32 {
            let mut u = u;
            let mut u_dis = quantization.distance(target, u);
            for i in levels.rev() {
                let mut changed = true;
                while changed {
                    changed = false;
                    let guard = graph.vertexs[u as usize].layers[i as usize].read();
                    for &(_, v) in guard.edges.iter() {
                        let v_dis = quantization.distance(target, v);
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
        fn local_search<O: OperatorHnsw>(
            quantization: &Quantization<O, StorageCollection<O>>,
            graph: &HnswRamGraph,
            visited: &mut VisitedGuard,
            vector: Borrowed<'_, O>,
            s: u32,
            k: usize,
            i: u8,
        ) -> Vec<(F32, u32)> {
            let mut visited = visited.fetch();
            let mut candidates = BinaryHeap::<Reverse<(F32, u32)>>::new();
            let mut results = BinaryHeap::new();
            let s_dis = quantization.distance(vector, s);
            visited.mark(s);
            candidates.push(Reverse((s_dis, s)));
            results.push((s_dis, s));
            while let Some(Reverse((u_dis, u))) = candidates.pop() {
                if !(results.len() < k || u_dis < results.peek().unwrap().0) {
                    break;
                }
                for &(_, v) in graph.vertexs[u as usize].layers[i as usize]
                    .read()
                    .edges
                    .iter()
                {
                    if !visited.check(v) {
                        continue;
                    }
                    visited.mark(v);
                    let v_dis = quantization.distance(vector, v);
                    if results.len() < k || v_dis < results.peek().unwrap().0 {
                        candidates.push(Reverse((v_dis, v)));
                        results.push((v_dis, v));
                        if results.len() > k {
                            results.pop();
                        }
                    }
                }
            }
            results.into_sorted_vec()
        }
        fn select<O: OperatorHnsw>(
            quantization: &Quantization<O, StorageCollection<O>>,
            input: &mut Vec<(F32, u32)>,
            size: u32,
        ) {
            if input.len() <= size as usize {
                return;
            }
            let mut res = Vec::new();
            for (u_dis, u) in input.iter().copied() {
                if res.len() == size as usize {
                    break;
                }
                let check = res
                    .iter()
                    .map(|&(_, v)| quantization.distance2(u, v))
                    .all(|dist| dist > u_dis);
                if check {
                    res.push((u_dis, u));
                }
            }
            *input = res;
        }
        rayon::check();
        let mut visited = visited.fetch();
        let target = storage.vector(i);
        let levels = graph.vertexs[i as usize].levels();
        let local_entry;
        let update_entry;
        {
            let check = |global: Option<u32>| {
                if let Some(u) = global {
                    graph.vertexs[u as usize].levels() < levels
                } else {
                    true
                }
            };
            let read = entry.read();
            if check(*read) {
                drop(read);
                let write = entry.write();
                if check(*write) {
                    local_entry = *write;
                    update_entry = Some(write);
                } else {
                    local_entry = *write;
                    update_entry = None;
                }
            } else {
                local_entry = *read;
                update_entry = None;
            }
        };
        let Some(mut u) = local_entry else {
            if let Some(mut write) = update_entry {
                *write = Some(i);
            }
            return;
        };
        let top = graph.vertexs[u as usize].levels();
        if top > levels {
            u = fast_search(&quantization, &graph, levels + 1..=top, u, target);
        }
        let mut result = Vec::with_capacity(1 + std::cmp::min(levels, top) as usize);
        for j in (0..=std::cmp::min(levels, top)).rev() {
            let mut edges = local_search(
                &quantization,
                &graph,
                &mut visited,
                target,
                u,
                ef_construction as usize,
                j,
            );
            edges.sort();
            select(&quantization, &mut edges, count_max_edges_of_a_layer(m, j));
            u = edges.first().unwrap().1;
            result.push(edges);
        }
        for j in 0..=std::cmp::min(levels, top) {
            let mut write = graph.vertexs[i as usize].layers[j as usize].write();
            write.edges = result.pop().unwrap();
            let read = RwLockWriteGuard::downgrade(write);
            for (n_dis, n) in read.edges.iter().copied() {
                let mut write = graph.vertexs[n as usize].layers[j as usize].write();
                let element = (n_dis, i);
                let (Ok(index) | Err(index)) = write.edges.binary_search(&element);
                write.edges.insert(index, element);
                select(
                    &quantization,
                    &mut write.edges,
                    count_max_edges_of_a_layer(m, j),
                );
            }
        }
        if let Some(mut write) = update_entry {
            *write = Some(i);
        }
    });
    HnswRam {
        storage,
        quantization,
        m,
        graph,
        visited,
    }
}

pub fn save<O: OperatorHnsw>(mut ram: HnswRam<O>, path: &Path) -> HnswMmap<O> {
    let edges = MmapArray::create(
        &path.join("edges"),
        ram.graph
            .vertexs
            .iter_mut()
            .flat_map(|v| v.layers.iter_mut())
            .flat_map(|v| &v.get_mut().edges)
            .map(|&(_0, _1)| HnswMmapEdge(_0, _1)),
    );
    rayon::check();
    let by_layer_id = MmapArray::create(&path.join("by_layer_id"), {
        let iter = ram.graph.vertexs.iter_mut();
        let iter = iter.flat_map(|v| v.layers.iter_mut());
        let iter = iter.map(|v| v.get_mut().edges.len());
        caluate_offsets(iter)
    });
    rayon::check();
    let by_vertex_id = MmapArray::create(&path.join("by_vertex_id"), {
        let iter = ram.graph.vertexs.iter_mut();
        let iter = iter.map(|v| v.layers.len());
        caluate_offsets(iter)
    });
    rayon::check();
    HnswMmap {
        storage: ram.storage,
        quantization: ram.quantization,
        m: ram.m,
        edges,
        by_layer_id,
        by_vertex_id,
        visited: ram.visited,
    }
}

pub fn open<O: OperatorHnsw>(path: &Path, options: IndexOptions) -> HnswMmap<O> {
    let idx_opts = options.indexing.clone().unwrap_hnsw();
    let storage = Arc::new(StorageCollection::open(&path.join("raw"), options.clone()));
    let quantization = Quantization::open(
        &path.join("quantization"),
        options.clone(),
        idx_opts.quantization,
        &storage,
    );
    let edges = MmapArray::open(&path.join("edges"));
    let by_layer_id = MmapArray::open(&path.join("by_layer_id"));
    let by_vertex_id = MmapArray::open(&path.join("by_vertex_id"));
    let idx_opts = options.indexing.unwrap_hnsw();
    let n = storage.len();
    HnswMmap {
        storage,
        quantization,
        m: idx_opts.m,
        edges,
        by_layer_id,
        by_vertex_id,
        visited: VisitedPool::new(n),
    }
}

pub fn basic<O: OperatorHnsw>(
    mmap: &HnswMmap<O>,
    vector: Borrowed<'_, O>,
    ef_search: u32,
    filter: impl Filter,
) -> BinaryHeap<Reverse<Element>> {
    let Some(s) = entry(mmap, filter.clone()) else {
        return BinaryHeap::new();
    };
    let levels = count_layers_of_a_vertex(mmap.m, s) - 1;
    let u = fast_search(mmap, 1..=levels, s, vector, filter.clone());
    local_search_basic(mmap, ef_search as usize, u, vector, filter).into_reversed_heap()
}

pub fn vbase<'a, O: OperatorHnsw>(
    mmap: &'a HnswMmap<O>,
    vector: Borrowed<'a, O>,
    ef_search: u32,
    filter: impl Filter + 'a,
) -> (Vec<Element>, Box<(dyn Iterator<Item = Element> + 'a)>) {
    let Some(s) = entry(mmap, filter.clone()) else {
        return (Vec::new(), Box::new(std::iter::empty()));
    };
    let levels = count_layers_of_a_vertex(mmap.m, s) - 1;
    let u = fast_search(mmap, 1..=levels, s, vector, filter.clone());
    let mut iter = local_search_vbase(mmap, u, vector, filter.clone());
    let mut queue = BinaryHeap::<Element>::with_capacity(1 + ef_search as usize);
    let mut stage1 = Vec::new();
    for x in &mut iter {
        if queue.len() == ef_search as usize && queue.peek().unwrap().distance < x.distance {
            stage1.push(x);
            break;
        }
        if queue.len() == ef_search as usize {
            queue.pop();
        }
        queue.push(x);
        stage1.push(x);
    }
    (stage1, Box::new(iter))
}

pub fn entry<O: OperatorHnsw>(mmap: &HnswMmap<O>, mut filter: impl Filter) -> Option<u32> {
    let m = mmap.m;
    let n = mmap.storage.len();
    let mut shift = 1u64;
    let mut count = 0u64;
    while shift * m as u64 <= n as u64 {
        shift *= m as u64;
    }
    while shift != 0 {
        let mut i = 1u64;
        while i * shift <= n as u64 {
            let e = (i * shift - 1) as u32;
            if i % m as u64 != 0 {
                if filter.check(mmap.storage.payload(e)) {
                    return Some(e);
                }
                count += 1;
                if count >= 10000 {
                    return None;
                }
            }
            i += 1;
        }
        shift /= m as u64;
    }
    None
}

pub fn fast_search<O: OperatorHnsw>(
    mmap: &HnswMmap<O>,
    levels: RangeInclusive<u8>,
    u: u32,
    vector: Borrowed<'_, O>,
    mut filter: impl Filter,
) -> u32 {
    let mut u = u;
    let mut u_dis = mmap.quantization.distance(vector, u);
    for i in levels.rev() {
        let mut changed = true;
        while changed {
            changed = false;
            let edges = find_edges(mmap, u, i);
            for &HnswMmapEdge(_, v) in edges.iter() {
                if !filter.check(mmap.storage.payload(v)) {
                    continue;
                }
                let v_dis = mmap.quantization.distance(vector, v);
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

pub fn local_search_basic<O: OperatorHnsw>(
    mmap: &HnswMmap<O>,
    k: usize,
    s: u32,
    vector: Borrowed<'_, O>,
    mut filter: impl Filter,
) -> ElementHeap {
    let mut visited = mmap.visited.fetch();
    let mut visited = visited.fetch();
    let mut candidates = BinaryHeap::<Reverse<(F32, u32)>>::new();
    let mut results = ElementHeap::new(k);
    visited.mark(s);
    let s_dis = mmap.quantization.distance(vector, s);
    candidates.push(Reverse((s_dis, s)));
    results.push(Element {
        distance: s_dis,
        payload: mmap.storage.payload(s),
    });
    while let Some(Reverse((u_dis, u))) = candidates.pop() {
        if !results.check(u_dis) {
            break;
        }
        let edges = find_edges(mmap, u, 0);
        for &HnswMmapEdge(_, v) in edges.iter() {
            if !visited.check(v) {
                continue;
            }
            visited.mark(v);
            if !filter.check(mmap.storage.payload(v)) {
                continue;
            }
            let v_dis = mmap.quantization.distance(vector, v);
            if !results.check(v_dis) {
                continue;
            }
            candidates.push(Reverse((v_dis, v)));
            results.push(Element {
                distance: v_dis,
                payload: mmap.storage.payload(v),
            });
        }
    }
    results
}

pub fn local_search_vbase<'a, O: OperatorHnsw>(
    mmap: &'a HnswMmap<O>,
    s: u32,
    vector: Borrowed<'a, O>,
    mut filter: impl Filter + 'a,
) -> impl Iterator<Item = Element> + 'a {
    let mut visited = mmap.visited.fetch2();
    let mut candidates = BinaryHeap::<Reverse<(F32, u32)>>::new();
    visited.mark(s);
    let s_dis = mmap.quantization.distance(vector, s);
    candidates.push(Reverse((s_dis, s)));
    std::iter::from_fn(move || {
        let Reverse((u_dis, u)) = candidates.pop()?;
        {
            let edges = find_edges(mmap, u, 0);
            for &HnswMmapEdge(_, v) in edges.iter() {
                if !visited.check(v) {
                    continue;
                }
                visited.mark(v);
                if filter.check(mmap.storage.payload(v)) {
                    let v_dis = mmap.quantization.distance(vector, v);
                    candidates.push(Reverse((v_dis, v)));
                }
            }
        }
        Some(Element {
            distance: u_dis,
            payload: mmap.storage.payload(u),
        })
    })
}

pub fn count_layers_of_a_vertex(m: u32, i: u32) -> u8 {
    let mut x = i + 1;
    let mut ans = 1;
    while x % m == 0 {
        ans += 1;
        x /= m;
    }
    ans
}

pub fn count_max_edges_of_a_layer(m: u32, j: u8) -> u32 {
    if j == 0 {
        m * 2
    } else {
        m
    }
}

fn caluate_offsets(iter: impl Iterator<Item = usize>) -> impl Iterator<Item = usize> {
    let mut offset = 0usize;
    let mut iter = std::iter::once(0).chain(iter);
    std::iter::from_fn(move || {
        let x = iter.next()?;
        offset += x;
        Some(offset)
    })
}

fn find_edges<O: OperatorHnsw>(mmap: &HnswMmap<O>, u: u32, level: u8) -> &[HnswMmapEdge] {
    let offset = u as usize;
    let index = mmap.by_vertex_id[offset]..mmap.by_vertex_id[offset + 1];
    let offset = index.start + level as usize;
    let index = mmap.by_layer_id[offset]..mmap.by_layer_id[offset + 1];
    &mmap.edges[index]
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
}
