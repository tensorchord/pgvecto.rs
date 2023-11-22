use super::quantization::Quantization;
use super::raw::Raw;
use crate::index::indexing::hnsw::HnswIndexingOptions;
use crate::index::segments::growing::GrowingSegment;
use crate::index::segments::sealed::SealedSegment;
use crate::index::{IndexOptions, VectorOptions};
use crate::prelude::*;
use crate::utils::dir_ops::sync_dir;
use crate::utils::mmap_array::MmapArray;
use crate::utils::semaphore::Semaphore;
use bytemuck::{Pod, Zeroable};
use parking_lot::{RwLock, RwLockWriteGuard};
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::create_dir;
use std::ops::RangeInclusive;
use std::path::PathBuf;
use std::sync::Arc;

pub struct Hnsw {
    mmap: HnswMmap,
}

impl Hnsw {
    pub fn create(
        path: PathBuf,
        options: IndexOptions,
        sealed: Vec<Arc<SealedSegment>>,
        growing: Vec<Arc<GrowingSegment>>,
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

    pub fn vector(&self, i: u32) -> &[Scalar] {
        self.mmap.raw.vector(i)
    }

    pub fn payload(&self, i: u32) -> Payload {
        self.mmap.raw.payload(i)
    }

    pub fn search<F: FnMut(Payload) -> bool>(&self, k: usize, vector: &[Scalar], f: F) -> Heap {
        search(&self.mmap, k, vector, f)
    }
}

unsafe impl Send for Hnsw {}
unsafe impl Sync for Hnsw {}

pub struct HnswRam {
    raw: Arc<Raw>,
    quantization: Quantization,
    // ----------------------
    d: Distance,
    // ----------------------
    m: u32,
    // ----------------------
    graph: HnswRamGraph,
    entry: Option<u32>,
    // ----------------------
    visited: Semaphore<Visited>,
}

struct HnswRamGraph {
    vertexs: Vec<HnswRamVertex>,
}

struct HnswRamVertex {
    layers: Vec<RwLock<HnswRamLayer>>,
}

impl HnswRamVertex {
    fn levels(&self) -> u8 {
        self.layers.len() as u8 - 1
    }
}

struct HnswRamLayer {
    edges: Vec<(Scalar, u32)>,
}

pub struct HnswMmap {
    raw: Arc<Raw>,
    quantization: Quantization,
    // ----------------------
    d: Distance,
    // ----------------------
    m: u32,
    // ----------------------
    edges: MmapArray<HnswMmapEdge>,
    by_layer_id: MmapArray<usize>,
    by_vertex_id: MmapArray<usize>,
    entry: u32,
    // ----------------------
    visited: Semaphore<Visited>,
}

#[derive(Debug, Clone, Copy, Default)]
struct HnswMmapEdge(Scalar, u32);

unsafe impl Send for HnswMmap {}
unsafe impl Sync for HnswMmap {}
unsafe impl Pod for HnswMmapEdge {}
unsafe impl Zeroable for HnswMmapEdge {}

pub fn make(
    path: PathBuf,
    sealed: Vec<Arc<SealedSegment>>,
    growing: Vec<Arc<GrowingSegment>>,
    options: IndexOptions,
) -> HnswRam {
    let VectorOptions { d, .. } = options.vector;
    let HnswIndexingOptions {
        m,
        ef_construction,
        quantization: quantization_opts,
    } = options.indexing.clone().unwrap_hnsw();
    let raw = Arc::new(Raw::create(
        path.join("raw"),
        options.clone(),
        sealed,
        growing,
    ));
    let quantization = Quantization::create(
        path.join("quantization"),
        options.clone(),
        quantization_opts,
        &raw,
    );
    let n = raw.len();
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
    let visited = {
        let semaphore = Semaphore::<Visited>::new();
        for _ in 0..std::thread::available_parallelism().unwrap().get() * 2 {
            semaphore.push(Visited::new(n as usize));
        }
        semaphore
    };
    (0..n).into_par_iter().for_each(|i| {
        fn fast_search(
            quantization: &Quantization,
            graph: &HnswRamGraph,
            d: Distance,
            levels: RangeInclusive<u8>,
            u: u32,
            target: &[Scalar],
        ) -> u32 {
            let mut u = u;
            let mut u_dis = quantization.distance(d, target, u);
            for i in levels.rev() {
                let mut changed = true;
                while changed {
                    changed = false;
                    let guard = graph.vertexs[u as usize].layers[i as usize].read();
                    for &(_, v) in guard.edges.iter() {
                        let v_dis = quantization.distance(d, target, v);
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
        fn local_search(
            quantization: &Quantization,
            graph: &HnswRamGraph,
            d: Distance,
            visited: &mut Visited,
            vector: &[Scalar],
            s: u32,
            k: usize,
            i: u8,
        ) -> Vec<(Scalar, u32)> {
            assert!(k > 0);
            let mut visited = visited.new_version();
            let mut candidates = BinaryHeap::<Reverse<(Scalar, u32)>>::new();
            let mut results = BinaryHeap::new();
            let s_dis = quantization.distance(d, vector, s);
            visited.set(s as usize);
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
                    if visited.test(v as usize) {
                        continue;
                    }
                    visited.set(v as usize);
                    let v_dis = quantization.distance(d, vector, v);
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
        fn select(
            quantization: &Quantization,
            d: Distance,
            input: &mut Vec<(Scalar, u32)>,
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
                    .map(|&(_, v)| quantization.distance2(d, u, v))
                    .all(|dist| dist > u_dis);
                if check {
                    res.push((u_dis, u));
                }
            }
            *input = res;
        }
        let mut visited = visited.acquire();
        let target = raw.vector(i);
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
            u = fast_search(&quantization, &graph, d, levels + 1..=top, u, target);
        }
        let mut result = Vec::with_capacity(1 + std::cmp::min(levels, top) as usize);
        for j in (0..=std::cmp::min(levels, top)).rev() {
            let mut edges = local_search(
                &quantization,
                &graph,
                d,
                &mut visited,
                target,
                u,
                ef_construction,
                j,
            );
            edges.sort();
            select(
                &quantization,
                d,
                &mut edges,
                count_max_edges_of_a_layer(m, j),
            );
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
                    d,
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
        raw,
        quantization,
        d,
        m,
        graph,
        entry: entry.into_inner(),
        visited: {
            let semaphore = Semaphore::<Visited>::new();
            for _ in 0..std::thread::available_parallelism().unwrap().get() * 2 {
                semaphore.push(Visited::new(n as usize));
            }
            semaphore
        },
    }
}

pub fn save(mut ram: HnswRam, path: PathBuf) -> HnswMmap {
    let edges = MmapArray::create(
        path.join("edges"),
        ram.graph
            .vertexs
            .iter_mut()
            .flat_map(|v| v.layers.iter_mut())
            .flat_map(|v| &v.get_mut().edges)
            .map(|&(_0, _1)| HnswMmapEdge(_0, _1)),
    );
    let by_layer_id = MmapArray::create(path.join("by_layer_id"), {
        let iter = ram.graph.vertexs.iter_mut();
        let iter = iter.flat_map(|v| v.layers.iter_mut());
        let iter = iter.map(|v| v.get_mut().edges.len());
        caluate_offsets(iter)
    });
    let by_vertex_id = MmapArray::create(path.join("by_vertex_id"), {
        let iter = ram.graph.vertexs.iter_mut();
        let iter = iter.map(|v| v.layers.len());
        caluate_offsets(iter)
    });
    HnswMmap {
        raw: ram.raw,
        quantization: ram.quantization,
        d: ram.d,
        m: ram.m,
        edges,
        by_layer_id,
        by_vertex_id,
        entry: ram.entry.unwrap(),
        visited: ram.visited,
    }
}

pub fn load(path: PathBuf, options: IndexOptions) -> HnswMmap {
    let idx_opts = options.indexing.clone().unwrap_hnsw();
    let raw = Arc::new(Raw::open(path.join("raw"), options.clone()));
    let quantization = Quantization::open(
        path.join("quantization"),
        options.clone(),
        idx_opts.quantization,
        &raw,
    );
    let edges = MmapArray::open(path.join("edges"));
    let by_layer_id = MmapArray::open(path.join("by_layer_id"));
    let by_vertex_id = MmapArray::open(path.join("by_vertex_id"));
    let idx_opts = options.indexing.unwrap_hnsw();
    let n = raw.len();
    let m = idx_opts.m;
    HnswMmap {
        raw,
        quantization,
        d: options.vector.d,
        m: idx_opts.m,
        edges,
        by_layer_id,
        by_vertex_id,
        entry: entry_for_hnsw_graph(m, n),
        visited: {
            let semaphore = Semaphore::<Visited>::new();
            for _ in 0..std::thread::available_parallelism().unwrap().get() * 2 {
                semaphore.push(Visited::new(n as usize));
            }
            semaphore
        },
    }
}

pub fn search<F: FnMut(Payload) -> bool>(
    mmap: &HnswMmap,
    k: usize,
    vector: &[Scalar],
    f: F,
) -> Heap {
    let s = mmap.entry;
    let levels = count_layers_of_a_vertex(mmap.m, s) - 1;
    let u = fast_search(mmap, 1..=levels, s, vector);
    local_search(mmap, k, u, vector, f)
}

pub fn fast_search(mmap: &HnswMmap, levels: RangeInclusive<u8>, u: u32, vector: &[Scalar]) -> u32 {
    let mut u = u;
    let mut u_dis = mmap.quantization.distance(mmap.d, vector, u);
    for i in levels.rev() {
        let mut changed = true;
        while changed {
            changed = false;
            let edges = find_edges(mmap, u, i);
            for &HnswMmapEdge(_, v) in edges.iter() {
                let v_dis = mmap.quantization.distance(mmap.d, vector, v);
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

pub fn local_search<F: FnMut(Payload) -> bool>(
    mmap: &HnswMmap,
    k: usize,
    s: u32,
    vector: &[Scalar],
    mut f: F,
) -> Heap {
    assert!(k > 0);
    let mut visited = mmap.visited.acquire();
    let mut visited = visited.new_version();
    let mut candidates = BinaryHeap::<Reverse<(Scalar, u32)>>::new();
    let mut results = Heap::new(k);
    let s_dis = mmap.quantization.distance(mmap.d, vector, s);
    visited.set(s as usize);
    candidates.push(Reverse((s_dis, s)));
    if f(mmap.raw.payload(s)) {
        results.push(HeapElement {
            distance: s_dis,
            payload: mmap.raw.payload(s),
        });
    }
    while let Some(Reverse((u_dis, u))) = candidates.pop() {
        if !results.check(u_dis) {
            break;
        }
        let edges = find_edges(mmap, u, 0);
        for &HnswMmapEdge(_, v) in edges.iter() {
            if visited.test(v as usize) {
                continue;
            }
            visited.set(v as usize);
            let v_dis = mmap.quantization.distance(mmap.d, vector, v);
            if results.check(v_dis) {
                candidates.push(Reverse((v_dis, v)));
                if f(mmap.raw.payload(v)) {
                    results.push(HeapElement {
                        distance: v_dis,
                        payload: mmap.raw.payload(v),
                    });
                }
            }
        }
    }
    results
}

fn entry_for_hnsw_graph(m: u32, n: u32) -> u32 {
    let mut ans = 1u64;
    while ans * m as u64 <= n as u64 {
        ans *= m as u64;
    }
    (ans - 1) as u32
}

fn count_layers_of_a_vertex(m: u32, i: u32) -> u8 {
    let mut x = i + 1;
    let mut ans = 1;
    while x % m == 0 {
        ans += 1;
        x /= m;
    }
    ans
}

fn count_max_edges_of_a_layer(m: u32, j: u8) -> u32 {
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

fn find_edges(mmap: &HnswMmap, u: u32, level: u8) -> &[HnswMmapEdge] {
    let offset = u as usize;
    let index = mmap.by_vertex_id[offset]..mmap.by_vertex_id[offset + 1];
    let offset = index.start + level as usize;
    let index = mmap.by_layer_id[offset]..mmap.by_layer_id[offset + 1];
    &mmap.edges[index]
}

struct Visited {
    version: usize,
    data: Box<[usize]>,
}

impl Visited {
    fn new(capacity: usize) -> Self {
        Self {
            version: 0,
            data: bytemuck::zeroed_slice_box(capacity),
        }
    }
    fn new_version(&mut self) -> VisitedVersion<'_> {
        assert_ne!(self.version, usize::MAX);
        self.version += 1;
        VisitedVersion { inner: self }
    }
}

struct VisitedVersion<'a> {
    inner: &'a mut Visited,
}

impl<'a> VisitedVersion<'a> {
    fn test(&mut self, i: usize) -> bool {
        self.inner.data[i] == self.inner.version
    }
    fn set(&mut self, i: usize) {
        self.inner.data[i] = self.inner.version;
    }
}
