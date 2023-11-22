use super::quantization::Quantization;
use super::raw::Raw;
use crate::index::indexing::hnsw::HnswIndexingOptions;
use crate::index::segments::growing::GrowingSegment;
use crate::index::segments::sealed::SealedSegment;
use crate::index::{IndexOptions, VectorOptions};
use crate::prelude::*;
use crate::utils::dir_ops::sync_dir;
use crate::utils::mmap_array::MmapArray;
use bytemuck::{Pod, Zeroable};
use parking_lot::{Mutex, RwLock, RwLockWriteGuard};
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

    pub fn search(&self, k: usize, vector: &[Scalar], filter: &mut impl Filter) -> Heap {
        search(&self.mmap, k, vector, filter)
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
    // ----------------------
    visited: VisitedPool,
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
    // ----------------------
    visited: VisitedPool,
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
    let visited = VisitedPool::new(raw.len());
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
            visited: &mut VisitedGuard,
            vector: &[Scalar],
            s: u32,
            k: usize,
            i: u8,
        ) -> Vec<(Scalar, u32)> {
            assert!(k > 0);
            let mut visited = visited.fetch();
            let mut candidates = BinaryHeap::<Reverse<(Scalar, u32)>>::new();
            let mut results = BinaryHeap::new();
            let s_dis = quantization.distance(d, vector, s);
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
        let mut visited = visited.fetch();
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
        visited,
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
    HnswMmap {
        raw,
        quantization,
        d: options.vector.d,
        m: idx_opts.m,
        edges,
        by_layer_id,
        by_vertex_id,
        visited: VisitedPool::new(n),
    }
}

pub fn search(mmap: &HnswMmap, k: usize, vector: &[Scalar], filter: &mut impl Filter) -> Heap {
    let Some(s) = entry(mmap, filter) else {
        return Heap::new(k);
    };
    let levels = count_layers_of_a_vertex(mmap.m, s) - 1;
    let u = fast_search(mmap, 1..=levels, s, vector, filter);
    local_search(mmap, k, u, vector, filter)
}

pub fn entry(mmap: &HnswMmap, filter: &mut impl Filter) -> Option<u32> {
    let m = mmap.m;
    let n = mmap.raw.len();
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
                if filter.check(mmap.raw.payload(e)) {
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

pub fn fast_search(
    mmap: &HnswMmap,
    levels: RangeInclusive<u8>,
    u: u32,
    vector: &[Scalar],
    filter: &mut impl Filter,
) -> u32 {
    let mut u = u;
    let mut u_dis = mmap.quantization.distance(mmap.d, vector, u);
    for i in levels.rev() {
        let mut changed = true;
        while changed {
            changed = false;
            let edges = find_edges(mmap, u, i);
            for &HnswMmapEdge(_, v) in edges.iter() {
                if !filter.check(mmap.raw.payload(v)) {
                    continue;
                }
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

pub fn local_search(
    mmap: &HnswMmap,
    k: usize,
    s: u32,
    vector: &[Scalar],
    filter: &mut impl Filter,
) -> Heap {
    assert!(k > 0);
    let mut visited = mmap.visited.fetch();
    let mut visited = visited.fetch();
    let mut candidates = BinaryHeap::<Reverse<(Scalar, u32)>>::new();
    let mut results = Heap::new(k);
    visited.mark(s);
    let s_dis = mmap.quantization.distance(mmap.d, vector, s);
    candidates.push(Reverse((s_dis, s)));
    results.push(HeapElement {
        distance: s_dis,
        payload: mmap.raw.payload(s),
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
            if !filter.check(mmap.raw.payload(v)) {
                continue;
            }
            let v_dis = mmap.quantization.distance(mmap.d, vector, v);
            if !results.check(v_dis) {
                continue;
            }
            candidates.push(Reverse((v_dis, v)));
            results.push(HeapElement {
                distance: v_dis,
                payload: mmap.raw.payload(v),
            });
        }
    }
    results
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

struct VisitedPool {
    n: u32,
    locked_buffers: Mutex<Vec<VisitedBuffer>>,
}

impl VisitedPool {
    pub fn new(n: u32) -> Self {
        Self {
            n,
            locked_buffers: Mutex::new(Vec::new()),
        }
    }
    pub fn fetch(&self) -> VisitedGuard<'_> {
        let buffer = self
            .locked_buffers
            .lock()
            .pop()
            .unwrap_or_else(|| VisitedBuffer::new(self.n as _));
        VisitedGuard { buffer, pool: self }
    }
}

struct VisitedGuard<'a> {
    buffer: VisitedBuffer,
    pool: &'a VisitedPool,
}

impl<'a> VisitedGuard<'a> {
    fn fetch(&mut self) -> VisitedChecker<'_> {
        self.buffer.version = self.buffer.version.wrapping_add(1);
        if self.buffer.version == 0 {
            self.buffer.data.fill(0);
        }
        VisitedChecker {
            buffer: &mut self.buffer,
        }
    }
}

impl<'a> Drop for VisitedGuard<'a> {
    fn drop(&mut self) {
        let src = VisitedBuffer {
            version: 0,
            data: Box::new([]),
        };
        let buffer = std::mem::replace(&mut self.buffer, src);
        self.pool.locked_buffers.lock().push(buffer);
    }
}

struct VisitedChecker<'a> {
    buffer: &'a mut VisitedBuffer,
}

impl<'a> VisitedChecker<'a> {
    fn check(&mut self, i: u32) -> bool {
        self.buffer.data[i as usize] != self.buffer.version
    }
    fn mark(&mut self, i: u32) {
        self.buffer.data[i as usize] = self.buffer.version;
    }
}

struct VisitedBuffer {
    version: usize,
    data: Box<[usize]>,
}

impl VisitedBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            version: 0,
            data: bytemuck::zeroed_slice_box(capacity),
        }
    }
}
