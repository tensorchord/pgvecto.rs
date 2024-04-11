use base::index::*;
use base::operator::*;
use base::scalar::F32;
use base::search::*;
use common::dir_ops::sync_dir;
use common::mmap_array::MmapArray;
use hnsw::visited::*;
use hnsw::*;
use parking_lot::{RwLock, RwLockWriteGuard};
use quantization::Quantization;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::create_dir;
use std::ops::RangeInclusive;
use std::path::Path;
use std::sync::Arc;
use storage::StorageCollection;

fn mock_make(path: &Path, data_path: &Path, options: IndexOptions) -> HnswRam<Vecf32L2> {
    use storage::vec::VecStorage;
    let HnswIndexingOptions {
        m,
        ef_construction,
        quantization: quantization_opts,
    } = options.indexing.clone().unwrap_hnsw();
    let vectors = MmapArray::open(&Path::new(data_path).join("vectors"));
    let payload = MmapArray::open(&Path::new(data_path).join("payload"));
    let dims = options.vector.dims as u16;
    let storage = Arc::new(StorageCollection::<Vecf32L2>::new(VecStorage::<F32>::new(
        vectors, payload, dims,
    )));
    let quantization = Quantization::create(
        &path.join("quantization"),
        options.clone(),
        quantization_opts,
        &storage,
        (0..storage.len()).collect::<Vec<_>>(),
    );
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
    HnswRam::new(storage, quantization, m, graph, visited)
}

pub fn mock_create(path: &Path, data_path: &Path, options: IndexOptions) -> Hnsw<Vecf32L2> {
    create_dir(path).unwrap();
    let ram = mock_make(path, data_path, options);
    let mmap = save(ram, path);
    sync_dir(path);
    Hnsw::new(mmap)
}

pub fn mock_open(path: &Path, data_path: &Path, options: IndexOptions) -> Hnsw<Vecf32L2> {
    use storage::vec::VecStorage;
    let idx_opts = options.indexing.clone().unwrap_hnsw();
    let vectors = MmapArray::open(&Path::new(data_path).join("vectors"));
    let payload = MmapArray::open(&Path::new(data_path).join("payload"));
    let dims = options.vector.dims as u16;
    let storage = Arc::new(StorageCollection::<Vecf32L2>::new(VecStorage::<F32>::new(
        vectors, payload, dims,
    )));
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
    let mmap = HnswMmap::new(
        storage,
        quantization,
        idx_opts.m,
        edges,
        by_layer_id,
        by_vertex_id,
        VisitedPool::new(n),
    );
    Hnsw::new(mmap)
}
