#![allow(clippy::len_without_is_empty)]
#![allow(clippy::type_complexity)]

use base::index::*;
use base::operator::*;
use base::scalar::F32;
use base::search::*;
use base::vector::VectorBorrowed;
use common::json::Json;
use common::mmap_array::MmapArray;
use common::remap::RemappedCollection;
use common::visited::VisitedPool;
use num_traits::Float;
use parking_lot::RwLock;
use quantization::operator::OperatorQuantization;
use quantization::Quantization;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::fs::create_dir;
use std::ops::RangeInclusive;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use stoppable_rayon as rayon;
use storage::OperatorStorage;
use storage::Storage;

pub trait OperatorHnsw: OperatorQuantization + OperatorStorage {}

impl<T: OperatorQuantization + OperatorStorage> OperatorHnsw for T {}

pub struct Hnsw<O: OperatorHnsw> {
    storage: O::Storage,
    quantization: Quantization<O>,
    payloads: MmapArray<Payload>,
    base_graph_outs: MmapArray<u32>,
    base_graph_weights: MmapArray<F32>,
    hyper_graph_outs: MmapArray<u32>,
    hyper_graph_weights: MmapArray<F32>,
    m: Json<u32>,
    s: Option<u32>,
    visited: VisitedPool,
}

impl<O: OperatorHnsw> Hnsw<O> {
    pub fn create(
        path: impl AsRef<Path>,
        options: IndexOptions,
        source: &(impl Source<O> + Sync),
    ) -> Self {
        let remapped = RemappedCollection::from_source(source);
        if let Some(main) = source.get_main::<Self>() {
            if remapped.barrier() != 0 {
                from_main(path, options, &remapped, main)
            } else {
                from_nothing(path, options, &remapped)
            }
        } else {
            from_nothing(path, options, &remapped)
        }
    }

    pub fn open(path: impl AsRef<Path>) -> Self {
        open(path)
    }

    pub fn vbase<'a>(
        &'a self,
        vector: Borrowed<'a, O>,
        opts: &'a SearchOptions,
    ) -> (Vec<Element>, Box<dyn Iterator<Item = Element> + 'a>) {
        let Some(s) = self.s else {
            return (Vec::new(), Box::new(std::iter::empty()));
        };
        let s = {
            let processed = self.quantization.preprocess(vector);
            fast_search(
                |x| self.quantization.process(&self.storage, &processed, x),
                |x, i| hyper_outs(self, x, i),
                1..=hierarchy_for_a_vertex(*self.m, s) - 1,
                s,
            )
        };
        graph::search::vbase_generic(
            &self.visited,
            s,
            opts.hnsw_ef_search,
            self.quantization.graph_rerank(vector, move |u| {
                (
                    O::distance(self.storage.vector(u), vector),
                    (self.payloads[u as usize], base_outs(self, u)),
                )
            }),
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

fn from_nothing<O: OperatorHnsw>(
    path: impl AsRef<Path>,
    options: IndexOptions,
    collection: &(impl Collection<O> + Sync),
) -> Hnsw<O> {
    create_dir(path.as_ref()).unwrap();
    let HnswIndexingOptions {
        m,
        ef_construction,
        quantization: quantization_options,
    } = options.indexing.clone().unwrap_hnsw();
    let mut g = fresh(collection.len(), m);
    patch_insertions(
        |u, v| O::distance(collection.vector(u), collection.vector(v)),
        |_| false,
        collection.len(),
        ef_construction,
        m,
        &mut g,
    );
    rayon::check();
    finish(&mut g, m);
    let storage = O::Storage::create(path.as_ref().join("storage"), collection);
    rayon::check();
    let quantization = Quantization::create(
        path.as_ref().join("quantization"),
        options.vector,
        quantization_options,
        collection,
        |vector| vector.own(),
    );
    rayon::check();
    let payloads = MmapArray::create(
        path.as_ref().join("payloads"),
        (0..collection.len()).map(|i| collection.payload(i)),
    );
    rayon::check();
    let base_graph_outs = MmapArray::create(
        path.as_ref().join("base_graph_outs"),
        g.iter_mut()
            .flat_map(|x| x[0].get_mut())
            .map(|&mut (_0, _1)| _1),
    );
    rayon::check();
    let base_graph_weights = MmapArray::create(
        path.as_ref().join("base_graph_weights"),
        g.iter_mut()
            .flat_map(|x| x[0].get_mut())
            .map(|&mut (_0, _1)| _0),
    );
    rayon::check();
    let hyper_graph_outs = MmapArray::create(
        path.as_ref().join("hyper_graph_outs"),
        g.iter_mut()
            .flat_map(|x| x.iter_mut())
            .skip(1)
            .flat_map(|x| x.get_mut())
            .map(|&mut (_0, _1)| _1),
    );
    rayon::check();
    let hyper_graph_weights = MmapArray::create(
        path.as_ref().join("hyper_graph_weights"),
        g.iter_mut()
            .flat_map(|x| x.iter_mut())
            .skip(1)
            .flat_map(|x| x.get_mut())
            .map(|&mut (_0, _1)| _0),
    );
    rayon::check();
    let m = Json::create(path.as_ref().join("m"), m);
    Hnsw {
        storage,
        quantization,
        payloads,
        base_graph_outs,
        base_graph_weights,
        hyper_graph_outs,
        hyper_graph_weights,
        m,
        s: start(collection.len(), *m),
        visited: VisitedPool::new(collection.len()),
    }
}

fn from_main<O: OperatorHnsw>(
    path: impl AsRef<Path>,
    options: IndexOptions,
    remapped: &RemappedCollection<O, impl Collection<O> + Sync>,
    main: &Hnsw<O>,
) -> Hnsw<O> {
    create_dir(path.as_ref()).unwrap();
    let HnswIndexingOptions {
        m,
        ef_construction,
        quantization: quantization_options,
    } = options.indexing.clone().unwrap_hnsw();
    let mut g = fresh(remapped.len(), m);
    patch_deletions(
        |u, v| O::distance(remapped.vector(u), remapped.vector(v)),
        |u| remapped.skip(u),
        |u, level| {
            if level == 0 {
                Box::new(base_edges(main, u)) as Box<dyn Iterator<Item = (F32, u32)>>
            } else {
                Box::new(hyper_edges(main, u, level)) as Box<dyn Iterator<Item = (F32, u32)>>
            }
        },
        remapped.len(),
        m,
        &mut g,
    );
    rayon::check();
    patch_insertions(
        |u, v| O::distance(remapped.vector(u), remapped.vector(v)),
        |u| remapped.skip(u),
        remapped.len(),
        ef_construction,
        m,
        &mut g,
    );
    rayon::check();
    finish(&mut g, m);
    let storage = O::Storage::create(path.as_ref().join("storage"), remapped);
    rayon::check();
    let quantization = Quantization::create(
        path.as_ref().join("quantization"),
        options.vector,
        quantization_options,
        remapped,
        |vector| vector.own(),
    );
    rayon::check();
    let payloads = MmapArray::create(
        path.as_ref().join("payloads"),
        (0..remapped.len()).map(|i| remapped.payload(i)),
    );
    rayon::check();
    let base_graph_outs = MmapArray::create(
        path.as_ref().join("base_graph_outs"),
        g.iter_mut()
            .flat_map(|x| x[0].get_mut())
            .map(|&mut (_0, _1)| _1),
    );
    rayon::check();
    let base_graph_weights = MmapArray::create(
        path.as_ref().join("base_graph_weights"),
        g.iter_mut()
            .flat_map(|x| x[0].get_mut())
            .map(|&mut (_0, _1)| _0),
    );
    rayon::check();
    let hyper_graph_outs = MmapArray::create(
        path.as_ref().join("hyper_graph_outs"),
        g.iter_mut()
            .flat_map(|x| x.iter_mut())
            .skip(1)
            .flat_map(|x| x.get_mut())
            .map(|&mut (_0, _1)| _1),
    );
    rayon::check();
    let hyper_graph_weights = MmapArray::create(
        path.as_ref().join("hyper_graph_weights"),
        g.iter_mut()
            .flat_map(|x| x.iter_mut())
            .skip(1)
            .flat_map(|x| x.get_mut())
            .map(|&mut (_0, _1)| _0),
    );
    rayon::check();
    let m = Json::create(path.as_ref().join("m"), m);
    rayon::check();
    Hnsw {
        storage,
        quantization,
        payloads,
        base_graph_outs,
        base_graph_weights,
        hyper_graph_outs,
        hyper_graph_weights,
        m,
        s: start(remapped.len(), *m),
        visited: VisitedPool::new(remapped.len()),
    }
}

fn open<O: OperatorHnsw>(path: impl AsRef<Path>) -> Hnsw<O> {
    let storage = O::Storage::open(path.as_ref().join("storage"));
    let quantization = Quantization::open(path.as_ref().join("quantization"));
    let payloads = MmapArray::open(path.as_ref().join("payloads"));
    let base_graph_outs = MmapArray::open(path.as_ref().join("base_graph_outs"));
    let base_graph_weights = MmapArray::open(path.as_ref().join("base_graph_weights"));
    let hyper_graph_outs = MmapArray::open(path.as_ref().join("hyper_graph_outs"));
    let hyper_graph_weights = MmapArray::open(path.as_ref().join("hyper_graph_weights"));
    let m = Json::open(path.as_ref().join("m"));
    let n = storage.len();
    Hnsw {
        storage,
        quantization,
        payloads,
        base_graph_outs,
        base_graph_weights,
        hyper_graph_outs,
        hyper_graph_weights,
        m,
        s: start(n, *m),
        visited: VisitedPool::new(n),
    }
}

fn fast_search<E>(
    dist: impl Fn(u32) -> F32,
    read_outs: impl Fn(u32, u8) -> E,
    levels: RangeInclusive<u8>,
    u: u32,
) -> u32
where
    E: Iterator<Item = u32>,
{
    let mut u = u;
    let mut dis_u = dist(u);
    for i in levels.rev() {
        let mut changed = true;
        while changed {
            changed = false;
            for v in read_outs(u, i) {
                let dis_v = dist(v);
                if dis_v < dis_u {
                    u = v;
                    dis_u = dis_v;
                    changed = true;
                }
            }
        }
    }
    u
}

fn fresh(n: u32, m: u32) -> Vec<Vec<RwLock<Vec<(F32, u32)>>>> {
    let mut g = Vec::with_capacity(n as usize);
    for u in 0..n {
        let l = hierarchy_for_a_vertex(m, u);
        let mut vertex = Vec::new();
        vertex.resize_with(l as usize, || RwLock::new(Vec::new()));
        g.push(vertex);
    }
    g
}

fn patch_deletions<E>(
    dist: impl Fn(u32, u32) -> F32 + Copy + Sync,
    skip: impl Fn(u32) -> bool + Sync,
    read_edges: impl Fn(u32, u8) -> E + Sync,
    n: u32,
    m: u32,
    g: &mut [Vec<RwLock<Vec<(F32, u32)>>>],
) where
    E: Iterator<Item = (F32, u32)>,
{
    (0..n).into_par_iter().for_each(|u| {
        rayon::check();
        if !skip(u) {
            return;
        }
        for level in 0..hierarchy_for_a_vertex(m, u) {
            let ori = read_edges(u, level).collect::<Vec<_>>();
            let mut base = ori
                .iter()
                .copied()
                .filter(|&(_, v)| skip(v))
                .collect::<Vec<_>>();
            let d = ori
                .iter()
                .copied()
                .filter(|&(_, v)| !skip(v))
                .collect::<Vec<_>>();
            let mut add = vec![];
            for (_, v) in d {
                let v_ori = read_edges(v, level).map(|(_, w)| w);
                add.extend(v_ori.filter(|&w| skip(w)).map(|w| (dist(u, w), w)));
            }
            graph::prune::prune(dist, u, &mut base, &add, m);
            *g[u as usize][level as usize].write() = base;
        }
    });
}

fn patch_insertions(
    dist: impl Fn(u32, u32) -> F32 + Copy + Sync,
    skip: impl Fn(u32) -> bool + Sync,
    n: u32,
    ef_construction: u32,
    m: u32,
    g: &mut [Vec<RwLock<Vec<(F32, u32)>>>],
) {
    #[repr(C)]
    #[derive(Debug, Clone, Copy)]
    struct Start {
        val: u32,
        holding: bool,
    }
    impl Start {
        fn into_u64(self) -> u64 {
            (self.val as u64) | ((self.holding as u64) << 32)
        }
        fn from_u64(x: u64) -> Start {
            Start {
                val: x as u32,
                holding: (x >> 32) != 0,
            }
        }
        fn is_holding(self) -> bool {
            self.val == 1
        }
        fn is_empty(self) -> bool {
            self.val == u32::MAX
        }
        fn new(val: u32, holding: bool) -> Start {
            Start { val, holding }
        }
        fn start(self) -> Option<u32> {
            if self.val != u32::MAX {
                Some(self.val)
            } else {
                None
            }
        }
    }
    let s = AtomicU64::new(
        Start::new(
            'a: {
                let mut shift = 1u64;
                while shift * m as u64 <= n as u64 {
                    shift *= m as u64;
                }
                while shift != 0 {
                    let mut i = 1u64;
                    while i * shift <= n as u64 {
                        let e = (i * shift - 1) as u32;
                        if i % m as u64 != 0 && skip(e) {
                            break 'a e;
                        }
                        i += 1;
                    }
                    shift /= m as u64;
                }
                break 'a u32::MAX;
            },
            false,
        )
        .into_u64(),
    );
    let visited = VisitedPool::new(n);
    (0..n).into_par_iter().for_each(|u| {
        rayon::check();
        if skip(u) {
            return;
        }
        let mut visited = visited.fetch_guard();
        let l = hierarchy_for_a_vertex(m, u);
        let mut start = Start::from_u64(s.load(Ordering::Acquire));
        let update_start = loop {
            if start.is_holding() {
                rayon::check();
                std::thread::yield_now();
                continue;
            }
            if start.is_empty() || g[start.val as usize].len() < g[u as usize].len() {
                match s.compare_exchange_weak(
                    start.into_u64(),
                    Start::new(u, true).into_u64(),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break true,
                    Err(val) => start = Start::from_u64(val),
                }
                continue;
            }
            break false;
        };
        let mut temp = vec![vec![]; l as usize];
        if let Some(mut cursor) = start.start() {
            let t = hierarchy_for_a_vertex(m, cursor);
            if t > l {
                cursor = fast_search(
                    |x| dist(u, x),
                    |x, level| {
                        g[x as usize][level as usize]
                            .read()
                            .clone()
                            .into_iter()
                            .map(|(_, x)| x)
                    },
                    l - 1..=t - 1,
                    cursor,
                );
            }
            for j in (0..std::cmp::min(l, t)).rev() {
                let scope = graph::search::search(
                    |x| dist(u, x),
                    |x| {
                        g[x as usize][j as usize]
                            .read()
                            .clone()
                            .into_iter()
                            .map(|(_, x)| x)
                    },
                    &mut visited,
                    cursor,
                    ef_construction,
                );
                graph::prune::prune(
                    dist,
                    u,
                    &mut temp[j as usize],
                    &scope,
                    capacity_for_a_hierarchy(m, j),
                );
                cursor = if let Some(x) = scope.first() {
                    x.1
                } else {
                    break;
                };
                temp[j as usize] = scope;
            }
        }
        for j in 0..l {
            g[u as usize][j as usize]
                .write()
                .clone_from(&temp[j as usize]);
        }
        for j in 0..l {
            for (dis_v, v) in temp[j as usize].iter().copied() {
                let mut lock = g[v as usize][j as usize].write();
                if lock.iter().any(|(_, k)| *k == u) {
                    continue;
                }
                if lock.len() < m as usize {
                    let (Ok(index) | Err(index)) = lock.binary_search(&(dis_v, u));
                    lock.insert(index, (dis_v, u));
                    continue;
                }
                graph::prune::prune(
                    dist,
                    v,
                    &mut lock,
                    &[(dis_v, u)],
                    capacity_for_a_hierarchy(m, j),
                );
            }
        }
        if update_start {
            s.store(Start::new(u, false).into_u64(), Ordering::Release);
        }
    });
}

fn finish(g: &mut [Vec<RwLock<Vec<(F32, u32)>>>], m: u32) {
    for u in 0..g.len() as u32 {
        let l = hierarchy_for_a_vertex(m, u);
        for j in 0..l {
            g[u as usize][j as usize].get_mut().resize(
                capacity_for_a_hierarchy(m, j) as usize,
                (F32::infinity(), u32::MAX),
            );
        }
    }
}

fn hierarchy_for_a_vertex(m: u32, u: u32) -> u8 {
    let mut x = u + 1;
    let mut ans = 1;
    while x % m == 0 {
        ans += 1;
        x /= m;
    }
    ans
}

fn capacity_for_a_hierarchy(m: u32, level: u8) -> u32 {
    if level == 0 {
        m * 2
    } else {
        m
    }
}

fn base_edges<O: OperatorHnsw>(hnsw: &Hnsw<O>, u: u32) -> impl Iterator<Item = (F32, u32)> + '_ {
    let m = *hnsw.m;
    let offset = 2 * m as usize * u as usize;
    let edges_outs = hnsw.base_graph_outs[offset..offset + 2 * m as usize]
        .iter()
        .take_while(|v| **v != u32::MAX)
        .copied();
    let edges_weights = hnsw.base_graph_weights[offset..offset + 2 * m as usize]
        .iter()
        .copied();
    edges_weights.zip(edges_outs)
}

fn base_outs<O: OperatorHnsw>(hnsw: &Hnsw<O>, u: u32) -> impl Iterator<Item = u32> + '_ {
    let m = *hnsw.m;
    let offset = 2 * m as usize * u as usize;
    hnsw.base_graph_outs[offset..offset + 2 * m as usize]
        .iter()
        .take_while(|v| **v != u32::MAX)
        .copied()
}

fn hyper_edges<O: OperatorHnsw>(
    hnsw: &Hnsw<O>,
    u: u32,
    level: u8,
) -> impl Iterator<Item = (F32, u32)> + '_ {
    let m = *hnsw.m;
    let offset = {
        let mut offset = 0;
        let mut x = u as usize;
        loop {
            x /= m as usize;
            if x == 0 {
                break;
            }
            offset += x * m as usize;
        }
        offset + (level as usize - 1) * m as usize
    };
    let edges_outs = hnsw.hyper_graph_outs[offset..offset + m as usize]
        .iter()
        .take_while(|v| **v != u32::MAX)
        .copied();
    let edges_weights = hnsw.hyper_graph_weights[offset..offset + m as usize]
        .iter()
        .copied();
    edges_weights.zip(edges_outs)
}

fn hyper_outs<O: OperatorHnsw>(
    hnsw: &Hnsw<O>,
    u: u32,
    level: u8,
) -> impl Iterator<Item = u32> + '_ {
    let m = *hnsw.m;
    let offset = {
        let mut offset = 0;
        let mut x = u as usize;
        loop {
            x /= m as usize;
            if x == 0 {
                break;
            }
            offset += x * m as usize;
        }
        offset + (level as usize - 1) * m as usize
    };
    hnsw.hyper_graph_outs[offset..offset + m as usize]
        .iter()
        .take_while(|v| **v != u32::MAX)
        .copied()
}

fn start(n: u32, m: u32) -> Option<u32> {
    if n == 0 {
        return None;
    }
    let mut shift = 1u64;
    while shift * m as u64 <= n as u64 {
        shift *= m as u64;
    }
    Some(shift as u32 - 1)
}
