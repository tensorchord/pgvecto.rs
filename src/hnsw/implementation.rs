use super::semaphore::Semaphore;
use super::slab::Slab;
use super::visited::Visited;
use crate::prelude::*;
use byteorder::NativeEndian as E;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use parking_lot::RwLock;
use parking_lot::RwLockReadGuard;
use parking_lot::RwLockWriteGuard;
use rand::Rng;
use std::alloc::Allocator;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Read;
use std::io::Write;
use std::ops::RangeInclusive;
use std::path::Path;

pub struct Implementation<A: Allocator> {
    distance: Distance,
    dimensions: u16,
    m: usize,
    ef_construction: usize,
    max_level: usize,
    entry: RwLock<Option<usize>>,
    graph: Slab<Vertex>,
    visited: Semaphore<Visited>,
    #[allow(dead_code)]
    allocator: A,
}

unsafe impl<A: Allocator + Send> Send for Implementation<A> {}
unsafe impl<A: Allocator + Sync> Sync for Implementation<A> {}

impl<A: Allocator> Implementation<A> {
    pub fn save(&mut self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        fn f<T: BincodeSerialize>(mut x: impl Write, t: &T) -> anyhow::Result<()> {
            let bytes = t.serialize()?;
            x.write_u64::<E>(bytes.len() as _)?;
            x.write_all(&bytes)?;
            Ok(())
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        let mut w = BufWriter::new(file);
        f(&mut w, &self.dimensions)?;
        f(&mut w, &self.distance)?;
        f(&mut w, &self.entry.get_mut())?;
        f(&mut w, &self.graph.capacity())?;
        f(&mut w, &self.graph.len_mut())?;
        for i in 0..self.graph.len() {
            let x = self.graph.get_mut(i).unwrap();
            f(&mut w, &x.data)?;
            f(&mut w, &x.vector)?;
            f(&mut w, &x.layers.len())?;
            for j in 0..x.layers.len() {
                let y = x.layers[j].get_mut();
                f(&mut w, y)?;
            }
        }
        w.flush()?;
        Ok(())
    }
    pub fn load(
        max_threads: usize,
        m: usize,
        ef_construction: usize,
        max_level: usize,
        path: impl AsRef<Path>,
        allocator: A,
    ) -> anyhow::Result<Self> {
        fn f<T: for<'a> serde::Deserialize<'a>>(mut x: impl Read) -> anyhow::Result<T> {
            let len = x.read_u64::<E>()? as usize;
            let mut buffer = vec![0u8; len];
            x.read_exact(&mut buffer)?;
            Ok(buffer.deserialize()?)
        }
        let file = std::fs::OpenOptions::new()
            .create(false)
            .read(true)
            .write(false)
            .open(path)?;
        let mut r = BufReader::new(file);
        let dimensions: u16 = f(&mut r)?;
        let distance: Distance = f(&mut r)?;
        let entry: Option<usize> = f(&mut r)?;
        let capacity: usize = f(&mut r)?;
        let len: usize = f(&mut r)?;
        let mut graph = Slab::<Vertex>::new(capacity);
        for _ in 0..len {
            let data: u64 = f(&mut r)?;
            let vector: Vec<Scalar> = f(&mut r)?;
            let len: usize = f(&mut r)?;
            let mut layers = Vec::new();
            for _ in 0..len {
                let y: SortedVec<(Tcalar, usize)> = f(&mut r)?;
                layers.push(y);
            }
            let x = Vertex::new(data, vector, layers);
            graph.push_mut(x).ok().unwrap();
        }
        Ok(Self {
            dimensions,
            distance,
            entry: RwLock::new(entry),
            graph,
            visited: {
                let semaphore = Semaphore::<Visited>::new();
                for _ in 0..max_threads {
                    semaphore.push(Visited::new(capacity));
                }
                semaphore
            },
            m,
            ef_construction,
            max_level,
            allocator,
        })
    }
    pub fn new(
        dimensions: u16,
        capacity: usize,
        distance: Distance,
        max_threads: usize,
        m: usize,
        ef_construction: usize,
        max_level: usize,
        allocator: A,
    ) -> Self {
        Self {
            dimensions,
            distance,
            entry: RwLock::new(None),
            graph: Slab::new(capacity),
            visited: {
                let semaphore = Semaphore::<Visited>::new();
                for _ in 0..max_threads {
                    semaphore.push(Visited::new(capacity));
                }
                semaphore
            },
            m,
            ef_construction,
            max_level,
            allocator,
        }
    }
    pub fn search(&self, (vector, k): (Vec<Scalar>, usize)) -> anyhow::Result<Vec<(Scalar, u64)>> {
        let entry = self.entry.read().clone();
        let Some(u) = entry else { return Ok(Vec::new()) };
        let top = self.graph[u].levels();
        let u = self._go(1..=top, u, &vector);
        let mut visited = self.visited.acquire();
        Ok(self
            ._search(&mut visited, &vector, u, k, 0)
            .iter()
            .map(|&(score, u)| (score.0, self.graph[u].data()))
            .collect::<Vec<_>>())
    }
    pub fn insert(&self, insert: (Vec<Scalar>, u64)) -> anyhow::Result<()> {
        let mut visited = self.visited.acquire();
        Ok(self._insert(&mut visited, insert)?)
    }
    fn _go(&self, levels: RangeInclusive<u8>, u: usize, target: &[Scalar]) -> usize {
        let mut u = u;
        let mut u_dis = self._dist1(u, target);
        for i in levels.rev() {
            let mut changed = true;
            while changed {
                changed = false;
                let guard = self.graph[u].read(i);
                for (_, v) in guard.iter().copied() {
                    let v_dis = self._dist1(v, target);
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
    fn _insert(
        &self,
        visited: &mut Visited,
        (vector, data): (Vec<Scalar>, u64),
    ) -> anyhow::Result<()> {
        let levels = generate_random_levels(self.m, self.max_level);
        let entry;
        let lock = {
            let cond = |global: Option<_>| global.map(|u| self.graph[u].levels()) < Some(levels);
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
            let layers = vec![SortedVec::new(); 1 + levels as usize];
            let vertex = Vertex::new(data, vector, layers);
            let pointer = self.graph.push(vertex).ok().unwrap();
            *lock.unwrap() = Some(pointer);
            return Ok(());
        };
        let top = self.graph[u].levels();
        if top > levels {
            u = self._go(levels + 1..=top, u, &vector);
        }
        let mut layers = Vec::with_capacity(1 + levels as usize);
        for i in (0..=std::cmp::min(levels, top)).rev() {
            let mut layer = self._search(visited, &vector, u, self.ef_construction, i);
            self._select(&mut layer, size_of_a_layer(self.m, i));
            u = layer.min().unwrap().1;
            layers.push(layer);
        }
        layers.reverse();
        layers.resize(1 + levels as usize, SortedVec::new());
        let vertex = Vertex::new(data, vector, layers.clone());
        let pointer = self.graph.push(vertex).ok().unwrap();
        for (i, layer) in layers.into_iter().enumerate() {
            let i = i as u8;
            for (n_dis, n) in layer.iter().copied() {
                let mut guard = self.graph[n].write(i);
                guard.insert((n_dis, pointer));
                self._select(&mut guard, size_of_a_layer(self.m, i));
            }
        }
        if let Some(mut lock) = lock {
            *lock = Some(pointer);
        }
        Ok(())
    }
    fn _select(&self, input: &mut SortedVec<(Tcalar, usize)>, size: usize) {
        if input.len() <= size {
            return;
        }
        let mut output = SortedVec::new();
        for (u_dis, u) in input.iter().copied() {
            if output.len() == size {
                break;
            }
            let check = output
                .iter()
                .map(|&(_, v)| self._dist2(u, v))
                .all(|dist| dist > u_dis);
            if check {
                output.push((u_dis, u));
            }
        }
        *input = output;
    }
    fn _search(
        &self,
        visited: &mut Visited,
        target: &[Scalar],
        s: usize,
        k: usize,
        i: u8,
    ) -> SortedVec<(Tcalar, usize)> {
        assert!(k > 0);
        let mut bound = Tcalar(Scalar::INFINITY);
        let mut visited = visited.new_version();
        let mut candidates = BinaryHeap::<Reverse<(Tcalar, usize)>>::new();
        let mut results = BinaryHeap::<(Tcalar, usize)>::new();
        let s_dis = self._dist1(s, target);
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
            let guard = self.graph[u].read(i);
            for (_, v) in guard.iter().copied() {
                if visited.test(v) {
                    continue;
                }
                visited.set(v);
                let v_dis = self._dist1(v, target);
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
        SortedVec::from_unsorted(results.into_vec())
    }
    fn _dist1(&self, u: usize, target: &[Scalar]) -> Tcalar {
        let u = self.graph[u].vector();
        Tcalar(self.distance.distance(u, target))
    }
    fn _dist2(&self, u: usize, v: usize) -> Tcalar {
        let u = self.graph[u].vector();
        let v = self.graph[v].vector();
        Tcalar(self.distance.distance(u, v))
    }
}

struct Vertex {
    data: u64,
    vector: Vec<Scalar>,
    layers: Vec<RwLock<SortedVec<(Tcalar, usize)>>>,
}

impl Vertex {
    fn new(data: u64, vector: Vec<Scalar>, layers: Vec<SortedVec<(Tcalar, usize)>>) -> Self {
        assert!(layers.len() != 0);
        Self {
            data,
            vector,
            layers: layers.into_iter().map(RwLock::new).collect(),
        }
    }
    fn data(&self) -> u64 {
        self.data
    }
    fn vector(&self) -> &[Scalar] {
        &self.vector
    }
    fn levels(&self) -> u8 {
        self.layers.len() as u8 - 1
    }
    fn read(&self, i: u8) -> RwLockReadGuard<'_, SortedVec<(Tcalar, usize)>> {
        self.layers[i as usize].read()
    }
    fn write(&self, i: u8) -> RwLockWriteGuard<'_, SortedVec<(Tcalar, usize)>> {
        self.layers[i as usize].write()
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
struct SortedVec<T> {
    vec: Vec<T>,
}

impl<T> SortedVec<T> {
    fn new() -> Self {
        Self { vec: Vec::new() }
    }
    fn from_unsorted(mut vec: Vec<T>) -> Self
    where
        T: Ord,
    {
        vec.sort();
        Self { vec }
    }
    fn push(&mut self, x: T)
    where
        T: Ord,
    {
        assert!(self.vec.last() <= Some(&x));
        self.vec.push(x);
    }
    fn insert(&mut self, element: T) -> usize
    where
        T: Ord,
    {
        let (Ok(index) | Err(index)) = self.vec.binary_search(&element);
        self.vec.insert(index, element);
        index
    }
    fn iter(&self) -> impl Iterator<Item = &T> {
        self.vec.iter()
    }
    fn min(&self) -> Option<&T> {
        self.vec.first()
    }
    fn len(&self) -> usize {
        self.vec.len()
    }
}

// "Tcalar" is "Total-ordering scalar".
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
struct Tcalar(Scalar);

impl PartialEq for Tcalar {
    fn eq(&self, other: &Self) -> bool {
        use std::cmp::Ordering;
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Tcalar {}

impl PartialOrd for Tcalar {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Tcalar {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
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
