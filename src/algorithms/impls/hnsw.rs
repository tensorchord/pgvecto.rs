use crate::algorithms::Vectors;
use crate::memory::Address;
use crate::memory::PBox;
use crate::memory::Persistent;
use crate::memory::Ptr;
use crate::prelude::*;
use crate::utils::parray::PArray;
use crate::utils::semaphore::Semaphore;
use crate::utils::unsafe_once::UnsafeOnce;
use parking_lot::RwLock;
use rand::Rng;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::ops::RangeInclusive;
use std::sync::Arc;

type Vertex = PBox<[RwLock<PArray<(Scalar, usize)>>]>;

pub struct Root {
    vertexs: PBox<[UnsafeOnce<Vertex>]>,
    entry: RwLock<Option<usize>>,
}

static_assertions::assert_impl_all!(Root: Persistent);

pub struct HnswImpl {
    pub address: Address,
    root: &'static Root,
    vectors: Arc<Vectors>,
    distance: Distance,
    dims: u16,
    m: usize,
    ef_construction: usize,
    visited: Semaphore<Visited>,
    storage: Storage,
}

unsafe impl Send for HnswImpl {}
unsafe impl Sync for HnswImpl {}

impl HnswImpl {
    pub fn new(
        vectors: Arc<Vectors>,
        dims: u16,
        distance: Distance,
        capacity: usize,
        max_threads: usize,
        m: usize,
        ef_construction: usize,
        storage: Storage,
    ) -> anyhow::Result<Self> {
        let ptr = PBox::new(
            Root {
                vertexs: unsafe { PBox::new_zeroed_slice(capacity, storage)?.assume_init() },
                entry: RwLock::new(None),
            },
            storage,
        )?
        .into_raw();
        Ok(Self {
            address: ptr.address(),
            root: unsafe { ptr.as_ref() },
            vectors,
            dims,
            distance,
            visited: {
                let semaphore = Semaphore::<Visited>::new();
                for _ in 0..max_threads {
                    semaphore.push(Visited::new(capacity));
                }
                semaphore
            },
            m,
            ef_construction,
            storage,
        })
    }
    pub fn load(
        vectors: Arc<Vectors>,
        distance: Distance,
        dims: u16,
        capacity: usize,
        max_threads: usize,
        m: usize,
        ef_construction: usize,
        address: Address,
        storage: Storage,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            address,
            root: unsafe { Ptr::new(address, ()).as_ref() },
            vectors,
            distance,
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
            storage,
        })
    }
    pub fn search(
        &self,
        (x_vector, k): (Box<[Scalar]>, usize),
    ) -> anyhow::Result<Vec<(Scalar, u64)>> {
        anyhow::ensure!(x_vector.len() == self.dims as usize);
        let entry = *self.root.entry.read();
        let Some(u) = entry else { return Ok(Vec::new()) };
        let top = self._levels(u);
        let u = self._go(1..=top, u, &x_vector);
        let mut visited = self.visited.acquire();
        let mut result = self._search(&mut visited, &x_vector, u, k, 0);
        result.sort();
        Ok(result
            .iter()
            .map(|&(score, u)| (score, self.vectors.get_data(u)))
            .collect::<Vec<_>>())
    }
    pub fn insert(&self, x: usize) -> anyhow::Result<()> {
        let mut visited = self.visited.acquire();
        self._insert(&mut visited, x)
    }
    fn _go(&self, levels: RangeInclusive<u8>, u: usize, target: &[Scalar]) -> usize {
        let mut u = u;
        unsafe {
            std::intrinsics::prefetch_read_data(target.as_ptr(), 3);
        }
        let mut u_dis = self._dist0(u, target);
        for i in levels.rev() {
            let mut changed = true;
            while changed {
                changed = false;
                unsafe {
                    std::intrinsics::prefetch_read_data(
                        self.vectors.get_vector(u).as_ref().as_ptr(),
                        3,
                    );
                }
                let guard = self.root.vertexs[u][i as usize].read();
                for (_, v) in guard.iter().copied() {
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
    fn _insert(&self, visited: &mut Visited, insert: usize) -> anyhow::Result<()> {
        let vertexs = self.root.vertexs.as_ref();
        let vector = self.vectors.get_vector(insert);
        let levels = generate_random_levels(self.m, 63);
        let entry;
        let lock = {
            let cond = move |global: Option<usize>| {
                if let Some(u) = global {
                    self._levels(u) < levels
                } else {
                    true
                }
            };
            let lock = self.root.entry.read();
            if cond(*lock) {
                drop(lock);
                let lock = self.root.entry.write();
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
            let vertex = {
                let mut vertex = PBox::new_uninit_slice(1 + levels as usize, self.storage)?;
                for i in 0..=levels {
                    let array = PArray::new(1 + size_of_a_layer(self.m, i), self.storage)?;
                    vertex[i as usize].write(RwLock::new(array));
                }
                unsafe { vertex.assume_init() }
            };
            vertexs[insert].set(vertex);
            *lock.unwrap() = Some(insert);
            return Ok(());
        };
        let top = self._levels(u);
        if top > levels {
            u = self._go(levels + 1..=top, u, vector);
        }
        let mut layers = Vec::with_capacity(1 + levels as usize);
        for i in (0..=std::cmp::min(levels, top)).rev() {
            let mut layer = self._search(visited, vector, u, self.ef_construction, i);
            layer.sort();
            self._select0(&mut layer, size_of_a_layer(self.m, i))?;
            u = layer.first().unwrap().1;
            layers.push(layer);
        }
        layers.reverse();
        layers.resize_with(1 + levels as usize, Vec::new);
        let backup = layers.iter().map(|x| x.to_vec()).collect::<Vec<_>>();
        let vertex = {
            let mut vertex = PBox::new_uninit_slice(1 + levels as usize, self.storage)?;
            for i in 0..=levels {
                let mut array = PArray::new(1 + size_of_a_layer(self.m, i), self.storage)?;
                for &x in layers[i as usize].iter() {
                    array.push(x)?;
                }
                vertex[i as usize].write(RwLock::new(array));
            }
            unsafe { vertex.assume_init() }
        };
        vertexs[insert].set(vertex);
        for (i, layer) in backup.into_iter().enumerate() {
            let i = i as u8;
            for (n_dis, n) in layer.iter().copied() {
                let mut guard = vertexs[n][i as usize].write();
                orderedly_insert(&mut guard, (n_dis, insert))?;
                self._select1(&mut guard, size_of_a_layer(self.m, i))?;
            }
        }
        if let Some(mut lock) = lock {
            *lock = Some(insert);
        }
        Ok(())
    }
    fn _select0(&self, v: &mut Vec<(Scalar, usize)>, size: usize) -> anyhow::Result<()> {
        unsafe {
            std::intrinsics::prefetch_read_data(v.as_ptr(), 3);
        }
        if v.len() <= size {
            return Ok(());
        }
        let cloned = v.to_vec();
        v.clear();
        for (u_dis, u) in cloned.iter().copied() {
            if v.len() == size {
                break;
            }
            unsafe {
                std::intrinsics::prefetch_read_data(
                    self.vectors.get_vector(u).as_ref().as_ptr(),
                    3,
                );
            }
            let check = v
                .iter()
                .map(|&(_, v)| self._dist1(u, v))
                .all(|dist| dist > u_dis);
            if check {
                v.push((u_dis, u));
            }
        }
        Ok(())
    }
    fn _select1(&self, v: &mut PArray<(Scalar, usize)>, size: usize) -> anyhow::Result<()> {
        unsafe {
            std::intrinsics::prefetch_read_data(v.as_ptr(), 3);
        }
        if v.len() <= size {
            return Ok(());
        }
        let cloned = v.to_vec();
        v.clear();
        for (u_dis, u) in cloned.iter().copied() {
            if v.len() == size {
                break;
            }
            unsafe {
                std::intrinsics::prefetch_read_data(
                    self.vectors.get_vector(u).as_ref().as_ptr(),
                    3,
                );
            }
            let check = v
                .iter()
                .map(|&(_, v)| self._dist1(u, v))
                .all(|dist| dist > u_dis);
            if check {
                v.push((u_dis, u)).unwrap();
            }
        }
        Ok(())
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
        unsafe {
            std::intrinsics::prefetch_read_data(target.as_ptr(), 3);
        }
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
            let guard = self.root.vertexs[u][i as usize].read();
            for (_, v) in guard.iter().copied() {
                if visited.test(v) {
                    continue;
                }
                visited.set(v);
                unsafe {
                    std::intrinsics::prefetch_read_data(
                        self.vectors.get_vector(v).as_ref().as_ptr(),
                        3,
                    );
                }
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
    fn _dist0(&self, u: usize, target: &[Scalar]) -> Scalar {
        let u = self.vectors.get_vector(u);
        self.distance.distance(u, target)
    }
    fn _dist1(&self, u: usize, v: usize) -> Scalar {
        let u = self.vectors.get_vector(u);
        let v = self.vectors.get_vector(v);
        self.distance.distance(u, v)
    }
    fn _levels(&self, u: usize) -> u8 {
        self.root.vertexs[u].len() as u8 - 1
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

pub fn orderedly_insert<T: Ord>(a: &mut PArray<T>, element: T) -> anyhow::Result<usize> {
    let (Ok(index) | Err(index)) = a.binary_search(&element);
    a.insert(index, element)?;
    Ok(index)
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
