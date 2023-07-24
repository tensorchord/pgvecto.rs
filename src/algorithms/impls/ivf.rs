use super::kmeans::Kmeans;
use crate::algorithms::Vectors;
use crate::memory::Address;
use crate::memory::PBox;
use crate::memory::Persistent;
use crate::memory::Ptr;
use crate::prelude::*;
use crate::utils::fixed_heap::FixedHeap;
use crate::utils::unsafe_once::UnsafeOnce;
use crate::utils::vec2::Vec2;
use crossbeam::atomic::AtomicCell;
use rand::seq::index::sample;
use rand::thread_rng;
use std::sync::Arc;

struct List {
    centroid: PBox<[Scalar]>,
    head: AtomicCell<Option<usize>>,
}

type Vertex = Option<usize>;

pub struct Root {
    lists: PBox<[List]>,
    vertexs: PBox<[UnsafeOnce<Vertex>]>,
}

static_assertions::assert_impl_all!(Root: Persistent);

pub struct IvfImpl {
    pub address: Address,
    root: &'static Root,
    vectors: Arc<Vectors>,
    distance: Distance,
    nprobe: usize,
}

impl IvfImpl {
    pub fn new(
        vectors: Arc<Vectors>,
        dims: u16,
        distance: Distance,
        n: usize,
        nlist: usize,
        nsample: usize,
        nprobe: usize,
        least_iterations: usize,
        iterations: usize,
        capacity: usize,
        storage: Storage,
    ) -> anyhow::Result<Self> {
        let m = std::cmp::min(nsample, n);
        let f = sample(&mut thread_rng(), n, m).into_vec();
        let mut samples = Vec2::new(dims, m);
        for i in 0..m {
            samples[i].copy_from_slice(vectors.get_vector(f[i]));
            distance.kmeans_normalize(&mut samples[i]);
        }
        let mut kmeans = Kmeans::new(distance, dims, nlist, samples);
        for _ in 0..least_iterations {
            kmeans.iterate();
        }
        for _ in least_iterations..iterations {
            if kmeans.iterate() {
                break;
            }
        }
        let centroids = kmeans.finish();
        let ptr = PBox::new(
            Root {
                lists: {
                    let mut lists = PBox::new_zeroed_slice(nlist, storage)?;
                    for i in 0..nlist {
                        lists[i].write(List {
                            centroid: {
                                let mut centroid = unsafe {
                                    PBox::new_zeroed_slice(dims as _, storage)?.assume_init()
                                };
                                centroid.copy_from_slice(&centroids[i]);
                                centroid
                            },
                            head: AtomicCell::new(None),
                        });
                    }
                    unsafe { lists.assume_init() }
                },
                vertexs: {
                    let vertexs = PBox::new_zeroed_slice(capacity, storage)?;
                    unsafe { vertexs.assume_init() }
                },
            },
            storage,
        )?
        .into_raw();
        Ok(Self {
            address: ptr.address(),
            root: unsafe { ptr.as_ref() },
            vectors,
            distance,
            nprobe,
        })
    }
    pub fn load(
        vectors: Arc<Vectors>,
        distance: Distance,
        address: Address,
        nprobe: usize,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            address,
            root: unsafe { Ptr::new(address, ()).as_ref() },
            vectors,
            distance,
            nprobe,
        })
    }
    pub fn search(
        &self,
        (mut x_vector, k): (Box<[Scalar]>, usize),
    ) -> anyhow::Result<Vec<(Scalar, u64)>> {
        let vectors = self.vectors.as_ref();
        self.distance.kmeans_normalize(&mut x_vector);
        let mut lists = FixedHeap::new(self.nprobe);
        for (i, list) in self.root.lists.iter().enumerate() {
            let dis = self.distance.kmeans_distance(&x_vector, &list.centroid);
            lists.push((dis, i));
        }
        let mut result = FixedHeap::new(k);
        for (_, i) in lists.into_vec().into_iter() {
            let mut cursor = self.root.lists[i].head.load();
            while let Some(u) = cursor {
                let u_vector = vectors.get_vector(u);
                let u_data = vectors.get_data(u);
                let u_dis = self.distance.distance(&x_vector, u_vector);
                result.push((u_dis, u_data));
                cursor = *self.root.vertexs[u];
            }
        }
        Ok(result.into_sorted_vec())
    }
    pub fn insert(&self, x: usize) -> anyhow::Result<()> {
        self._insert(x)?;
        Ok(())
    }
    pub fn _insert(&self, x: usize) -> anyhow::Result<()> {
        let vertexs = self.root.vertexs.as_ref();
        let mut x_vector = self.vectors.get_vector(x).to_vec();
        self.distance.kmeans_normalize(&mut x_vector);
        let mut result = (Scalar::INFINITY, 0);
        for (i, list) in self.root.lists.iter().enumerate() {
            let dis = self.distance.kmeans_distance(&x_vector, &list.centroid);
            result = std::cmp::min(result, (dis, i));
        }
        loop {
            let next = self.root.lists[result.1].head.load();
            vertexs[x].set(next);
            let list = &self.root.lists[result.1];
            if list.head.compare_exchange(next, Some(x)).is_ok() {
                break;
            }
        }
        Ok(())
    }
}
