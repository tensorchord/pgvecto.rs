pub mod delete;
pub mod indexing;
pub mod optimizing;
pub mod segments;

use self::delete::Delete;
use self::indexing::IndexingOptions;
use self::optimizing::OptimizingOptions;
use self::segments::growing::GrowingSegment;
use self::segments::growing::GrowingSegmentInsertError;
use self::segments::sealed::SealedSegment;
use self::segments::SegmentsOptions;
use crate::prelude::*;
use crate::utils::clean::clean;
use crate::utils::dir_ops::sync_dir;
use crate::utils::file_atomic::FileAtomic;
use arc_swap::ArcSwap;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Weak};
use std::time::Duration;
use thiserror::Error;
use uuid::Uuid;
use validator::Validate;

#[derive(Debug, Error)]
pub enum IndexInsertError {
    #[error("The vector is invalid.")]
    InvalidVector(Vec<Scalar>),
    #[error("The index view is outdated.")]
    OutdatedView(#[from] Option<GrowingSegmentInsertError>),
}

#[derive(Debug, Error)]
pub enum IndexSearchError {
    #[error("The vector is invalid.")]
    InvalidVector(Vec<Scalar>),
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct VectorOptions {
    #[validate(range(min = 1, max = 65535))]
    #[serde(rename = "dimensions")]
    pub dims: u16,
    #[serde(rename = "distance")]
    pub d: Distance,
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct IndexOptions {
    #[validate]
    pub vector: VectorOptions,
    #[validate]
    pub segment: SegmentsOptions,
    #[validate]
    pub optimizing: OptimizingOptions,
    #[validate]
    pub indexing: IndexingOptions,
}

pub struct Index {
    path: PathBuf,
    options: IndexOptions,
    delete: Arc<Delete>,
    protect: Mutex<IndexProtect>,
    view: ArcSwap<IndexView>,
    _tracker: Arc<IndexTracker>,
}

impl Index {
    pub fn create(path: PathBuf, options: IndexOptions) -> Arc<Self> {
        assert!(options.validate().is_ok());
        std::fs::create_dir(&path).unwrap();
        std::fs::create_dir(path.join("segments")).unwrap();
        let startup = FileAtomic::create(
            path.join("startup"),
            IndexStartup {
                sealeds: HashSet::new(),
                growings: HashSet::new(),
            },
        );
        let delete = Delete::create(path.join("delete"));
        sync_dir(&path);
        let index = Arc::new(Index {
            path: path.clone(),
            options: options.clone(),
            delete: delete.clone(),
            protect: Mutex::new(IndexProtect {
                startup,
                sealed: HashMap::new(),
                growing: HashMap::new(),
                write: None,
            }),
            view: ArcSwap::new(Arc::new(IndexView {
                options: options.clone(),
                sealed: HashMap::new(),
                growing: HashMap::new(),
                delete: delete.clone(),
                write: None,
            })),
            _tracker: Arc::new(IndexTracker { path }),
        });
        IndexBackground {
            index: Arc::downgrade(&index),
        }
        .spawn();
        index
    }
    pub fn open(path: PathBuf, options: IndexOptions) -> Arc<Self> {
        let tracker = Arc::new(IndexTracker { path: path.clone() });
        let startup = FileAtomic::<IndexStartup>::open(path.join("startup"));
        clean(
            path.join("segments"),
            startup
                .get()
                .sealeds
                .iter()
                .map(|s| s.to_string())
                .chain(startup.get().growings.iter().map(|s| s.to_string())),
        );
        let sealed = startup
            .get()
            .sealeds
            .iter()
            .map(|&uuid| {
                (
                    uuid,
                    SealedSegment::open(
                        tracker.clone(),
                        path.join("segments").join(uuid.to_string()),
                        uuid,
                        options.clone(),
                    ),
                )
            })
            .collect::<HashMap<_, _>>();
        let growing = startup
            .get()
            .growings
            .iter()
            .map(|&uuid| {
                (
                    uuid,
                    GrowingSegment::open(
                        tracker.clone(),
                        path.join("segments").join(uuid.to_string()),
                        uuid,
                        options.clone(),
                    ),
                )
            })
            .collect::<HashMap<_, _>>();
        let delete = Delete::open(path.join("delete"));
        let index = Arc::new(Index {
            path: path.clone(),
            options: options.clone(),
            delete: delete.clone(),
            protect: Mutex::new(IndexProtect {
                startup,
                sealed: sealed.clone(),
                growing: growing.clone(),
                write: None,
            }),
            view: ArcSwap::new(Arc::new(IndexView {
                options: options.clone(),
                delete: delete.clone(),
                sealed,
                growing,
                write: None,
            })),
            _tracker: tracker,
        });
        IndexBackground {
            index: Arc::downgrade(&index),
        }
        .spawn();
        index
    }
    pub fn options(&self) -> &IndexOptions {
        &self.options
    }
    pub fn view(&self) -> Arc<IndexView> {
        self.view.load_full()
    }
    pub fn refresh(&self) {
        let mut protect = self.protect.lock();
        if let Some((uuid, write)) = protect.write.clone() {
            write.seal();
            protect.growing.insert(uuid, write);
        }
        let write_segment_uuid = Uuid::new_v4();
        let write_segment = GrowingSegment::create(
            self._tracker.clone(),
            self.path
                .join("segments")
                .join(write_segment_uuid.to_string()),
            write_segment_uuid,
            self.options.clone(),
        );
        protect.write = Some((write_segment_uuid, write_segment));
        protect.maintain(self.options.clone(), self.delete.clone(), &self.view);
    }
}

#[derive(Debug, Clone)]
pub struct IndexTracker {
    path: PathBuf,
}

impl Drop for IndexTracker {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.path).unwrap();
    }
}

pub struct IndexView {
    options: IndexOptions,
    delete: Arc<Delete>,
    sealed: HashMap<Uuid, Arc<SealedSegment>>,
    growing: HashMap<Uuid, Arc<GrowingSegment>>,
    write: Option<(Uuid, Arc<GrowingSegment>)>,
}

impl IndexView {
    #[allow(dead_code)]
    pub fn len(&self) -> u32 {
        self.sealed.iter().map(|(_, x)| x.len()).sum::<u32>()
            + self.growing.iter().map(|(_, x)| x.len()).sum::<u32>()
    }
    pub fn search<F: FnMut(Pointer) -> bool>(
        &self,
        k: usize,
        vector: &[Scalar],
        mut f: F,
    ) -> Result<Vec<Pointer>, IndexSearchError> {
        if self.options.vector.dims as usize != vector.len() {
            return Err(IndexSearchError::InvalidVector(vector.to_vec()));
        }

        struct Comparer(BinaryHeap<Reverse<HeapElement>>);

        impl PartialEq for Comparer {
            fn eq(&self, other: &Self) -> bool {
                self.cmp(other).is_eq()
            }
        }

        impl Eq for Comparer {}

        impl PartialOrd for Comparer {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }

        impl Ord for Comparer {
            fn cmp(&self, other: &Self) -> Ordering {
                self.0.peek().cmp(&other.0.peek()).reverse()
            }
        }

        let mut filter = |data| {
            if let Some(p) = self.delete.check(data) {
                f(p)
            } else {
                false
            }
        };
        let n = self.sealed.len() + self.growing.len() + 1;
        let mut result = Heap::new(k);
        let mut heaps = BinaryHeap::with_capacity(1 + n);
        for (_, sealed) in self.sealed.iter() {
            let p = sealed.search(k, vector, &mut filter).into_reversed_heap();
            heaps.push(Comparer(p));
        }
        for (_, growing) in self.growing.iter() {
            let p = growing.search(k, vector, &mut filter).into_reversed_heap();
            heaps.push(Comparer(p));
        }
        if let Some((_, write)) = &self.write {
            let p = write.search(k, vector, &mut filter).into_reversed_heap();
            heaps.push(Comparer(p));
        }
        while let Some(Comparer(mut heap)) = heaps.pop() {
            if let Some(Reverse(x)) = heap.pop() {
                result.push(x);
                heaps.push(Comparer(heap));
            }
        }
        Ok(result
            .into_sorted_vec()
            .iter()
            .map(|x| Pointer::from_u48(x.data >> 16))
            .collect())
    }
    pub fn insert(&self, vector: Vec<Scalar>, pointer: Pointer) -> Result<(), IndexInsertError> {
        if self.options.vector.dims as usize != vector.len() {
            return Err(IndexInsertError::InvalidVector(vector));
        }
        let data = (pointer.as_u48() << 16) | self.delete.version(pointer) as u64;
        if let Some((_, growing)) = self.write.as_ref() {
            Ok(growing.insert(vector, data)?)
        } else {
            Err(IndexInsertError::OutdatedView(None))
        }
    }
    pub fn delete<F: FnMut(Pointer) -> bool>(&self, mut f: F) {
        for (_, sealed) in self.sealed.iter() {
            let n = sealed.len();
            for i in 0..n {
                if let Some(p) = self.delete.check(sealed.data(i)) {
                    if f(p) {
                        self.delete.delete(p);
                    }
                }
            }
        }
        for (_, growing) in self.growing.iter() {
            let n = growing.len();
            for i in 0..n {
                if let Some(p) = self.delete.check(growing.data(i)) {
                    if f(p) {
                        self.delete.delete(p);
                    }
                }
            }
        }
        if let Some((_, write)) = &self.write {
            let n = write.len();
            for i in 0..n {
                if let Some(p) = self.delete.check(write.data(i)) {
                    if f(p) {
                        self.delete.delete(p);
                    }
                }
            }
        }
    }
    pub fn flush(&self) -> Result<(), IndexInsertError> {
        self.delete.flush();
        if let Some((_, write)) = &self.write {
            write.flush();
        }
        Ok(())
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct IndexStartup {
    sealeds: HashSet<Uuid>,
    growings: HashSet<Uuid>,
}

struct IndexProtect {
    startup: FileAtomic<IndexStartup>,
    sealed: HashMap<Uuid, Arc<SealedSegment>>,
    growing: HashMap<Uuid, Arc<GrowingSegment>>,
    write: Option<(Uuid, Arc<GrowingSegment>)>,
}

impl IndexProtect {
    fn maintain(&mut self, options: IndexOptions, delete: Arc<Delete>, swap: &ArcSwap<IndexView>) {
        let view: Arc<IndexView> = Arc::new(IndexView {
            options,
            delete,
            sealed: self.sealed.clone(),
            growing: self.growing.clone(),
            write: self.write.clone(),
        });
        let startup_write = self.write.as_ref().map(|(uuid, _)| *uuid);
        let startup_sealeds = self.sealed.iter().map(|(uuid, _)| *uuid).collect();
        let startup_growings = self
            .growing
            .iter()
            .map(|(uuid, _)| *uuid)
            .chain(startup_write)
            .collect();
        self.startup.set(IndexStartup {
            sealeds: startup_sealeds,
            growings: startup_growings,
        });
        swap.swap(view);
    }
}

pub struct IndexBackground {
    index: Weak<Index>,
}

impl IndexBackground {
    pub fn main(self) {
        let pool;
        if let Some(index) = self.index.upgrade() {
            pool = rayon::ThreadPoolBuilder::new()
                .num_threads(index.options.optimizing.optimizing_threads)
                .build()
                .unwrap();
        } else {
            return;
        }
        while let Some(index) = self.index.upgrade() {
            pool.install(|| {
                optimizing::indexing::optimizing_indexing(index.clone());
            });
            std::thread::sleep(Duration::from_secs(60));
        }
    }
    pub fn spawn(self) {
        std::thread::spawn(move || {
            self.main();
        });
    }
}
