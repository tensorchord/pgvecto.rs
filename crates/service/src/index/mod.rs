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
use crate::index::optimizing::indexing::OptimizerIndexing;
use crate::index::optimizing::sealing::OptimizerSealing;
use crate::prelude::*;
use crate::utils::clean::clean;
use crate::utils::dir_ops::sync_dir;
use crate::utils::file_atomic::FileAtomic;
use crate::utils::iter::RefPeekable;
use arc_swap::ArcSwap;
use crossbeam::atomic::AtomicCell;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use thiserror::Error;
use uuid::Uuid;
use validator::Validate;

#[derive(Debug, Error)]
#[error("The index view is outdated.")]
pub struct OutdatedError(#[from] pub Option<GrowingSegmentInsertError>);

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct VectorOptions {
    #[validate(range(min = 1, max = 65535))]
    #[serde(rename = "dimensions")]
    pub dims: u16,
    #[serde(rename = "distance")]
    pub d: Distance,
    #[serde(rename = "kind")]
    pub k: Kind,
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
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

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct SearchOptions {
    #[validate(range(min = 1, max = 65535))]
    pub search_k: usize,
    #[validate(range(min = 1, max = 65535))]
    pub vbase_range: usize,
    #[validate(range(min = 1, max = 1_000_000))]
    pub ivf_nprobe: u32,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SegmentStat {
    pub id: Uuid,
    #[serde(rename = "type")]
    pub typ: String,
    pub length: usize,
    pub size: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum IndexStat {
    Normal {
        indexing: bool,
        segments: Vec<SegmentStat>,
        options: IndexOptions,
    },
    Upgrade,
}

pub struct Index<S: G> {
    path: PathBuf,
    options: IndexOptions,
    delete: Arc<Delete>,
    protect: Mutex<IndexProtect<S>>,
    view: ArcSwap<IndexView<S>>,
    instant_index: AtomicCell<Instant>,
    instant_write: AtomicCell<Instant>,
    _tracker: Arc<IndexTracker>,
}

impl<S: G> Index<S> {
    pub fn create(path: PathBuf, options: IndexOptions) -> Arc<Self> {
        assert!(options.validate().is_ok());
        std::fs::create_dir(&path).unwrap();
        std::fs::write(
            path.join("options"),
            serde_json::to_string::<IndexOptions>(&options).unwrap(),
        )
        .unwrap();
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
            instant_index: AtomicCell::new(Instant::now()),
            instant_write: AtomicCell::new(Instant::now()),
            _tracker: Arc::new(IndexTracker { path }),
        });
        OptimizerIndexing::new(index.clone()).spawn();
        OptimizerSealing::new(index.clone()).spawn();
        index
    }
    pub fn open(path: PathBuf) -> Arc<Self> {
        let options =
            serde_json::from_slice::<IndexOptions>(&std::fs::read(path.join("options")).unwrap())
                .unwrap();
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
            instant_index: AtomicCell::new(Instant::now()),
            instant_write: AtomicCell::new(Instant::now()),
            _tracker: tracker,
        });
        OptimizerIndexing::new(index.clone()).spawn();
        OptimizerSealing::new(index.clone()).spawn();
        index
    }
    pub fn options(&self) -> &IndexOptions {
        &self.options
    }
    pub fn view(&self) -> Arc<IndexView<S>> {
        self.view.load_full()
    }
    pub fn refresh(&self) {
        let mut protect = self.protect.lock();
        if let Some((uuid, write)) = protect.write.clone() {
            if !write.is_full() {
                return;
            }
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
        self.instant_write.store(Instant::now());
    }
    pub fn seal(&self, check: Uuid) {
        let mut protect = self.protect.lock();
        if let Some((uuid, write)) = protect.write.clone() {
            if check != uuid {
                return;
            }
            write.seal();
            protect.growing.insert(uuid, write);
        }
        protect.write = None;
        protect.maintain(self.options.clone(), self.delete.clone(), &self.view);
        self.instant_write.store(Instant::now());
    }
    pub fn stat(&self) -> IndexStat {
        let view = self.view();
        IndexStat::Normal {
            indexing: self.instant_index.load() < self.instant_write.load(),
            options: self.options().clone(),
            segments: {
                let mut segments = Vec::new();
                for sealed in view.sealed.values() {
                    segments.push(sealed.stat_sealed());
                }
                for growing in view.growing.values() {
                    segments.push(growing.stat_growing());
                }
                if let Some(write) = view.write.as_ref().map(|(_, x)| x) {
                    segments.push(write.stat_write());
                }
                segments
            },
        }
    }
}

impl<S: G> Drop for Index<S> {
    fn drop(&mut self) {}
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

pub struct IndexView<S: G> {
    pub options: IndexOptions,
    pub delete: Arc<Delete>,
    pub sealed: HashMap<Uuid, Arc<SealedSegment<S>>>,
    pub growing: HashMap<Uuid, Arc<GrowingSegment<S>>>,
    pub write: Option<(Uuid, Arc<GrowingSegment<S>>)>,
}

impl<S: G> IndexView<S> {
    pub fn search<F: FnMut(Pointer) -> bool>(
        &self,
        vector: &[S::Scalar],
        opts: &SearchOptions,
        mut filter: F,
    ) -> Vec<Pointer> {
        assert_eq!(self.options.vector.dims as usize, vector.len());

        struct Comparer(BinaryHeap<Reverse<HeapElement>>);

        impl PartialEq for Comparer {
            fn eq(&self, other: &Self) -> bool {
                self.cmp(other).is_eq()
            }
        }

        impl Eq for Comparer {}

        impl PartialOrd for Comparer {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }

        impl Ord for Comparer {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.0.peek().cmp(&other.0.peek()).reverse()
            }
        }

        let mut filter = |payload| {
            if let Some(p) = self.delete.check(payload) {
                filter(p)
            } else {
                false
            }
        };
        let n = self.sealed.len() + self.growing.len() + 1;
        let mut result = Heap::new(opts.search_k);
        let mut heaps = BinaryHeap::with_capacity(1 + n);
        for (_, sealed) in self.sealed.iter() {
            let p = sealed
                .search(vector, opts, &mut filter)
                .into_reversed_heap();
            heaps.push(Comparer(p));
        }
        for (_, growing) in self.growing.iter() {
            let p = growing
                .search(vector, opts, &mut filter)
                .into_reversed_heap();
            heaps.push(Comparer(p));
        }
        if let Some((_, write)) = &self.write {
            let p = write.search(vector, opts, &mut filter).into_reversed_heap();
            heaps.push(Comparer(p));
        }
        while let Some(Comparer(mut heap)) = heaps.pop() {
            if let Some(Reverse(x)) = heap.pop() {
                result.push(x);
                heaps.push(Comparer(heap));
            }
        }
        result
            .into_sorted_vec()
            .iter()
            .map(|x| Pointer::from_u48(x.payload >> 16))
            .collect()
    }
    pub fn vbase<'a>(
        &'a self,
        vector: &'a [S::Scalar],
        opts: &'a SearchOptions,
    ) -> impl Iterator<Item = Pointer> + 'a {
        assert_eq!(self.options.vector.dims as usize, vector.len());

        struct Comparer<'a>(RefPeekable<Box<dyn Iterator<Item = HeapElement> + 'a>>);

        impl PartialEq for Comparer<'_> {
            fn eq(&self, other: &Self) -> bool {
                self.cmp(other).is_eq()
            }
        }

        impl Eq for Comparer<'_> {}

        impl PartialOrd for Comparer<'_> {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }

        impl Ord for Comparer<'_> {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.0.peek().cmp(&other.0.peek()).reverse()
            }
        }

        let filter = |payload| self.delete.check(payload).is_some();
        let n = self.sealed.len() + self.growing.len() + 1;
        let mut alpha = Vec::new();
        let mut beta = BinaryHeap::with_capacity(1 + n);
        for (_, sealed) in self.sealed.iter() {
            let (stage1, stage2) = sealed.vbase(vector, opts);
            alpha.extend(stage1);
            beta.push(Comparer(RefPeekable::new(stage2)));
        }
        for (_, growing) in self.growing.iter() {
            let (stage1, stage2) = growing.vbase(vector);
            alpha.extend(stage1);
            beta.push(Comparer(RefPeekable::new(stage2)));
        }
        if let Some((_, write)) = &self.write {
            let (stage1, stage2) = write.vbase(vector);
            alpha.extend(stage1);
            beta.push(Comparer(RefPeekable::new(stage2)));
        }
        alpha.sort_unstable();
        beta.push(Comparer(RefPeekable::new(Box::new(alpha.into_iter()))));
        std::iter::from_fn(move || {
            while let Some(mut iter) = beta.pop() {
                if let Some(x) = iter.0.next() {
                    if !filter(x.payload) {
                        continue;
                    }
                    beta.push(iter);
                    return Some(Pointer::from_u48(x.payload >> 16));
                }
            }
            None
        })
    }
    pub fn insert(&self, vector: Vec<S::Scalar>, pointer: Pointer) -> Result<(), OutdatedError> {
        assert_eq!(self.options.vector.dims as usize, vector.len());
        let payload = (pointer.as_u48() << 16) | self.delete.version(pointer) as Payload;
        if let Some((_, growing)) = self.write.as_ref() {
            Ok(growing.insert(vector, payload)?)
        } else {
            Err(OutdatedError(None))
        }
    }
    pub fn delete<F: FnMut(Pointer) -> bool>(&self, mut f: F) {
        for (_, sealed) in self.sealed.iter() {
            let n = sealed.len();
            for i in 0..n {
                if let Some(p) = self.delete.check(sealed.payload(i)) {
                    if f(p) {
                        self.delete.delete(p);
                    }
                }
            }
        }
        for (_, growing) in self.growing.iter() {
            let n = growing.len();
            for i in 0..n {
                if let Some(p) = self.delete.check(growing.payload(i)) {
                    if f(p) {
                        self.delete.delete(p);
                    }
                }
            }
        }
        if let Some((_, write)) = &self.write {
            let n = write.len();
            for i in 0..n {
                if let Some(p) = self.delete.check(write.payload(i)) {
                    if f(p) {
                        self.delete.delete(p);
                    }
                }
            }
        }
    }
    pub fn flush(&self) {
        self.delete.flush();
        if let Some((_, write)) = &self.write {
            write.flush();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IndexStartup {
    sealeds: HashSet<Uuid>,
    growings: HashSet<Uuid>,
}

struct IndexProtect<S: G> {
    startup: FileAtomic<IndexStartup>,
    sealed: HashMap<Uuid, Arc<SealedSegment<S>>>,
    growing: HashMap<Uuid, Arc<GrowingSegment<S>>>,
    write: Option<(Uuid, Arc<GrowingSegment<S>>)>,
}

impl<S: G> IndexProtect<S> {
    fn maintain(
        &mut self,
        options: IndexOptions,
        delete: Arc<Delete>,
        swap: &ArcSwap<IndexView<S>>,
    ) {
        let view = Arc::new(IndexView {
            options,
            delete,
            sealed: self.sealed.clone(),
            growing: self.growing.clone(),
            write: self.write.clone(),
        });
        let startup_write = self.write.as_ref().map(|(uuid, _)| *uuid);
        let startup_sealeds = self.sealed.keys().copied().collect();
        let startup_growings = self.growing.keys().copied().chain(startup_write).collect();
        self.startup.set(IndexStartup {
            sealeds: startup_sealeds,
            growings: startup_growings,
        });
        swap.swap(view);
    }
}
