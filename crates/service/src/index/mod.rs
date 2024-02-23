pub mod delete;
pub mod indexing;
pub mod optimizing;
pub mod segments;

use self::delete::Delete;
use self::segments::growing::GrowingSegment;
use self::segments::sealed::SealedSegment;
use crate::index::optimizing::indexing::OptimizerIndexing;
use crate::index::optimizing::sealing::OptimizerSealing;
use crate::prelude::*;
use crate::utils::clean::clean;
use crate::utils::dir_ops::sync_dir;
use crate::utils::file_atomic::FileAtomic;
use crate::utils::tournament_tree::LoserTree;
use arc_swap::ArcSwap;
use crossbeam::atomic::AtomicCell;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
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
pub struct OutdatedError;

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
    pub fn create(path: PathBuf, options: IndexOptions) -> Result<Arc<Self>, CreateError> {
        if let Err(err) = options.validate() {
            return Err(CreateError::InvalidIndexOptions {
                reason: err.to_string(),
            });
        }
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
        Ok(index)
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
        IndexStat {
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
    pub fn basic<'a, F: Fn(Pointer) -> bool + Clone + 'a>(
        &'a self,
        vector: Borrowed<'_, S>,
        opts: &'a SearchOptions,
        filter: F,
    ) -> Result<impl Iterator<Item = Pointer> + 'a, BasicError> {
        if self.options.vector.dims != vector.dims() {
            return Err(BasicError::InvalidVector);
        }
        if let Err(err) = opts.validate() {
            return Err(BasicError::InvalidSearchOptions {
                reason: err.to_string(),
            });
        }

        struct Comparer(std::collections::BinaryHeap<Reverse<Element>>);

        impl Iterator for Comparer {
            type Item = Element;

            fn next(&mut self) -> Option<Self::Item> {
                self.0.pop().map(|Reverse(x)| x)
            }
        }

        struct Filtering<'a, F: 'a> {
            enable: bool,
            delete: &'a Delete,
            external: F,
        }

        impl<'a, F: Clone> Clone for Filtering<'a, F> {
            fn clone(&self) -> Self {
                Self {
                    enable: self.enable,
                    delete: self.delete,
                    external: self.external.clone(),
                }
            }
        }

        impl<'a, F: FnMut(Pointer) -> bool + Clone> Filter for Filtering<'a, F> {
            fn check(&mut self, payload: Payload) -> bool {
                !self.enable
                    || (self.delete.check(payload).is_some()
                        && (self.external)(Pointer::from_u48(payload >> 16)))
            }
        }

        let filter = Filtering {
            enable: opts.prefilter_enable,
            delete: &self.delete,
            external: filter,
        };

        let n = self.sealed.len() + self.growing.len() + 1;
        let mut heaps = Vec::with_capacity(1 + n);
        for (_, sealed) in self.sealed.iter() {
            let p = sealed.basic(vector, opts, filter.clone());
            heaps.push(Comparer(p));
        }
        for (_, growing) in self.growing.iter() {
            let p = growing.basic(vector, opts, filter.clone());
            heaps.push(Comparer(p));
        }
        if let Some((_, write)) = &self.write {
            let p = write.basic(vector, opts, filter.clone());
            heaps.push(Comparer(p));
        }
        let loser = LoserTree::new(heaps);
        Ok(loser.filter_map(|x| {
            if opts.prefilter_enable || self.delete.check(x.payload).is_some() {
                Some(Pointer::from_u48(x.payload >> 16))
            } else {
                None
            }
        }))
    }
    pub fn vbase<'a, F: FnMut(Pointer) -> bool + Clone + 'a>(
        &'a self,
        vector: Borrowed<'a, S>,
        opts: &'a SearchOptions,
        filter: F,
    ) -> Result<impl Iterator<Item = Pointer> + 'a, VbaseError> {
        if self.options.vector.dims != vector.dims() {
            return Err(VbaseError::InvalidVector);
        }
        if let Err(err) = opts.validate() {
            return Err(VbaseError::InvalidSearchOptions {
                reason: err.to_string(),
            });
        }

        struct Filtering<'a, F: 'a> {
            enable: bool,
            delete: &'a Delete,
            external: F,
        }

        impl<'a, F: Clone + 'a> Clone for Filtering<'a, F> {
            fn clone(&self) -> Self {
                Self {
                    enable: self.enable,
                    delete: self.delete,
                    external: self.external.clone(),
                }
            }
        }

        impl<'a, F: FnMut(Pointer) -> bool + Clone + 'a> Filter for Filtering<'a, F> {
            fn check(&mut self, payload: Payload) -> bool {
                !self.enable
                    || (self.delete.check(payload).is_some()
                        && (self.external)(Pointer::from_u48(payload >> 16)))
            }
        }

        let filter = Filtering {
            enable: opts.prefilter_enable,
            delete: &self.delete,
            external: filter,
        };

        let n = self.sealed.len() + self.growing.len() + 1;
        let mut alpha = Vec::new();
        let mut beta = Vec::with_capacity(1 + n);
        for (_, sealed) in self.sealed.iter() {
            let (stage1, stage2) = sealed.vbase(vector, opts, filter.clone());
            alpha.extend(stage1);
            beta.push(stage2);
        }
        for (_, growing) in self.growing.iter() {
            let (stage1, stage2) = growing.vbase(vector, opts, filter.clone());
            alpha.extend(stage1);
            beta.push(stage2);
        }
        if let Some((_, write)) = &self.write {
            let (stage1, stage2) = write.vbase(vector, opts, filter.clone());
            alpha.extend(stage1);
            beta.push(stage2);
        }
        alpha.sort_unstable();
        beta.push(Box::new(alpha.into_iter()));
        let loser = LoserTree::new(beta);
        Ok(loser.filter_map(|x| {
            if opts.prefilter_enable || self.delete.check(x.payload).is_some() {
                Some(Pointer::from_u48(x.payload >> 16))
            } else {
                None
            }
        }))
    }
    pub fn list(&self) -> Result<impl Iterator<Item = Pointer> + '_, ListError> {
        let sealed = self
            .sealed
            .values()
            .flat_map(|x| (0..x.len()).map(|i| x.payload(i)));
        let growing = self
            .growing
            .values()
            .flat_map(|x| (0..x.len()).map(|i| x.payload(i)));
        let write = self
            .write
            .iter()
            .map(|(_, x)| x)
            .flat_map(|x| (0..x.len()).map(|i| x.payload(i)));
        let iter = sealed
            .chain(growing)
            .chain(write)
            .filter_map(|p| self.delete.check(p));
        Ok(iter)
    }
    pub fn insert(
        &self,
        vector: Owned<S>,
        pointer: Pointer,
    ) -> Result<Result<(), OutdatedError>, InsertError> {
        if self.options.vector.dims != vector.dims() {
            return Err(InsertError::InvalidVector);
        }

        let payload = (pointer.as_u48() << 16) | self.delete.version(pointer) as Payload;
        if let Some((_, growing)) = self.write.as_ref() {
            use crate::index::segments::growing::GrowingSegmentInsertError;
            if let Err(GrowingSegmentInsertError) = growing.insert(vector, payload) {
                return Ok(Err(OutdatedError));
            }
            Ok(Ok(()))
        } else {
            Ok(Err(OutdatedError))
        }
    }
    pub fn delete(&self, p: Pointer) -> Result<(), DeleteError> {
        self.delete.delete(p);
        Ok(())
    }
    pub fn flush(&self) -> Result<(), FlushError> {
        self.delete.flush();
        if let Some((_, write)) = &self.write {
            write.flush();
        }
        Ok(())
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
