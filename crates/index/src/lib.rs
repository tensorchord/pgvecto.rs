#![feature(trait_alias)]
#![allow(clippy::len_without_is_empty)]

pub mod delete;
pub mod indexing;
pub mod optimizing;
pub mod segments;

mod utils;

use self::delete::Delete;
use self::segments::growing::GrowingSegment;
use self::segments::sealed::SealedSegment;
use crate::optimizing::indexing::OptimizerIndexing;
use crate::optimizing::sealing::OptimizerSealing;
use crate::utils::tournament_tree::LoserTree;
use arc_swap::ArcSwap;
pub use base::distance::*;
pub use base::index::*;
use base::operator::*;
pub use base::search::*;
pub use base::vector::*;
use common::clean::clean;
use common::dir_ops::sync_dir;
use common::file_atomic::FileAtomic;
use crossbeam::atomic::AtomicCell;
use crossbeam::channel::Sender;
use elkan_k_means::operator::OperatorElkanKMeans;
use parking_lot::Mutex;
use quantization::operator::OperatorQuantization;
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Instant;
use storage::operator::OperatorStorage;
use thiserror::Error;
use uuid::Uuid;
use validator::Validate;

pub trait Op = Operator + OperatorElkanKMeans + OperatorQuantization + OperatorStorage;

#[derive(Debug, Error)]
#[error("The index view is outdated.")]
pub struct OutdatedError;

pub struct Index<O: Op> {
    path: PathBuf,
    options: IndexOptions,
    delete: Arc<Delete>,
    protect: Mutex<IndexProtect<O>>,
    view: ArcSwap<IndexView<O>>,
    instant_index: AtomicCell<Instant>,
    instant_write: AtomicCell<Instant>,
    background_indexing: Mutex<Option<(Sender<Infallible>, JoinHandle<()>)>>,
    background_sealing: Mutex<Option<(Sender<Infallible>, JoinHandle<()>)>>,
    _tracker: Arc<IndexTracker>,
}

impl<O: Op> Index<O> {
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
            background_indexing: Mutex::new(None),
            background_sealing: Mutex::new(None),
            _tracker: Arc::new(IndexTracker { path }),
        });
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
                    SealedSegment::<O>::open(
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
        Arc::new(Index {
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
            background_indexing: Mutex::new(None),
            background_sealing: Mutex::new(None),
            _tracker: tracker,
        })
    }
    pub fn options(&self) -> &IndexOptions {
        &self.options
    }
    pub fn view(&self) -> Arc<IndexView<O>> {
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
    pub fn start(self: &Arc<Self>) {
        {
            let mut background_indexing = self.background_indexing.lock();
            if background_indexing.is_none() {
                *background_indexing = Some(OptimizerIndexing::new(self.clone()).spawn());
            }
        }
        {
            let mut background_sealing = self.background_sealing.lock();
            if background_sealing.is_none() {
                *background_sealing = Some(OptimizerSealing::new(self.clone()).spawn());
            }
        }
    }
    pub fn stop(&self) {
        {
            let mut background_indexing = self.background_indexing.lock();
            if let Some((sender, join_handle)) = background_indexing.take() {
                drop(sender);
                let _ = join_handle.join();
            }
        }
        {
            let mut background_sealing = self.background_sealing.lock();
            if let Some((sender, join_handle)) = background_sealing.take() {
                drop(sender);
                let _ = join_handle.join();
            }
        }
    }
    pub fn wait(&self) -> Arc<IndexTracker> {
        Arc::clone(&self._tracker)
    }
}

impl<O: Op> Drop for Index<O> {
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

pub struct IndexView<O: Op> {
    pub options: IndexOptions,
    pub delete: Arc<Delete>,
    pub sealed: HashMap<Uuid, Arc<SealedSegment<O>>>,
    pub growing: HashMap<Uuid, Arc<GrowingSegment<O>>>,
    pub write: Option<(Uuid, Arc<GrowingSegment<O>>)>,
}

impl<O: Op> IndexView<O> {
    pub fn basic<'a, F: Fn(Pointer) -> bool + Clone + 'a>(
        &'a self,
        vector: Borrowed<'_, O>,
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
                    || (self.delete.check(payload).is_some() && (self.external)(payload.pointer()))
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
                Some(x.payload.pointer())
            } else {
                None
            }
        }))
    }
    pub fn vbase<'a, F: FnMut(Pointer) -> bool + Clone + 'a>(
        &'a self,
        vector: Borrowed<'a, O>,
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
                    || (self.delete.check(payload).is_some() && (self.external)(payload.pointer()))
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
                Some(x.payload.pointer())
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
        vector: Owned<O>,
        pointer: Pointer,
    ) -> Result<Result<(), OutdatedError>, InsertError> {
        if self.options.vector.dims != vector.dims() {
            return Err(InsertError::InvalidVector);
        }

        let payload = Payload::new(pointer, self.delete.version(pointer));
        if let Some((_, growing)) = self.write.as_ref() {
            use crate::segments::growing::GrowingSegmentInsertError;
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

struct IndexProtect<O: Op> {
    startup: FileAtomic<IndexStartup>,
    sealed: HashMap<Uuid, Arc<SealedSegment<O>>>,
    growing: HashMap<Uuid, Arc<GrowingSegment<O>>>,
    write: Option<(Uuid, Arc<GrowingSegment<O>>)>,
}

impl<O: Op> IndexProtect<O> {
    fn maintain(
        &mut self,
        options: IndexOptions,
        delete: Arc<Delete>,
        swap: &ArcSwap<IndexView<O>>,
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
