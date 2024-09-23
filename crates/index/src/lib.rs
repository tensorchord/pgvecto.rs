#![allow(clippy::len_without_is_empty)]

pub mod delete;
pub mod optimizing;
pub mod segment;

mod utils;

use self::delete::Delete;
use self::segment::growing::GrowingSegment;
use self::segment::sealed::SealedSegment;
use crate::optimizing::Optimizing;
use crate::utils::tournament_tree::LoserTree;
use arc_swap::ArcSwap;
use base::distance::Distance;
use base::index::*;
use base::operator::*;
use base::search::*;
use base::vector::*;
use common::clean::clean;
use common::clean::clean_files;
use common::dir_ops::sync_dir;
use common::dir_ops::sync_walk_from_dir;
use common::file_atomic::FileAtomic;
use crossbeam::atomic::AtomicCell;
use crossbeam::channel::Sender;
use indexing::OperatorIndexing;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::Infallible;
use std::num::NonZeroU128;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Instant;
use thiserror::Error;
use validator::Validate;

pub trait Op: OperatorIndexing {}

impl<T: OperatorIndexing> Op for T {}

#[derive(Debug, Error)]
#[error("The index view is outdated.")]
pub struct OutdatedError;

pub struct Index<O: Op> {
    path: PathBuf,
    options: IndexOptions,
    delete: Arc<Delete>,
    protect: Mutex<IndexProtect<O>>,
    view: ArcSwap<IndexView<O>>,
    instant_indexed: AtomicCell<Instant>,
    instant_written: AtomicCell<Instant>,
    check_deleted: AtomicCell<bool>,
    optimizing: Mutex<Option<(Sender<Infallible>, JoinHandle<()>)>>,
    _tracker: Arc<IndexTracker>,
}

impl<O: Op> Index<O> {
    pub fn create(
        path: PathBuf,
        options: IndexOptions,
        alterable_options: IndexAlterableOptions,
    ) -> Result<Arc<Self>, CreateError> {
        if let Err(err) = options.validate() {
            return Err(CreateError::InvalidIndexOptions {
                reason: err.to_string(),
            });
        }
        if let Err(e) = alterable_options.validate() {
            return Err(CreateError::InvalidIndexOptions {
                reason: e.to_string(),
            });
        }
        std::fs::create_dir(&path).unwrap();
        std::fs::write(
            path.join("options"),
            serde_json::to_string::<IndexOptions>(&options).unwrap(),
        )
        .unwrap();
        std::fs::create_dir(path.join("sealed_segments")).unwrap();
        std::fs::create_dir(path.join("wal")).unwrap();
        let startup = FileAtomic::create(
            path.join("startup"),
            IndexStartup {
                sealed_segment_ids: HashSet::new(),
                growing_segment_ids: HashSet::new(),
                alterable_options: alterable_options.clone(),
                sealed_counter: NonZeroU128::new(1).unwrap(),
                growing_counter: NonZeroU128::new(1).unwrap(),
            },
        );
        let delete = Delete::create(path.join("delete"));
        sync_walk_from_dir(&path);
        let index = Arc::new(Index {
            path: path.clone(),
            options: options.clone(),
            delete: delete.clone(),
            protect: Mutex::new(IndexProtect {
                startup,
                sealed_segments: HashMap::new(),
                read_segments: HashMap::new(),
                write_segment: None,
                alterable_options: alterable_options.clone(),
                sealed_counter: NonZeroU128::new(1).unwrap(),
                growing_counter: NonZeroU128::new(1).unwrap(),
            }),
            view: ArcSwap::new(Arc::new(IndexView {
                options: options.clone(),
                alterable_options: alterable_options.clone(),
                sealed_segments: HashMap::new(),
                read_segments: HashMap::new(),
                delete: delete.clone(),
                write_segment: None,
            })),
            instant_indexed: AtomicCell::new(Instant::now()),
            instant_written: AtomicCell::new(Instant::now()),
            check_deleted: AtomicCell::new(false),
            optimizing: Mutex::new(None),
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
        let alterable_options = startup.get().alterable_options.clone();
        clean(
            path.join("sealed_segments"),
            startup
                .get()
                .sealed_segment_ids
                .iter()
                .map(|s| s.to_string()),
        );
        clean_files(
            path.join("wal"),
            startup
                .get()
                .growing_segment_ids
                .iter()
                .map(|s| s.to_string()),
        );
        let sealed_segments = startup
            .get()
            .sealed_segment_ids
            .iter()
            .map(|&id| {
                (
                    id,
                    SealedSegment::<O>::open(
                        tracker.clone(),
                        path.join("sealed_segments").join(id.to_string()),
                        id,
                        options.clone(),
                    ),
                )
            })
            .collect::<HashMap<_, _>>();
        let read_segments = startup
            .get()
            .growing_segment_ids
            .iter()
            .map(|&id| {
                (
                    id,
                    GrowingSegment::open(
                        tracker.clone(),
                        path.join("wal").join(id.to_string()),
                        id,
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
                sealed_segments: sealed_segments.clone(),
                read_segments: read_segments.clone(),
                write_segment: None,
                alterable_options: alterable_options.clone(),
                sealed_counter: startup.get().sealed_counter,
                growing_counter: startup.get().growing_counter,
                startup,
            }),
            view: ArcSwap::new(Arc::new(IndexView {
                options: options.clone(),
                alterable_options: alterable_options.clone(),
                delete: delete.clone(),
                sealed_segments,
                read_segments,
                write_segment: None,
            })),
            instant_indexed: AtomicCell::new(Instant::now()),
            instant_written: AtomicCell::new(Instant::now()),
            check_deleted: AtomicCell::new(false),
            optimizing: Mutex::new(None),
            _tracker: tracker,
        })
    }
    pub fn options(&self) -> &IndexOptions {
        &self.options
    }
    pub fn view(&self) -> Arc<IndexView<O>> {
        self.view.load_full()
    }
    pub fn alter(self: &Arc<Self>, key: &str, value: &str) -> Result<(), AlterError> {
        let mut protect = self.protect.lock();
        let mut alterable_options = protect.alterable_options.clone();
        let key = key.split('.').collect::<Vec<_>>();
        alterable_options.alter(key.as_slice(), value)?;
        if let Err(e) = alterable_options.validate() {
            return Err(AlterError::InvalidIndexOptions {
                reason: e.to_string(),
            });
        }
        protect.alterable_options = alterable_options;
        protect.maintain(self.options.clone(), self.delete.clone(), &self.view);
        Ok(())
    }
    pub fn refresh(&self) {
        let mut protect = self.protect.lock();
        if let Some((id, write)) = protect.write_segment.clone() {
            if !write.is_full() {
                return;
            }
            write.seal();
            protect.read_segments.insert(id, write);
        }
        let write_segment_id = protect.growing_counter;
        protect.growing_counter = protect.growing_counter.checked_add(1).unwrap();
        let write_segment = GrowingSegment::create(
            self._tracker.clone(),
            self.path.join("wal").join(write_segment_id.to_string()),
            write_segment_id,
            protect.alterable_options.segment.max_growing_segment_size as usize,
        );
        sync_dir(self.path.join("wal"));
        protect.write_segment = Some((write_segment_id, write_segment));
        protect.maintain(self.options.clone(), self.delete.clone(), &self.view);
        self.instant_written.store(Instant::now());
    }
    pub fn seal(&self, check: NonZeroU128) {
        let mut protect = self.protect.lock();
        if let Some((id, write_segment)) = protect.write_segment.clone() {
            if check != id {
                return;
            }
            write_segment.seal();
            protect.read_segments.insert(id, write_segment);
        }
        protect.write_segment = None;
        protect.maintain(self.options.clone(), self.delete.clone(), &self.view);
        self.instant_written.store(Instant::now());
    }
    pub fn stat(&self) -> IndexStat {
        let view = self.view();
        IndexStat {
            indexing: self.instant_indexed.load() < self.instant_written.load(),
            options: self.options().clone(),
            segments: {
                let mut segments = Vec::new();
                for sealed_segment in view.sealed_segments.values() {
                    segments.push(sealed_segment.stat_sealed());
                }
                for read_segment in view.read_segments.values() {
                    segments.push(read_segment.stat_read());
                }
                if let Some(write_segment) = view.write_segment.as_ref().map(|(_, x)| x) {
                    segments.push(write_segment.stat_write());
                }
                segments
            },
        }
    }
    pub fn delete(&self, p: Pointer) -> Result<(), DeleteError> {
        self.delete.delete(p);
        self.check_deleted.store(false);
        Ok(())
    }
    pub fn start(self: &Arc<Self>) {
        let mut optimizing = self.optimizing.lock();
        if optimizing.is_none() {
            *optimizing = Some(Optimizing::new(self.clone()).spawn());
        }
    }
    pub fn stop(&self) {
        let mut optimizing = self.optimizing.lock();
        if let Some((sender, join_handle)) = optimizing.take() {
            drop(sender);
            let _ = join_handle.join();
        }
    }
    pub fn get_check_deleted_flag(&self) -> bool {
        self.check_deleted.load()
    }
    pub fn set_check_deleted_flag(&self) {
        self.check_deleted.store(true)
    }
    pub fn check_existing(&self, payload: Payload) -> bool {
        self.delete.check(payload)
    }
    pub fn wait(&self) -> Arc<IndexTracker> {
        Arc::clone(&self._tracker)
    }
    pub fn create_sealed_segment(
        &self,
        source: &(impl Vectors<O::Vector> + Collection + Source + Sync),
        sealed_segment_ids: &[NonZeroU128],
        growing_segment_ids: &[NonZeroU128],
    ) -> Option<Arc<SealedSegment<O>>> {
        let id;
        {
            let mut protect = self.protect.lock();
            id = protect.sealed_counter;
            protect.sealed_counter = protect.sealed_counter.checked_add(1).unwrap();
            protect.maintain(self.options.clone(), self.delete.clone(), &self.view);
        }
        let next = SealedSegment::create(
            self._tracker.clone(),
            self.path.join("sealed_segments").join(id.to_string()),
            id,
            self.options.clone(),
            source,
        );
        sync_walk_from_dir(self.path.join("sealed_segments").join(id.to_string()));
        sync_dir(self.path.join("sealed_segments"));
        {
            let mut protect = self.protect.lock();
            for sealed_segment_id in sealed_segment_ids {
                if protect.sealed_segments.contains_key(sealed_segment_id) {
                    continue;
                }
                return None;
            }
            for growing_segment_id in growing_segment_ids {
                if protect.read_segments.contains_key(growing_segment_id) {
                    continue;
                }
                return None;
            }
            for sealed_segment_id in sealed_segment_ids {
                protect.sealed_segments.remove(sealed_segment_id);
            }
            for growing_segment_id in growing_segment_ids {
                protect.read_segments.remove(growing_segment_id);
            }
            protect.sealed_segments.insert(next.id(), next.clone());
            protect.maintain(self.options.clone(), self.delete.clone(), &self.view);
        }
        Some(next)
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

pub struct IndexView<O: Op> {
    pub options: IndexOptions,
    pub alterable_options: IndexAlterableOptions,
    pub delete: Arc<Delete>,
    pub sealed_segments: HashMap<NonZeroU128, Arc<SealedSegment<O>>>,
    pub read_segments: HashMap<NonZeroU128, Arc<GrowingSegment<O>>>,
    pub write_segment: Option<(NonZeroU128, Arc<GrowingSegment<O>>)>,
}

impl<O: Op> IndexView<O> {
    pub fn vbase<'a>(
        &'a self,
        vector: Borrowed<'a, O>,
        opts: &'a SearchOptions,
    ) -> Result<impl Iterator<Item = (Distance, Pointer)> + 'a, VbaseError> {
        if self.options.vector.dims != vector.dims() {
            return Err(VbaseError::InvalidVector);
        }
        if let Err(err) = opts.validate() {
            return Err(VbaseError::InvalidSearchOptions {
                reason: err.to_string(),
            });
        }

        let n = self.sealed_segments.len() + self.read_segments.len() + 1;
        let mut iterators = Vec::with_capacity(n);
        for (_, sealed) in self.sealed_segments.iter() {
            let stage2 = sealed.vbase(vector, opts);
            iterators.push(stage2);
        }
        for (_, read) in self.read_segments.iter() {
            let stage2 = read.vbase(vector, opts);
            iterators.push(stage2);
        }
        if let Some((_, write)) = &self.write_segment {
            let stage2 = write.vbase(vector, opts);
            iterators.push(stage2);
        }
        let loser = LoserTree::new(iterators);
        Ok(loser.filter_map(|x| {
            if self.delete.check(x.payload.0) {
                Some((x.distance, x.payload.0.pointer()))
            } else {
                None
            }
        }))
    }
    pub fn list(&self) -> Result<impl Iterator<Item = Pointer> + '_, ListError> {
        let sealed_segments = self
            .sealed_segments
            .values()
            .flat_map(|x| (0..x.len()).map(|i| x.payload(i)));
        let read_segments = self
            .read_segments
            .values()
            .flat_map(|x| (0..x.len()).map(|i| x.payload(i)));
        let write_segments = self
            .write_segment
            .iter()
            .map(|(_, x)| x)
            .flat_map(|x| (0..x.len()).map(|i| x.payload(i)));
        let iter = sealed_segments
            .chain(read_segments)
            .chain(write_segments)
            .filter(|p| self.delete.check(*p))
            .map(|p| p.pointer());
        Ok(iter)
    }
    pub fn insert(
        &self,
        vector: O::Vector,
        pointer: Pointer,
    ) -> Result<Result<(), OutdatedError>, InsertError> {
        if self.options.vector.dims != vector.as_borrowed().dims() {
            return Err(InsertError::InvalidVector);
        }

        let payload = Payload::new(pointer, self.delete.version(pointer));
        if let Some((_, segment)) = self.write_segment.as_ref() {
            use crate::segment::growing::GrowingSegmentInsertError;
            if let Err(GrowingSegmentInsertError) = segment.insert(vector, payload) {
                return Ok(Err(OutdatedError));
            }
            Ok(Ok(()))
        } else {
            Ok(Err(OutdatedError))
        }
    }
    pub fn flush(&self) -> Result<(), FlushError> {
        self.delete.flush();
        if let Some((_, write)) = &self.write_segment {
            write.flush();
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IndexStartup {
    sealed_segment_ids: HashSet<NonZeroU128>,
    growing_segment_ids: HashSet<NonZeroU128>,
    alterable_options: IndexAlterableOptions,
    sealed_counter: NonZeroU128,
    growing_counter: NonZeroU128,
}

struct IndexProtect<O: Op> {
    startup: FileAtomic<IndexStartup>,
    sealed_segments: HashMap<NonZeroU128, Arc<SealedSegment<O>>>,
    read_segments: HashMap<NonZeroU128, Arc<GrowingSegment<O>>>,
    write_segment: Option<(NonZeroU128, Arc<GrowingSegment<O>>)>,
    alterable_options: IndexAlterableOptions,
    sealed_counter: NonZeroU128,
    growing_counter: NonZeroU128,
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
            alterable_options: self.alterable_options.clone(),
            delete,
            sealed_segments: self.sealed_segments.clone(),
            read_segments: self.read_segments.clone(),
            write_segment: self.write_segment.clone(),
        });
        let read_segment_ids = self.read_segments.keys().copied();
        let write_segment_id = self.write_segment.as_ref().map(|(id, _)| *id);
        let growing_segment_ids = read_segment_ids.chain(write_segment_id).collect();
        let sealed_segment_ids = self.sealed_segments.keys().copied().collect();
        self.startup.set(IndexStartup {
            sealed_segment_ids,
            growing_segment_ids,
            alterable_options: self.alterable_options.clone(),
            sealed_counter: self.sealed_counter,
            growing_counter: self.growing_counter,
        });
        swap.swap(view);
    }
}
