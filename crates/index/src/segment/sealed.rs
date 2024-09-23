use crate::utils::dir_ops::dir_size;
use crate::IndexTracker;
use crate::Op;
use base::index::*;
use base::operator::*;
use base::search::*;
use crossbeam::atomic::AtomicCell;
use indexing::SealedIndexing;
use std::any::Any;
use std::fmt::Debug;
use std::num::NonZeroU128;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

pub struct SealedSegment<O: Op> {
    id: NonZeroU128,
    path: PathBuf,
    indexing: SealedIndexing<O>,
    deletes: AtomicCell<(Instant, u32)>,
    _sealed_segment_tracker: SealedSegmentTracker,
    _index_tracker: Arc<IndexTracker>,
}

impl<O: Op> Debug for SealedSegment<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SealedSegment")
            .field("id", &self.id)
            .finish()
    }
}

impl<O: Op> SealedSegment<O> {
    pub fn create(
        index_tracker: Arc<IndexTracker>,
        path: PathBuf,
        id: NonZeroU128,
        options: IndexOptions,
        source: &(impl Vectors<O::Vector> + Collection + Source + Sync),
    ) -> Arc<Self> {
        let indexing = SealedIndexing::create(&path, options, source);
        Arc::new(Self {
            id,
            path: path.clone(),
            indexing,
            deletes: AtomicCell::new((Instant::now(), 0)),
            _sealed_segment_tracker: SealedSegmentTracker { path },
            _index_tracker: index_tracker,
        })
    }

    pub fn open(
        index_tracker: Arc<IndexTracker>,
        path: PathBuf,
        id: NonZeroU128,
        options: IndexOptions,
    ) -> Arc<Self> {
        let indexing = SealedIndexing::open(&path, options);
        Arc::new(Self {
            id,
            path: path.clone(),
            indexing,
            deletes: AtomicCell::new((Instant::now(), 0)),
            _sealed_segment_tracker: SealedSegmentTracker { path },
            _index_tracker: index_tracker,
        })
    }

    pub fn id(&self) -> NonZeroU128 {
        self.id
    }

    pub fn stat_sealed(&self) -> SegmentStat {
        SegmentStat {
            id: self.id,
            r#type: "sealed".to_string(),
            length: self.len() as usize,
            size: dir_size(&self.path).unwrap(),
        }
    }

    pub fn vbase<'a>(
        &'a self,
        vector: Borrowed<'a, O>,
        opts: &'a SearchOptions,
    ) -> Box<dyn Iterator<Item = Element> + 'a> {
        self.indexing.vbase(vector, opts)
    }

    pub fn len(&self) -> u32 {
        self.indexing.len()
    }

    pub fn vector(&self, i: u32) -> Borrowed<'_, O> {
        self.indexing.vector(i)
    }

    pub fn payload(&self, i: u32) -> Payload {
        self.indexing.payload(i)
    }

    pub fn inspect(&self, d: Duration, check: impl Fn(u64) -> bool) -> Result<u32, u32> {
        let (t, c) = self.deletes.load();
        if t.elapsed() > d {
            let mut counter = 0_u32;
            for i in 0..self.len() {
                if check(self.payload(i).time()) {
                    counter += 1;
                }
            }
            self.deletes.store((Instant::now(), counter));
            Ok(counter)
        } else {
            Err(c)
        }
    }

    pub fn indexing(&self) -> &dyn Any {
        self.indexing.as_any()
    }
}

#[derive(Debug, Clone)]
pub struct SealedSegmentTracker {
    path: PathBuf,
}

impl Drop for SealedSegmentTracker {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.path).unwrap();
    }
}
