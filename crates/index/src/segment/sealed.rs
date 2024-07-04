use super::SegmentTracker;
use crate::indexing::sealed::SealedIndexing;
use crate::utils::dir_ops::dir_size;
use crate::IndexTracker;
use crate::Op;
use base::index::*;
use base::operator::*;
use base::search::*;
use crossbeam::atomic::AtomicCell;
use std::any::Any;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use uuid::Uuid;

pub struct SealedSegment<O: Op> {
    id: Uuid,
    indexing: SealedIndexing<O>,
    deletes: AtomicCell<(Instant, u32)>,
    _tracker: Arc<SegmentTracker>,
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
        _tracker: Arc<IndexTracker>,
        path: PathBuf,
        id: Uuid,
        options: IndexOptions,
        source: &(impl Source<O> + Sync),
    ) -> Arc<Self> {
        let indexing = SealedIndexing::create(&path, options, source);
        Arc::new(Self {
            id,
            indexing,
            deletes: AtomicCell::new((Instant::now(), 0)),
            _tracker: Arc::new(SegmentTracker { path, _tracker }),
        })
    }

    pub fn open(
        _tracker: Arc<IndexTracker>,
        path: PathBuf,
        id: Uuid,
        options: IndexOptions,
    ) -> Arc<Self> {
        let indexing = SealedIndexing::open(&path, options);
        Arc::new(Self {
            id,
            indexing,
            deletes: AtomicCell::new((Instant::now(), 0)),
            _tracker: Arc::new(SegmentTracker { path, _tracker }),
        })
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn stat_sealed(&self) -> SegmentStat {
        SegmentStat {
            id: self.id,
            r#type: "sealed".to_string(),
            length: self.len() as usize,
            size: dir_size(&self._tracker.path).unwrap(),
        }
    }

    pub fn basic(
        &self,
        vector: Borrowed<'_, O>,
        opts: &SearchOptions,
    ) -> BinaryHeap<Reverse<Element>> {
        self.indexing.basic(vector, opts)
    }

    pub fn vbase<'a>(
        &'a self,
        vector: Borrowed<'a, O>,
        opts: &'a SearchOptions,
    ) -> (Vec<Element>, Box<dyn Iterator<Item = Element> + 'a>) {
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
        match &self.indexing {
            SealedIndexing::Flat(x) => x,
            SealedIndexing::Ivf(x) => x,
            SealedIndexing::Hnsw(x) => x,
            SealedIndexing::Inverted(x) => x,
        }
    }
}
