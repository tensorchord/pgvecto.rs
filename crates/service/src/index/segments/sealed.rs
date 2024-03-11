use super::growing::GrowingSegment;
use super::SegmentTracker;
use crate::index::indexing::DynamicIndexing;
use crate::index::IndexTracker;
use crate::prelude::*;
use crate::utils::dir_ops::{dir_size, sync_dir};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;

pub struct SealedSegment<S: G> {
    uuid: Uuid,
    indexing: DynamicIndexing<S>,
    _tracker: Arc<SegmentTracker>,
}

impl<S: G> SealedSegment<S> {
    pub fn create(
        _tracker: Arc<IndexTracker>,
        path: PathBuf,
        uuid: Uuid,
        options: IndexOptions,
        sealed: Vec<Arc<SealedSegment<S>>>,
        growing: Vec<Arc<GrowingSegment<S>>>,
    ) -> Arc<Self> {
        std::fs::create_dir(&path).unwrap();
        let indexing = DynamicIndexing::create(&path.join("indexing"), options, sealed, growing);
        sync_dir(&path);
        Arc::new(Self {
            uuid,
            indexing,
            _tracker: Arc::new(SegmentTracker { path, _tracker }),
        })
    }

    pub fn open(
        _tracker: Arc<IndexTracker>,
        path: PathBuf,
        uuid: Uuid,
        options: IndexOptions,
    ) -> Arc<Self> {
        let indexing = DynamicIndexing::open(&path.join("indexing"), options);
        Arc::new(Self {
            uuid,
            indexing,
            _tracker: Arc::new(SegmentTracker { path, _tracker }),
        })
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    pub fn stat_sealed(&self) -> SegmentStat {
        let path = self._tracker.path.join("indexing");
        SegmentStat {
            id: self.uuid,
            typ: "sealed".to_string(),
            length: self.len() as usize,
            size: dir_size(&path).unwrap(),
        }
    }

    pub fn basic(
        &self,
        vector: Borrowed<'_, S>,
        opts: &SearchOptions,
        filter: impl Filter,
    ) -> BinaryHeap<Reverse<Element>> {
        self.indexing.basic(vector, opts, filter)
    }

    pub fn vbase<'a>(
        &'a self,
        vector: Borrowed<'a, S>,
        opts: &'a SearchOptions,
        filter: impl Filter + 'a,
    ) -> (Vec<Element>, Box<dyn Iterator<Item = Element> + 'a>) {
        self.indexing.vbase(vector, opts, filter)
    }

    pub fn len(&self) -> u32 {
        self.indexing.len()
    }

    pub fn vector(&self, i: u32) -> Borrowed<'_, S> {
        self.indexing.vector(i)
    }

    pub fn payload(&self, i: u32) -> Payload {
        self.indexing.payload(i)
    }
}
