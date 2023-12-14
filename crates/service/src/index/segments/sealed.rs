use super::growing::GrowingSegment;
use super::SegmentTracker;
use crate::index::indexing::{DynamicIndexIter, DynamicIndexing};
use crate::index::{IndexOptions, IndexTracker};
use crate::prelude::*;
use crate::utils::dir_ops::sync_dir;
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
        let indexing = DynamicIndexing::create(path.join("indexing"), options, sealed, growing);
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
        let indexing = DynamicIndexing::open(path.join("indexing"), options);
        Arc::new(Self {
            uuid,
            indexing,
            _tracker: Arc::new(SegmentTracker { path, _tracker }),
        })
    }
    pub fn uuid(&self) -> Uuid {
        self.uuid
    }
    pub fn len(&self) -> u32 {
        self.indexing.len()
    }
    pub fn vector(&self, i: u32) -> &[S::Scalar] {
        self.indexing.vector(i)
    }
    pub fn payload(&self, i: u32) -> Payload {
        self.indexing.payload(i)
    }
    pub fn search(&self, k: usize, vector: &[S::Scalar], filter: &mut impl Filter) -> Heap {
        self.indexing.search(k, vector, filter)
    }
    pub fn vbase(&self, range: usize, vector: &[S::Scalar]) -> DynamicIndexIter<'_, S> {
        self.indexing.vbase(range, vector)
    }
}
