use super::AbstractIndexing;
use crate::algorithms::ivf::Ivf;
use crate::index::segments::growing::GrowingSegment;
use crate::index::segments::sealed::SealedSegment;
use crate::prelude::*;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::path::Path;
use std::sync::Arc;

pub struct IvfIndexing<S: G> {
    raw: Ivf<S>,
}

impl<S: G> AbstractIndexing<S> for IvfIndexing<S> {
    fn create(
        path: &Path,
        options: IndexOptions,
        sealed: Vec<Arc<SealedSegment<S>>>,
        growing: Vec<Arc<GrowingSegment<S>>>,
    ) -> Self {
        let raw = Ivf::create(path, options, sealed, growing);
        Self { raw }
    }

    fn basic(
        &self,
        vector: Borrowed<'_, S>,
        opts: &SearchOptions,
        filter: impl Filter,
    ) -> BinaryHeap<Reverse<Element>> {
        self.raw.basic(vector, opts, filter)
    }

    fn vbase<'a>(
        &'a self,
        vector: Borrowed<'a, S>,
        opts: &'a SearchOptions,
        filter: impl Filter + 'a,
    ) -> (Vec<Element>, Box<(dyn Iterator<Item = Element> + 'a)>) {
        self.raw.vbase(vector, opts, filter)
    }
}

impl<S: G> IvfIndexing<S> {
    pub fn len(&self) -> u32 {
        self.raw.len()
    }

    pub fn vector(&self, i: u32) -> Borrowed<'_, S> {
        self.raw.vector(i)
    }

    pub fn payload(&self, i: u32) -> Payload {
        self.raw.payload(i)
    }

    pub fn open(path: &Path, options: IndexOptions) -> Self {
        Self {
            raw: Ivf::open(path, options),
        }
    }
}
