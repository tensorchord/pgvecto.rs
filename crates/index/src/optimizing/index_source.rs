use crate::Op;
use crate::{GrowingSegment, SealedSegment};
use base::index::IndexOptions;
use base::operator::Borrowed;
use base::search::*;
use std::sync::Arc;

pub struct IndexSource<O: Op> {
    pub(super) sealed: Option<Arc<SealedSegment<O>>>,
    pub(super) growing: Vec<Arc<GrowingSegment<O>>>,
    pub(super) dims: u32,
}

impl<O: Op> IndexSource<O> {
    pub fn new(
        options: IndexOptions,
        sealed: Option<Arc<SealedSegment<O>>>,
        growing: Vec<Arc<GrowingSegment<O>>>,
    ) -> Self {
        IndexSource {
            sealed,
            growing,
            dims: options.vector.dims,
        }
    }
}

impl<O: Op> Collection<O> for IndexSource<O> {
    fn dims(&self) -> u32 {
        self.dims
    }

    fn len(&self) -> u32 {
        self.sealed.iter().map(|x| x.len()).sum::<u32>()
            + self.growing.iter().map(|x| x.len()).sum::<u32>()
    }

    fn vector(&self, mut index: u32) -> Borrowed<'_, O> {
        for x in self.sealed.iter() {
            if index < x.len() {
                return x.vector(index);
            }
            index -= x.len();
        }
        for x in self.growing.iter() {
            if index < x.len() {
                return x.vector(index);
            }
            index -= x.len();
        }
        panic!("Out of bound.")
    }

    fn payload(&self, mut index: u32) -> Payload {
        for x in self.sealed.iter() {
            if index < x.len() {
                return x.payload(index);
            }
            index -= x.len();
        }
        for x in self.growing.iter() {
            if index < x.len() {
                return x.payload(index);
            }
            index -= x.len();
        }
        panic!("Out of bound.")
    }
}

impl<O: Op> Source<O> for IndexSource<O> {}
