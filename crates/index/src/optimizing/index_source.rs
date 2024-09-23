use crate::delete::Delete;
use crate::Op;
use crate::{GrowingSegment, SealedSegment};
use base::index::IndexOptions;
use base::operator::Borrowed;
use base::search::*;
use std::any::Any;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

pub struct IndexSource<V, O: Op> {
    pub(super) sealed: Option<Arc<SealedSegment<O>>>,
    pub(super) growing: Vec<Arc<GrowingSegment<O>>>,
    pub(super) dims: u32,
    pub(super) delete: Arc<Delete>,
    _phantom: PhantomData<fn(V) -> V>,
}

impl<O: Op> IndexSource<O::Vector, O> {
    pub fn new(
        options: IndexOptions,
        sealed: Option<Arc<SealedSegment<O>>>,
        growing: Vec<Arc<GrowingSegment<O>>>,
        delete: Arc<Delete>,
    ) -> Self {
        IndexSource {
            sealed,
            growing,
            dims: options.vector.dims,
            delete,
            _phantom: PhantomData,
        }
    }
}

impl<O: Op> Vectors<O::Vector> for IndexSource<O::Vector, O> {
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
}

impl<O: Op> Collection for IndexSource<O::Vector, O> {
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

impl<O: Op> Source for IndexSource<O::Vector, O> {
    fn get_main<T: Any>(&self) -> Option<&T> {
        let x = self.sealed.as_ref()?;
        Some(
            x.indexing()
                .downcast_ref::<T>()
                .expect("called with incorrect index type"),
        )
    }

    fn get_main_len(&self) -> u32 {
        self.sealed.as_ref().map(|x| x.len()).unwrap_or_default()
    }

    fn check_existing(&self, i: u32) -> bool {
        self.delete.check(self.payload(i))
    }
}

pub struct RoGrowingCollection<V, O: Op> {
    pub(super) growing: Vec<Arc<GrowingSegment<O>>>,
    pub(super) dims: u32,
    _phantom: PhantomData<fn(V) -> V>,
}

impl<O: Op> Debug for RoGrowingCollection<O::Vector, O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoGrowingCollection")
            .field("growing", &self.growing)
            .field("dims", &self.dims)
            .finish()
    }
}

impl<O: Op> Vectors<O::Vector> for RoGrowingCollection<O::Vector, O> {
    fn dims(&self) -> u32 {
        self.dims
    }

    fn len(&self) -> u32 {
        self.growing.iter().map(|x| x.len()).sum::<u32>()
    }

    fn vector(&self, mut index: u32) -> Borrowed<'_, O> {
        for x in self.growing.iter() {
            if index < x.len() {
                return x.vector(index);
            }
            index -= x.len();
        }
        panic!("Out of bound.")
    }
}

impl<O: Op> Collection for RoGrowingCollection<O::Vector, O> {
    fn payload(&self, mut index: u32) -> Payload {
        for x in self.growing.iter() {
            if index < x.len() {
                return x.payload(index);
            }
            index -= x.len();
        }
        panic!("Out of bound.")
    }
}
