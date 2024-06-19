use crate::Op;
use base::index::*;
use base::operator::*;
use base::search::*;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::convert::Infallible;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("`GrowingIndexing` is read-only.")]
pub struct GrowingIndexingInsertError;

pub enum GrowingIndexing<O: Op> {
    Infallible(Infallible, fn(O) -> O),
}

impl<O: Op> GrowingIndexing<O> {
    pub fn new(_: VectorOptions, _: usize) -> Self {
        unimplemented!()
    }

    pub fn is_full(&self) -> bool {
        unimplemented!()
    }

    pub fn seal(&self) {
        unimplemented!()
    }

    pub fn insert(&self, _: O::VectorOwned, _: Payload) -> Result<(), GrowingIndexingInsertError> {
        unimplemented!()
    }

    pub fn basic(&self, _: Borrowed<'_, O>, _: &SearchOptions) -> BinaryHeap<Reverse<Element>> {
        unimplemented!()
    }

    pub fn vbase<'a>(
        &'a self,
        _: Borrowed<'a, O>,
        _: &'a SearchOptions,
    ) -> (Vec<Element>, Box<(dyn Iterator<Item = Element> + 'a)>) {
        unimplemented!()
    }

    pub fn len(&self) -> u32 {
        unimplemented!()
    }

    pub fn vector(&self, _i: u32) -> Borrowed<'_, O> {
        unimplemented!()
    }

    pub fn payload(&self, _i: u32) -> Payload {
        unimplemented!()
    }
}
