use super::utils::filtered_fixed_heap::FilteredFixedHeap;
use super::Algo;
use crate::bgworker::index::IndexOptions;
use crate::bgworker::storage::Storage;
use crate::bgworker::storage::StoragePreallocator;
use crate::bgworker::vectors::Vectors;
use crate::prelude::*;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Clone, Error, Serialize, Deserialize)]
pub enum FlatError {
    //
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlatOptions {}

pub struct Flat<D: DistanceFamily> {
    vectors: Arc<Vectors>,
    _maker: PhantomData<D>,
}

impl<D: DistanceFamily> Algo for Flat<D> {
    type Error = FlatError;

    type Save = ();

    fn prebuild(_: &mut StoragePreallocator, _: IndexOptions) -> Result<(), Self::Error> {
        Ok(())
    }

    fn build(
        _: &mut Storage,
        _: IndexOptions,
        vectors: Arc<Vectors>,
        _: usize,
    ) -> Result<Self, FlatError> {
        Ok(Self {
            vectors,
            _maker: PhantomData,
        })
    }

    fn save(&self) {}

    fn load(
        _: &mut Storage,
        _: IndexOptions,
        vectors: Arc<Vectors>,
        _: (),
    ) -> Result<Self, FlatError> {
        Ok(Self {
            vectors,
            _maker: PhantomData,
        })
    }

    fn insert(&self, _: usize) -> Result<(), FlatError> {
        Ok(())
    }

    fn search<F>(
        &self,
        target: Box<[Scalar]>,
        k: usize,
        filter: F,
    ) -> Result<Vec<(Scalar, u64)>, FlatError>
    where
        F: FnMut(u64) -> bool,
    {
        let mut result = FilteredFixedHeap::new(k, filter);
        for i in 0..self.vectors.len() {
            let this_vector = self.vectors.get_vector(i);
            let this_data = self.vectors.get_data(i);
            let dis = D::distance(&target, this_vector);
            result.push((dis, this_data));
        }
        Ok(result.into_sorted_vec())
    }
}
