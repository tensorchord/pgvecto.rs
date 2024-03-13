#![allow(clippy::len_without_is_empty)]

mod bvector;
pub mod operator;
mod svec;
mod vec;
mod veci8;

use self::operator::OperatorStorage;
use base::index::*;
use base::operator::*;
use base::search::*;
use base::vector::*;
use std::path::Path;

pub trait Storage {
    type VectorOwned: VectorOwned;

    fn dims(&self) -> u32;
    fn len(&self) -> u32;
    fn vector(&self, i: u32) -> <Self::VectorOwned as VectorOwned>::Borrowed<'_>;
    fn payload(&self, i: u32) -> Payload;
    fn open(path: &Path, options: IndexOptions) -> Self;
    fn save<O: Operator<VectorOwned = Self::VectorOwned>, C: Collection<O>>(
        path: &Path,
        collection: &C,
    ) -> Self;
}

pub struct StorageCollection<O: OperatorStorage> {
    storage: O::Storage,
}

impl<O: OperatorStorage> StorageCollection<O> {
    pub fn create<C: Collection<O>>(path: &Path, source: &C) -> Self {
        std::fs::create_dir(path).unwrap();
        let storage = O::Storage::save(path, source);
        common::dir_ops::sync_dir(path);
        Self { storage }
    }

    pub fn open(path: &Path, options: IndexOptions) -> Self {
        Self {
            storage: O::Storage::open(path, options),
        }
    }
}

impl<O: OperatorStorage> Collection<O> for StorageCollection<O> {
    fn dims(&self) -> u32 {
        self.storage.dims()
    }

    fn len(&self) -> u32 {
        self.storage.len()
    }

    fn vector(&self, i: u32) -> Borrowed<'_, O> {
        self.storage.vector(i)
    }

    fn payload(&self, i: u32) -> Payload {
        self.storage.payload(i)
    }
}

unsafe impl<O: OperatorStorage> Send for StorageCollection<O> {}
unsafe impl<O: OperatorStorage> Sync for StorageCollection<O> {}
