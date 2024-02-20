mod binary;
mod dense;
mod sparse;

pub use binary::BinaryMmap;
pub use dense::DenseMmap;
pub use sparse::SparseMmap;

use crate::algorithms::raw::RawRam;
use crate::index::IndexOptions;
use crate::prelude::*;
use std::path::Path;

pub trait Storage {
    type VectorRef<'a>: Copy + 'a
    where
        Self: 'a;

    fn dims(&self) -> u16;
    fn len(&self) -> u32;
    fn vector(&self, i: u32) -> Self::VectorRef<'_>;
    fn payload(&self, i: u32) -> Payload;
    fn open(path: &Path, options: IndexOptions) -> Self;
    fn save<S: for<'a> G<VectorRef<'a> = Self::VectorRef<'a>>>(path: &Path, ram: RawRam<S>)
        -> Self;
}
