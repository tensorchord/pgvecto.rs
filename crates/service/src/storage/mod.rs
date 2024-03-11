mod binary;
mod dense;
mod i8_quant;
mod sparse;

pub use binary::BinaryMmap;
pub use dense::DenseMmap;
pub use i8_quant::I8QuantMmap;
pub use sparse::SparseMmap;

use crate::algorithms::raw::RawRam;
use crate::prelude::*;
use std::path::Path;

pub trait Storage {
    type VectorOwned: VectorOwned;

    #[allow(unused)]
    fn dims(&self) -> u32;
    fn len(&self) -> u32;
    fn vector(&self, i: u32) -> <Self::VectorOwned as VectorOwned>::Borrowed<'_>;
    fn payload(&self, i: u32) -> Payload;
    fn open(path: &Path, options: IndexOptions) -> Self;
    fn save<S: G<VectorOwned = Self::VectorOwned>>(path: &Path, ram: RawRam<S>) -> Self;
}

pub trait GlobalStorage: Global {
    type Storage: Storage<VectorOwned = Self::VectorOwned>;
}

impl GlobalStorage for SVecf32Cos {
    type Storage = SparseMmap;
}

impl GlobalStorage for SVecf32Dot {
    type Storage = SparseMmap;
}

impl GlobalStorage for SVecf32L2 {
    type Storage = SparseMmap;
}

impl GlobalStorage for Vecf16Cos {
    type Storage = DenseMmap<F16>;
}

impl GlobalStorage for Vecf16Dot {
    type Storage = DenseMmap<F16>;
}

impl GlobalStorage for Vecf16L2 {
    type Storage = DenseMmap<F16>;
}

impl GlobalStorage for Vecf32Cos {
    type Storage = DenseMmap<F32>;
}

impl GlobalStorage for Vecf32Dot {
    type Storage = DenseMmap<F32>;
}

impl GlobalStorage for Vecf32L2 {
    type Storage = DenseMmap<F32>;
}

impl GlobalStorage for BVecf32Cos {
    type Storage = BinaryMmap;
}

impl GlobalStorage for BVecf32Dot {
    type Storage = BinaryMmap;
}

impl GlobalStorage for BVecf32L2 {
    type Storage = BinaryMmap;
}

impl GlobalStorage for BVecf32Jaccard {
    type Storage = BinaryMmap;
}

impl GlobalStorage for Veci8Cos {
    type Storage = I8QuantMmap;
}

impl GlobalStorage for Veci8Dot {
    type Storage = I8QuantMmap;
}

impl GlobalStorage for Veci8L2 {
    type Storage = I8QuantMmap;
}
