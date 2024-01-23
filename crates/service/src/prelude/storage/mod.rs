mod dense;
mod sparse;

pub use dense::DenseMmap;
pub use sparse::SparseMmap;

use crate::index::IndexOptions;
use crate::prelude::*;
use std::borrow::Cow;
use std::path::Path;

pub trait Storage {
    type Element: Copy + bytemuck::Pod;

    fn dims(&self) -> u16;
    fn len(&self) -> u32;
    fn content(&self, i: u32) -> &[Self::Element];
    fn payload(&self, i: u32) -> Payload;

    fn load(_path: &Path, _options: IndexOptions) -> Self
    where
        Self: Sized,
    {
        unimplemented!("It dones't support load from disk")
    }
}

pub trait AtomicStorage: Storage {
    type Scalar: Copy;

    fn check_dims(dims: u16, vector: &[Self::Element]) -> bool;
    fn vector(dims: u16, contents: &[Self::Element]) -> Cow<'_, [Self::Scalar]>;
    fn save(path: &Path, other: impl Storage<Element = Self::Element>) -> Self
    where
        Self: Sized;
}
