mod dense;
mod sparse;

pub use dense::DenseMmap;
pub use sparse::SparseMmap;

use crate::index::IndexOptions;
use crate::prelude::*;
use std::path::Path;

pub trait Ram {
    type Element: Copy + bytemuck::Pod;

    fn dims(&self) -> u16;
    fn len(&self) -> u32;
    fn content(&self, i: u32) -> &[Self::Element];
    fn payload(&self, i: u32) -> Payload;
}

pub trait Storage {
    type Element: Copy + bytemuck::Pod;

    fn dims(&self) -> u16;
    fn len(&self) -> u32;
    fn content(&self, i: u32) -> &[Self::Element];
    fn payload(&self, i: u32) -> Payload;
    fn load(path: &Path, options: IndexOptions) -> Self;
    fn save(path: &Path, other: impl Ram<Element = Self::Element>) -> Self
    where
        Self: Sized;
}
