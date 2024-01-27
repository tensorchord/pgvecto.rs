mod dense;
mod sparse;

pub use dense::DenseMmap;
pub use sparse::SparseMmap;

use crate::index::IndexOptions;
use crate::prelude::*;
use std::borrow::Cow;
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
    type Scalar: Copy;
    type Vector: Vector<Element = Self::Element>;
    type VectorRef<'a>: VectorRef<Element = Self::Element> + 'a
    where
        Self: 'a;

    fn dims(&self) -> u16;
    fn len(&self) -> u32;
    fn content(&self, i: u32) -> Self::VectorRef<'_>;
    fn payload(&self, i: u32) -> Payload;
    fn full_vector(contents: Self::VectorRef<'_>) -> Cow<'_, [Self::Scalar]>;
    fn load(path: &Path, options: IndexOptions) -> Self;
    fn save(path: &Path, other: impl Ram<Element = Self::Element>) -> Self
    where
        Self: Sized;
}

pub trait VectorRef {
    type Element;

    fn dims(&self) -> u16;
    fn vector<'a, 'b>(&'a self) -> &'b [Self::Element]
    where
        Self: 'b;
}

pub trait Vector {
    type Element;

    fn dims(&self) -> u16;
    fn vector(self) -> Vec<Self::Element>;
}

impl<T> Vector for Vec<T> {
    type Element = T;

    fn dims(&self) -> u16 {
        self.len() as u16
    }

    fn vector(self) -> Vec<Self::Element> {
        self
    }
}

impl<'c, T> VectorRef for &'c [T] {
    type Element = T;

    fn dims(&self) -> u16 {
        self.len() as u16
    }

    fn vector<'a, 'b>(&'a self) -> &'b [Self::Element]
    where
        'c: 'b,
    {
        self
    }
}
