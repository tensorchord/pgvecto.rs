use crate::{prelude::*, utils::iter::RefPeekable};
use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SparseF32Element {
    pub index: u32,
    pub value: F32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SparseF32 {
    pub dims: u16,
    pub elements: Vec<SparseF32Element>,
}

#[derive(Debug, Clone)]
pub struct SparseF32Ref<'a> {
    pub dims: u16,
    pub elements: &'a [SparseF32Element],
}

impl<'a> From<&'a SparseF32> for SparseF32Ref<'a> {
    fn from(value: &'a SparseF32) -> SparseF32Ref<'a> {
        Self {
            dims: value.dims,
            elements: value.elements.as_slice(),
        }
    }
}

impl Vector for SparseF32 {
    type Element = SparseF32Element;

    fn dims(&self) -> u16 {
        self.dims
    }

    fn vector(self) -> Vec<Self::Element> {
        self.elements
    }
}

impl<'c> VectorRef for SparseF32Ref<'c> {
    type Element = SparseF32Element;

    fn dims(&self) -> u16 {
        self.dims
    }

    fn vector<'a, 'b>(&'a self) -> &'b [Self::Element]
    where
        'c: 'b,
    {
        self.elements
    }
}

pub fn expand_sparse(sparse: &[SparseF32Element]) -> impl Iterator<Item = F32> + '_ {
    let mut data = RefPeekable::new(sparse.iter());
    let mut i = 0;
    std::iter::from_fn(move || {
        if let Some(&&SparseF32Element { index, value }) = data.peek() {
            if i == index {
                data.next();
                i += 1;
                Some(value)
            } else {
                i += 1;
                Some(F32::zero())
            }
        } else {
            None
        }
    })
}

impl Display for SparseF32Element {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{{}: {}}}", self.index, self.value)
    }
}

unsafe impl bytemuck::Zeroable for SparseF32Element {}

unsafe impl bytemuck::Pod for SparseF32Element {}
