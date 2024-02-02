use crate::prelude::*;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SparseF32Ref<'a> {
    pub dims: u16,
    pub elements: &'a [SparseF32Element],
}

impl Display for SparseF32Element {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{{}: {}}}", self.index, self.value)
    }
}

unsafe impl bytemuck::Zeroable for SparseF32Element {}

unsafe impl bytemuck::Pod for SparseF32Element {}

impl<'a> From<SparseF32Ref<'a>> for SparseF32 {
    fn from(value: SparseF32Ref<'a>) -> Self {
        Self {
            dims: value.dims,
            elements: value.elements.to_vec(),
        }
    }
}

impl<'a> From<&'a SparseF32> for SparseF32Ref<'a> {
    fn from(value: &'a SparseF32) -> Self {
        Self {
            dims: value.dims,
            elements: &value.elements,
        }
    }
}

impl VectorOwned for SparseF32 {
    type Element = SparseF32Element;

    fn dims(&self) -> u16 {
        self.dims
    }

    fn inner(self) -> Vec<Self::Element> {
        self.elements
    }
}

impl<'a> VectorRef<'a> for SparseF32Ref<'a> {
    type Element = SparseF32Element;

    fn dims(&self) -> u16 {
        self.dims
    }

    fn inner(self) -> &'a [Self::Element] {
        self.elements
    }
}

impl<'a> SparseF32Ref<'a> {
    pub fn to_dense(&self) -> Vec<F32> {
        let mut dense = vec![F32::zero(); self.dims as usize];
        for i in self.elements {
            dense[i.index as usize] = i.value;
        }
        dense
    }
}
