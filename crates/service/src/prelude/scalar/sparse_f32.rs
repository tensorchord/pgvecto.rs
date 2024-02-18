use crate::prelude::*;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SparseF32Element {
    pub index: u16,
    pub value: F32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SparseF32 {
    pub dims: u16,
    pub indexes: Vec<u16>,
    pub values: Vec<F32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SparseF32Ref<'a> {
    pub dims: u16,
    pub indexes: &'a [u16],
    pub values: &'a [F32],
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
            indexes: value.indexes.to_vec(),
            values: value.values.to_vec(),
        }
    }
}

impl<'a> From<&'a SparseF32> for SparseF32Ref<'a> {
    fn from(value: &'a SparseF32) -> Self {
        Self {
            dims: value.dims,
            indexes: &value.indexes,
            values: &value.values,
        }
    }
}

impl VectorOwned for SparseF32 {
    fn dims(&self) -> u16 {
        self.dims
    }
}

impl<'a> VectorRef<'a> for SparseF32Ref<'a> {
    fn dims(&self) -> u16 {
        self.dims
    }

    fn length(&self) -> u16 {
        self.indexes.len().try_into().unwrap()
    }
}

impl<'a> SparseF32Ref<'a> {
    pub fn to_dense(&self) -> Vec<F32> {
        let mut dense = vec![F32::zero(); self.dims as usize];
        for (&index, &value) in self.indexes.iter().zip(self.values.iter()) {
            dense[index as usize] = value;
        }
        dense
    }

    pub fn iter(&self) -> impl Iterator<Item = SparseF32Element> + 'a {
        self.indexes
            .iter()
            .copied()
            .zip(self.values.iter().copied())
            .map(|(index, value)| SparseF32Element { index, value })
    }
}
