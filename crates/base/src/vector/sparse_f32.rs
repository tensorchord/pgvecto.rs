use super::Vector;
use crate::scalar::F32;
use num_traits::Zero;
use serde::{Deserialize, Serialize};

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

impl Vector for SparseF32 {
    fn dims(&self) -> u16 {
        self.dims
    }
}

impl<'a> Vector for SparseF32Ref<'a> {
    fn dims(&self) -> u16 {
        self.dims
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

    pub fn length(&self) -> u16 {
        self.indexes.len().try_into().unwrap()
    }
}
