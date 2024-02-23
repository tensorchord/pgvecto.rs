use crate::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VecI8Owned {
    pub dims: u16,
    pub data: Vec<I8>,
    pub alpha: F32,
    pub offset: F32,
    // sum of a_i * alpha, precomputed for dot
    pub sum: F32,
    // l2 norm of original f_i, precomputed for l2
    pub l2_norm: F32,
}

impl Vector for VecI8Owned {
    fn dims(&self) -> u16 {
        self.dims
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VecI8Ref<'a> {
    pub dims: u16,
    pub data: &'a [I8],
    pub alpha: F32,
    pub offset: F32,
    // sum of a_i * alpha, precomputed for dot
    pub sum: F32,
    // l2 norm of original f_i, precomputed for l2
    pub l2_norm: F32,
}

impl VecI8Ref<'_> {
    pub fn new(dims: u16, data: &[I8], alpha: F32, offset: F32) -> VecI8Ref<'_> {
        let (sum, l2_norm) = crate::prelude::i8::precompute(data, alpha, offset);
        VecI8Ref {
            dims,
            data,
            alpha,
            offset,
            sum,
            l2_norm,
        }
    }

    pub fn to_owned(&self) -> VecI8Owned {
        VecI8Owned {
            dims: self.dims,
            data: self.data.to_vec(),
            alpha: self.alpha,
            offset: self.offset,
            sum: self.sum,
            l2_norm: self.l2_norm,
        }
    }
}

impl Vector for VecI8Ref<'_> {
    fn dims(&self) -> u16 {
        self.dims
    }
}

impl From<VecI8Ref<'_>> for VecI8Owned {
    fn from(value: VecI8Ref<'_>) -> Self {
        Self {
            dims: value.dims,
            data: value.data.to_vec(),
            alpha: value.alpha,
            offset: value.offset,
            sum: value.sum,
            l2_norm: value.l2_norm,
        }
    }
}

impl<'a> From<&'a VecI8Owned> for VecI8Ref<'a> {
    fn from(value: &'a VecI8Owned) -> Self {
        Self {
            dims: value.dims,
            data: &value.data,
            alpha: value.alpha,
            offset: value.offset,
            sum: value.sum,
            l2_norm: value.l2_norm,
        }
    }
}
