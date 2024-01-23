use crate::{prelude::*, utils::iter::RefPeekable};
use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SparseF32Element {
    pub index: u32,
    pub value: F32,
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
        write!(f, "{{index: {}, value: {}}}", self.index, self.value)
    }
}

unsafe impl bytemuck::Zeroable for SparseF32Element {}

unsafe impl bytemuck::Pod for SparseF32Element {}
