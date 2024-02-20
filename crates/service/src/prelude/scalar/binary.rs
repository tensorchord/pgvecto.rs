use crate::prelude::*;
use bitvec::{slice::BitSlice, vec::BitVec};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinaryVec {
    pub values: BitVec,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct BinaryVecRef<'a> {
    // NOTE: In order to store bitslice to &[usize], we need to ensure there are no prefix bits.
    pub values: &'a BitSlice,
}

impl<'a> From<BinaryVecRef<'a>> for BinaryVec {
    fn from(value: BinaryVecRef<'a>) -> Self {
        Self {
            values: value.values.to_bitvec(),
        }
    }
}

impl<'a> From<&'a BinaryVec> for BinaryVecRef<'a> {
    fn from(value: &'a BinaryVec) -> Self {
        Self {
            values: &value.values,
        }
    }
}

impl Vector for BinaryVec {
    fn dims(&self) -> u16 {
        self.values.len().try_into().unwrap()
    }
}

impl<'a> Vector for BinaryVecRef<'a> {
    fn dims(&self) -> u16 {
        self.values.len().try_into().unwrap()
    }
}

impl<'a> From<BinaryVecRef<'a>> for Vec<F32> {
    fn from(value: BinaryVecRef<'a>) -> Self {
        value.values.iter().map(|x| F32(*x as u32 as f32)).collect()
    }
}

impl<'a> BinaryVecRef<'a> {
    pub fn as_bytes(self) -> &'a [usize] {
        // ensure that the slice doesn't contain prefix bits
        assert!(self.values.as_bitptr().pointer().is_aligned());
        unsafe {
            std::slice::from_raw_parts(
                self.values.as_bitptr().pointer(),
                self.values.len().div_ceil(std::mem::size_of::<usize>() * 8),
            )
        }
    }
}
