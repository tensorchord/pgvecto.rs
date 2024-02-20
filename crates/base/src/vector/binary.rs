use super::Vector;
use crate::scalar::F32;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinaryVec {
    pub dims: u16,
    pub data: Vec<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BinaryVecRef<'a> {
    pub dims: u16,
    pub data: &'a [usize],
}

impl<'a> From<BinaryVecRef<'a>> for BinaryVec {
    fn from(value: BinaryVecRef<'a>) -> Self {
        Self {
            dims: value.dims,
            data: value.data.to_vec(),
        }
    }
}

impl<'a> From<&'a BinaryVec> for BinaryVecRef<'a> {
    fn from(value: &'a BinaryVec) -> Self {
        Self {
            dims: value.dims,
            data: &value.data,
        }
    }
}

impl Vector for BinaryVec {
    fn dims(&self) -> u16 {
        self.dims
    }
}

impl<'a> Vector for BinaryVecRef<'a> {
    fn dims(&self) -> u16 {
        self.dims
    }
}

impl<'a> From<BinaryVecRef<'a>> for Vec<F32> {
    fn from(value: BinaryVecRef<'a>) -> Self {
        value.iter().map(|x| F32(x as u32 as f32)).collect()
    }
}

impl<'a> Ord for BinaryVecRef<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        assert_eq!(self.dims, other.dims);
        self.data.cmp(other.data).reverse()
    }
}

impl<'a> PartialOrd for BinaryVecRef<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl BinaryVec {
    pub fn new(dims: u16) -> Self {
        let size = (dims as usize).div_ceil(std::mem::size_of::<usize>() * 8);
        Self {
            dims,
            data: vec![0; size],
        }
    }

    pub fn set(&mut self, index: usize, value: bool) {
        assert!(index < self.dims as usize);
        if value {
            self.data[index / 32] |= 1 << (index % 32);
        } else {
            self.data[index / 32] &= !(1 << (index % 32));
        }
    }
}

impl<'a> BinaryVecRef<'a> {
    pub fn iter(self) -> impl Iterator<Item = bool> + 'a {
        let mut index = 0;
        std::iter::from_fn(move || {
            if index < self.dims as usize {
                let result = self.data[index / 32] & (1 << (index % 32)) != 0;
                index += 1;
                Some(result)
            } else {
                None
            }
        })
    }

    pub fn get(&self, index: usize) -> bool {
        assert!(index < self.dims as usize);
        self.data[index / 32] & (1 << (index % 32)) != 0
    }
}
