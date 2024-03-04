use super::{VectorBorrowed, VectorOwned};
use crate::scalar::F32;
use num_traits::Zero;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SVecf32Owned {
    dims: u32,
    indexes: Vec<u32>,
    values: Vec<F32>,
}

impl SVecf32Owned {
    #[inline(always)]
    pub fn new(dims: u32, indexes: Vec<u32>, values: Vec<F32>) -> Self {
        Self::new_checked(dims, indexes, values).unwrap()
    }
    #[inline(always)]
    pub fn new_checked(dims: u32, indexes: Vec<u32>, values: Vec<F32>) -> Option<Self> {
        if !(1..=1_048_575).contains(&dims) {
            return None;
        }
        if indexes.len() != values.len() {
            return None;
        }
        let len = indexes.len();
        for i in 1..len {
            if !(indexes[i - 1] < indexes[i]) {
                return None;
            }
        }
        if len != 0 && !(indexes[len - 1] < dims) {
            return None;
        }
        for i in 0..len {
            if values[i].is_zero() {
                return None;
            }
        }
        unsafe { Some(Self::new_unchecked(dims, indexes, values)) }
    }
    /// # Safety
    ///
    /// * `dims` must be in `1..=1_048_575`.
    /// * `indexes.len()` must be equal to `values.len()`.
    /// * `indexes` must be a strictly increasing sequence and the last in the sequence must be less than `dims`.
    /// * A floating number in `values` must not be positive zero or negative zero.
    #[inline(always)]
    pub unsafe fn new_unchecked(dims: u32, indexes: Vec<u32>, values: Vec<F32>) -> Self {
        Self {
            dims,
            indexes,
            values,
        }
    }
    #[inline(always)]
    pub fn indexes(&self) -> &[u32] {
        &self.indexes
    }
    #[inline(always)]
    pub fn values(&self) -> &[F32] {
        &self.values
    }
}

impl VectorOwned for SVecf32Owned {
    type Scalar = F32;
    type Borrowed<'a> = SVecf32Borrowed<'a>;

    #[inline(always)]
    fn dims(&self) -> u32 {
        self.dims
    }

    fn for_borrow(&self) -> SVecf32Borrowed<'_> {
        SVecf32Borrowed {
            dims: self.dims,
            indexes: &self.indexes,
            values: &self.values,
        }
    }

    fn to_vec(&self) -> Vec<F32> {
        let mut dense = vec![F32::zero(); self.dims as usize];
        for (&index, &value) in self.indexes.iter().zip(self.values.iter()) {
            dense[index as usize] = value;
        }
        dense
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SVecf32Borrowed<'a> {
    dims: u32,
    indexes: &'a [u32],
    values: &'a [F32],
}

impl<'a> SVecf32Borrowed<'a> {
    #[inline(always)]
    pub fn new(dims: u32, indexes: &'a [u32], values: &'a [F32]) -> Self {
        Self::new_checked(dims, indexes, values).unwrap()
    }
    #[inline(always)]
    pub fn new_checked(dims: u32, indexes: &'a [u32], values: &'a [F32]) -> Option<Self> {
        if !(1..=1_048_575).contains(&dims) {
            return None;
        }
        if indexes.len() != values.len() {
            return None;
        }
        let len = indexes.len();
        for i in 1..len {
            if !(indexes[i - 1] < indexes[i]) {
                return None;
            }
        }
        if len != 0 && !(indexes[len - 1] < dims) {
            return None;
        }
        for i in 0..len {
            if values[i].is_zero() {
                return None;
            }
        }
        unsafe { Some(Self::new_unchecked(dims, indexes, values)) }
    }
    /// # Safety
    ///
    /// * `dims` must be in `1..=1_048_575`.
    /// * `indexes.len()` must be equal to `values.len()`.
    /// * `indexes` must be a strictly increasing sequence and the last in the sequence must be less than `dims`.
    /// * A floating number in `values` must not be positive zero or negative zero.
    #[inline(always)]
    pub unsafe fn new_unchecked(dims: u32, indexes: &'a [u32], values: &'a [F32]) -> Self {
        Self {
            dims,
            indexes,
            values,
        }
    }
    #[inline(always)]
    pub fn indexes(&self) -> &[u32] {
        self.indexes
    }
    #[inline(always)]
    pub fn values(&self) -> &[F32] {
        self.values
    }
}

impl<'a> VectorBorrowed for SVecf32Borrowed<'a> {
    type Scalar = F32;
    type Owned = SVecf32Owned;

    #[inline(always)]
    fn dims(&self) -> u32 {
        self.dims
    }

    fn for_own(&self) -> SVecf32Owned {
        SVecf32Owned {
            dims: self.dims,
            indexes: self.indexes.to_vec(),
            values: self.values.to_vec(),
        }
    }

    fn to_vec(&self) -> Vec<F32> {
        let mut dense = vec![F32::zero(); self.dims as usize];
        for (&index, &value) in self.indexes.iter().zip(self.values.iter()) {
            dense[index as usize] = value;
        }
        dense
    }
}

impl<'a> SVecf32Borrowed<'a> {
    #[inline(always)]
    pub fn len(&self) -> u32 {
        self.indexes.len().try_into().unwrap()
    }
}
