use super::{VectorBorrowed, VectorOwned};
use crate::scalar::F32;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[repr(transparent)]
pub struct Vecf32Owned(Vec<F32>);

impl Vecf32Owned {
    #[inline(always)]
    pub fn new(slice: Vec<F32>) -> Self {
        Self::new_checked(slice).unwrap()
    }
    #[inline(always)]
    pub fn new_checked(slice: Vec<F32>) -> Option<Self> {
        if !(1 <= slice.len() && slice.len() <= 65535) {
            return None;
        }
        Some(unsafe { Self::new_unchecked(slice) })
    }
    /// # Safety
    ///
    /// * `slice.len()` must not be zero.
    #[inline(always)]
    pub unsafe fn new_unchecked(slice: Vec<F32>) -> Self {
        Self(slice)
    }
    #[inline(always)]
    pub fn slice(&self) -> &[F32] {
        self.0.as_slice()
    }
    #[inline(always)]
    pub fn slice_mut(&mut self) -> &mut [F32] {
        self.0.as_mut_slice()
    }
}

impl VectorOwned for Vecf32Owned {
    type Scalar = F32;
    type Borrowed<'a> = Vecf32Borrowed<'a>;

    fn dims(&self) -> u32 {
        self.0.len() as u32
    }

    fn for_borrow(&self) -> Vecf32Borrowed<'_> {
        Vecf32Borrowed(self.0.as_slice())
    }

    fn to_vec(&self) -> Vec<F32> {
        self.0.clone()
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct Vecf32Borrowed<'a>(&'a [F32]);

impl<'a> Vecf32Borrowed<'a> {
    #[inline(always)]
    pub fn new(slice: &'a [F32]) -> Self {
        Self::new_checked(slice).unwrap()
    }
    #[inline(always)]
    pub fn new_checked(slice: &'a [F32]) -> Option<Self> {
        if !(1 <= slice.len() && slice.len() <= 65535) {
            return None;
        }
        Some(unsafe { Self::new_unchecked(slice) })
    }
    /// # Safety
    ///
    /// * `slice.len()` must not be zero.
    #[inline(always)]
    pub unsafe fn new_unchecked(slice: &'a [F32]) -> Self {
        Self(slice)
    }
    #[inline(always)]
    pub fn slice(&self) -> &[F32] {
        self.0
    }
}

impl<'a> VectorBorrowed for Vecf32Borrowed<'a> {
    type Scalar = F32;
    type Owned = Vecf32Owned;

    fn dims(&self) -> u32 {
        self.0.len() as u32
    }

    fn for_own(&self) -> Vecf32Owned {
        Vecf32Owned(self.0.to_vec())
    }

    fn to_vec(&self) -> Vec<F32> {
        self.0.to_vec()
    }
}
