use super::{VectorBorrowed, VectorOwned};
use crate::scalar::F16;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[repr(transparent)]
pub struct Vecf16Owned(Vec<F16>);

impl Vecf16Owned {
    #[inline(always)]
    pub fn new(slice: Vec<F16>) -> Self {
        Self::new_checked(slice).unwrap()
    }
    #[inline(always)]
    pub fn new_checked(slice: Vec<F16>) -> Option<Self> {
        if !(1 <= slice.len() && slice.len() <= 65535) {
            return None;
        }
        Some(unsafe { Self::new_unchecked(slice) })
    }
    /// # Safety
    ///
    /// * `slice.len()` must not be zero.
    #[inline(always)]
    pub unsafe fn new_unchecked(slice: Vec<F16>) -> Self {
        Self(slice)
    }
    #[inline(always)]
    pub fn slice(&self) -> &[F16] {
        self.0.as_slice()
    }
    #[inline(always)]
    pub fn slice_mut(&mut self) -> &mut [F16] {
        self.0.as_mut_slice()
    }
}

impl VectorOwned for Vecf16Owned {
    type Scalar = F16;
    type Borrowed<'a> = Vecf16Borrowed<'a>;

    fn dims(&self) -> u32 {
        self.0.len() as u32
    }

    fn for_borrow(&self) -> Vecf16Borrowed<'_> {
        Vecf16Borrowed(self.0.as_slice())
    }

    fn to_vec(&self) -> Vec<F16> {
        self.0.clone()
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct Vecf16Borrowed<'a>(&'a [F16]);

impl<'a> Vecf16Borrowed<'a> {
    #[inline(always)]
    pub fn new(slice: &'a [F16]) -> Self {
        Self::new_checked(slice).unwrap()
    }
    #[inline(always)]
    pub fn new_checked(slice: &'a [F16]) -> Option<Self> {
        if !(1 <= slice.len() && slice.len() <= 65535) {
            return None;
        }
        Some(unsafe { Self::new_unchecked(slice) })
    }
    /// # Safety
    ///
    /// * `slice.len()` must not be zero.
    #[inline(always)]
    pub unsafe fn new_unchecked(slice: &'a [F16]) -> Self {
        Self(slice)
    }
    #[inline(always)]
    pub fn slice(&self) -> &[F16] {
        self.0
    }
}

impl<'a> VectorBorrowed for Vecf16Borrowed<'a> {
    type Scalar = F16;
    type Owned = Vecf16Owned;

    fn dims(&self) -> u32 {
        self.0.len() as u32
    }

    fn for_own(&self) -> Vecf16Owned {
        Vecf16Owned(self.0.to_vec())
    }

    fn to_vec(&self) -> Vec<F16> {
        self.0.to_vec()
    }
}
