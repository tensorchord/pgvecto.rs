use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::{Debug, Display};

use super::F32;

#[derive(Clone, Copy, Default, Serialize, Deserialize)]
#[repr(transparent)]
#[serde(transparent)]
pub struct I8(pub i8);

impl Debug for I8 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

impl Display for I8 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl PartialEq for I8 {
    fn eq(&self, other: &Self) -> bool {
        self.0.cmp(&other.0) == Ordering::Equal
    }
}

impl Eq for I8 {}

impl PartialOrd for I8 {
    #[inline(always)]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(Ord::cmp(self, other))
    }
}

impl Ord for I8 {
    #[inline(always)]
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

unsafe impl bytemuck::Zeroable for I8 {}

unsafe impl bytemuck::Pod for I8 {}

impl From<i8> for I8 {
    fn from(value: i8) -> Self {
        Self(value)
    }
}

impl From<I8> for i8 {
    fn from(I8(value): I8) -> Self {
        value
    }
}

impl From<F32> for I8 {
    fn from(F32(value): F32) -> Self {
        // Because F32 may be out of range of i8 [-128, 127], so we can't use to_int_unchecked here.
        Self(value as i8)
    }
}

impl From<I8> for F32 {
    fn from(val: I8) -> Self {
        F32(val.0 as f32)
    }
}

impl I8 {
    #[inline(always)]
    pub fn to_f32(self) -> F32 {
        F32(self.0 as f32)
    }
}
