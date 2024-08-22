pub mod bvect;
pub mod svect;
pub mod vect;

pub use bvect::{BVectBorrowed, BVectOwned, BVECTOR_WIDTH};
pub use svect::{SVectBorrowed, SVectOwned};
pub use vect::{VectBorrowed, VectOwned};

use crate::distance::Distance;
use half::f16;
use serde::{Deserialize, Serialize};
use std::ops::RangeBounds;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum VectorKind {
    Vecf32,
    Vecf16,
    SVecf32,
    BVector,
}

pub trait VectorOwned: Clone + Serialize + for<'a> Deserialize<'a> + 'static {
    type Borrowed<'a>: VectorBorrowed<Owned = Self>;

    fn as_borrowed(&self) -> Self::Borrowed<'_>;
}

pub trait VectorBorrowed: Copy + PartialEq + PartialOrd {
    type Owned: VectorOwned;

    fn own(&self) -> Self::Owned;

    fn dims(&self) -> u32;

    fn norm(&self) -> f32;

    fn operator_dot(self, rhs: Self) -> Distance;

    fn operator_l2(self, rhs: Self) -> Distance;

    fn operator_cos(self, rhs: Self) -> Distance;

    fn operator_hamming(self, rhs: Self) -> Distance;

    fn operator_jaccard(self, rhs: Self) -> Distance;

    fn function_normalize(&self) -> Self::Owned;

    fn operator_add(&self, rhs: Self) -> Self::Owned;

    fn operator_sub(&self, rhs: Self) -> Self::Owned;

    fn operator_mul(&self, rhs: Self) -> Self::Owned;

    fn operator_and(&self, rhs: Self) -> Self::Owned;

    fn operator_or(&self, rhs: Self) -> Self::Owned;

    fn operator_xor(&self, rhs: Self) -> Self::Owned;

    fn subvector(&self, bounds: impl RangeBounds<u32>) -> Option<Self::Owned>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OwnedVector {
    Vecf32(VectOwned<f32>),
    Vecf16(VectOwned<f16>),
    SVecf32(SVectOwned<f32>),
    BVector(BVectOwned),
}

impl OwnedVector {
    pub fn as_borrowed(&self) -> BorrowedVector<'_> {
        match self {
            OwnedVector::Vecf32(x) => BorrowedVector::Vecf32(x.as_borrowed()),
            OwnedVector::Vecf16(x) => BorrowedVector::Vecf16(x.as_borrowed()),
            OwnedVector::SVecf32(x) => BorrowedVector::SVecf32(x.as_borrowed()),
            OwnedVector::BVector(x) => BorrowedVector::BVector(x.as_borrowed()),
        }
    }
}

impl PartialEq for OwnedVector {
    fn eq(&self, other: &Self) -> bool {
        self.as_borrowed().eq(&other.as_borrowed())
    }
}

impl PartialOrd for OwnedVector {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.as_borrowed().partial_cmp(&other.as_borrowed())
    }
}

#[derive(Debug, Clone, Copy)]
pub enum BorrowedVector<'a> {
    Vecf32(VectBorrowed<'a, f32>),
    Vecf16(VectBorrowed<'a, f16>),
    SVecf32(SVectBorrowed<'a, f32>),
    BVector(BVectBorrowed<'a>),
}

impl PartialEq for BorrowedVector<'_> {
    fn eq(&self, other: &Self) -> bool {
        use BorrowedVector::*;
        match (self, other) {
            (Vecf32(lhs), Vecf32(rhs)) => lhs == rhs,
            (Vecf16(lhs), Vecf16(rhs)) => lhs == rhs,
            (SVecf32(lhs), SVecf32(rhs)) => lhs == rhs,
            (BVector(lhs), BVector(rhs)) => lhs == rhs,
            _ => false,
        }
    }
}

impl PartialOrd for BorrowedVector<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        use BorrowedVector::*;
        match (self, other) {
            (Vecf32(lhs), Vecf32(rhs)) => lhs.partial_cmp(rhs),
            (Vecf16(lhs), Vecf16(rhs)) => lhs.partial_cmp(rhs),
            (SVecf32(lhs), SVecf32(rhs)) => lhs.partial_cmp(rhs),
            (BVector(lhs), BVector(rhs)) => lhs.partial_cmp(rhs),
            _ => None,
        }
    }
}
