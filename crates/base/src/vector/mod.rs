pub mod bvecf32;
pub mod svecf32;
pub mod vecf16;
pub mod vecf32;
pub mod veci8;

pub use bvecf32::{BVecf32Borrowed, BVecf32Owned, BVECF32_WIDTH};
pub use svecf32::{SVecf32Borrowed, SVecf32Owned};
pub use vecf16::{Vecf16Borrowed, Vecf16Owned};
pub use vecf32::{Vecf32Borrowed, Vecf32Owned};
pub use veci8::{Veci8Borrowed, Veci8Owned};

use crate::scalar::ScalarLike;
use crate::scalar::F32;
use serde::{Deserialize, Serialize};
use std::ops::RangeBounds;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum VectorKind {
    Vecf32,
    Vecf16,
    SVecf32,
    BVecf32,
    Veci8,
}

pub trait VectorOwned: Clone + Serialize + for<'a> Deserialize<'a> + 'static {
    type Scalar: ScalarLike;
    type Borrowed<'a>: VectorBorrowed<Scalar = Self::Scalar, Owned = Self>;

    const VECTOR_KIND: VectorKind;

    fn as_borrowed(&self) -> Self::Borrowed<'_>;
}

pub trait VectorBorrowed: Copy + PartialEq + PartialOrd {
    type Scalar: ScalarLike;
    type Owned: VectorOwned<Scalar = Self::Scalar>;

    fn own(&self) -> Self::Owned;

    fn dims(&self) -> u32;

    fn to_vec(&self) -> Vec<Self::Scalar>;

    fn length(&self) -> F32;

    fn function_normalize(&self) -> Self::Owned;

    fn operator_add(&self, rhs: Self) -> Self::Owned;

    fn operator_minus(&self, rhs: Self) -> Self::Owned;

    fn operator_mul(&self, rhs: Self) -> Self::Owned;

    fn operator_and(&self, rhs: Self) -> Self::Owned;

    fn operator_or(&self, rhs: Self) -> Self::Owned;

    fn operator_xor(&self, rhs: Self) -> Self::Owned;

    fn subvector(&self, bounds: impl RangeBounds<u32>) -> Option<Self::Owned>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OwnedVector {
    Vecf32(Vecf32Owned),
    Vecf16(Vecf16Owned),
    SVecf32(SVecf32Owned),
    BVecf32(BVecf32Owned),
    Veci8(Veci8Owned),
}

impl OwnedVector {
    pub fn as_borrowed(&self) -> BorrowedVector<'_> {
        match self {
            OwnedVector::Vecf32(x) => BorrowedVector::Vecf32(x.as_borrowed()),
            OwnedVector::Vecf16(x) => BorrowedVector::Vecf16(x.as_borrowed()),
            OwnedVector::SVecf32(x) => BorrowedVector::SVecf32(x.as_borrowed()),
            OwnedVector::BVecf32(x) => BorrowedVector::BVecf32(x.as_borrowed()),
            OwnedVector::Veci8(x) => BorrowedVector::Veci8(x.as_borrowed()),
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
    Vecf32(Vecf32Borrowed<'a>),
    Vecf16(Vecf16Borrowed<'a>),
    SVecf32(SVecf32Borrowed<'a>),
    BVecf32(BVecf32Borrowed<'a>),
    Veci8(Veci8Borrowed<'a>),
}

impl PartialEq for BorrowedVector<'_> {
    fn eq(&self, other: &Self) -> bool {
        use BorrowedVector::*;
        match (self, other) {
            (Vecf32(lhs), Vecf32(rhs)) => lhs == rhs,
            (Vecf16(lhs), Vecf16(rhs)) => lhs == rhs,
            (SVecf32(lhs), SVecf32(rhs)) => lhs == rhs,
            (BVecf32(lhs), BVecf32(rhs)) => lhs == rhs,
            (Veci8(lhs), Veci8(rhs)) => lhs == rhs,
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
            (BVecf32(lhs), BVecf32(rhs)) => lhs.partial_cmp(rhs),
            (Veci8(lhs), Veci8(rhs)) => lhs.partial_cmp(rhs),
            _ => None,
        }
    }
}
