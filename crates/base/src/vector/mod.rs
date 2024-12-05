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

    fn zero(dims: u32) -> Self;
}

pub trait VectorBorrowed: Copy {
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

#[derive(Debug, Clone, Copy)]
pub enum BorrowedVector<'a> {
    Vecf32(VectBorrowed<'a, f32>),
    Vecf16(VectBorrowed<'a, f16>),
    SVecf32(SVectBorrowed<'a, f32>),
    BVector(BVectBorrowed<'a>),
}
