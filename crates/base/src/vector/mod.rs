pub mod bvecf32;
pub mod svecf32;
pub mod vecf16;
pub mod vecf32;
pub mod veci8;

pub use bvecf32::{BVecf32Borrowed, BVecf32Owned, BVEC_WIDTH};
pub use svecf32::{SVecf32Borrowed, SVecf32Owned};
pub use vecf16::{Vecf16Borrowed, Vecf16Owned};
pub use vecf32::{Vecf32Borrowed, Vecf32Owned};
pub use veci8::{Veci8Borrowed, Veci8Owned};

use crate::scalar::ScalarLike;
use serde::{Deserialize, Serialize};

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

    fn for_borrow(&self) -> Self::Borrowed<'_>;

    fn dims(&self) -> u32;

    fn to_vec(&self) -> Vec<Self::Scalar>;
}

pub trait VectorBorrowed: Copy {
    type Scalar: ScalarLike;
    type Owned: VectorOwned<Scalar = Self::Scalar>;

    fn for_own(&self) -> Self::Owned;

    fn dims(&self) -> u32;

    fn to_vec(&self) -> Vec<Self::Scalar>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OwnedVector {
    Vecf32(Vecf32Owned),
    Vecf16(Vecf16Owned),
    SVecf32(SVecf32Owned),
    BVecf32(BVecf32Owned),
    Veci8(Veci8Owned),
}

#[derive(Debug, Clone)]
pub enum BorrowedVector<'a> {
    Vecf32(Vecf32Borrowed<'a>),
    Vecf16(Vecf16Borrowed<'a>),
    SVecf32(SVecf32Borrowed<'a>),
    BVecf32(BVecf32Borrowed<'a>),
    Veci8(Veci8Borrowed<'a>),
}
