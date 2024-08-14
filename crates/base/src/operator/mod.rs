mod bvector_dot;
mod bvector_hamming;
mod bvector_jaccard;
mod svecf32_dot;
mod svecf32_l2;
mod vecf16_dot;
mod vecf16_l2;
mod vecf32_dot;
mod vecf32_l2;

pub use bvector_dot::BVectorDot;
pub use bvector_hamming::BVectorHamming;
pub use bvector_jaccard::BVectorJaccard;
pub use svecf32_dot::SVecf32Dot;
pub use svecf32_l2::SVecf32L2;
pub use vecf16_dot::Vecf16Dot;
pub use vecf16_l2::Vecf16L2;
pub use vecf32_dot::Vecf32Dot;
pub use vecf32_l2::Vecf32L2;

use crate::distance::*;
use crate::scalar::*;
use crate::vector::*;

pub trait Operator: Copy + 'static + Send + Sync {
    type VectorOwned: VectorOwned;

    const DISTANCE_KIND: DistanceKind;

    fn distance(lhs: Borrowed<'_, Self>, rhs: Borrowed<'_, Self>) -> F32;
}

pub type Owned<T> = <T as Operator>::VectorOwned;
pub type Borrowed<'a, T> = <<T as Operator>::VectorOwned as VectorOwned>::Borrowed<'a>;
pub type Scalar<T> = <<T as Operator>::VectorOwned as VectorOwned>::Scalar;
