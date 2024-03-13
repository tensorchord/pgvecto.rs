mod bvecf32_cos;
mod bvecf32_dot;
mod bvecf32_jaccard;
mod bvecf32_l2;
mod svecf32_cos;
mod svecf32_dot;
mod svecf32_l2;
mod vecf16_cos;
mod vecf16_dot;
mod vecf16_l2;
mod vecf32_cos;
mod vecf32_dot;
mod vecf32_l2;
mod veci8_cos;
mod veci8_dot;
mod veci8_l2;

pub use bvecf32_cos::BVecf32Cos;
pub use bvecf32_dot::BVecf32Dot;
pub use bvecf32_jaccard::BVecf32Jaccard;
pub use bvecf32_l2::BVecf32L2;
pub use svecf32_cos::SVecf32Cos;
pub use svecf32_dot::SVecf32Dot;
pub use svecf32_l2::SVecf32L2;
pub use vecf16_cos::Vecf16Cos;
pub use vecf16_dot::Vecf16Dot;
pub use vecf16_l2::Vecf16L2;
pub use vecf32_cos::Vecf32Cos;
pub use vecf32_dot::Vecf32Dot;
pub use vecf32_l2::Vecf32L2;
pub use veci8_cos::Veci8Cos;
pub use veci8_dot::Veci8Dot;
pub use veci8_l2::Veci8L2;

use crate::distance::*;
use crate::scalar::*;
use crate::vector::*;

pub trait Operator: Copy + 'static {
    type VectorOwned: VectorOwned;

    const DISTANCE_KIND: DistanceKind;

    fn distance(lhs: Borrowed<'_, Self>, rhs: Borrowed<'_, Self>) -> F32;
}

pub type Owned<T> = <T as Operator>::VectorOwned;
pub type Borrowed<'a, T> = <<T as Operator>::VectorOwned as VectorOwned>::Borrowed<'a>;
pub type Scalar<T> = <<T as Operator>::VectorOwned as VectorOwned>::Scalar;
