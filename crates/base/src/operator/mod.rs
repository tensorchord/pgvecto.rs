mod bvect_dot;
mod bvect_hamming;
mod bvect_jaccard;
mod svect_dot;
mod svect_l2;
mod vect_dot;
mod vect_l2;

pub use bvect_dot::BVectorDot;
pub use bvect_hamming::BVectorHamming;
pub use bvect_jaccard::BVectorJaccard;
pub use svect_dot::SVectDot;
pub use svect_l2::SVectL2;
pub use vect_dot::VectDot;
pub use vect_l2::VectL2;

use crate::distance::*;
use crate::vector::*;

pub trait Operator: Copy + 'static + Send + Sync {
    type Vector: VectorOwned;

    fn distance(lhs: Borrowed<'_, Self>, rhs: Borrowed<'_, Self>) -> Distance;
}

pub type Owned<T> = <T as Operator>::Vector;
pub type Borrowed<'a, T> = <<T as Operator>::Vector as VectorOwned>::Borrowed<'a>;
