pub use base::distance::*;
pub use base::error::*;
pub use base::global::*;
pub use base::index::*;
pub use base::scalar::*;
pub use base::search::*;
pub use base::vector::*;
pub use num_traits::{Float, Zero};

use crate::storage::GlobalStorage;

pub trait G:
    Global + GlobalElkanKMeans + GlobalProductQuantization + GlobalScalarQuantization + GlobalStorage
{
}

impl G for SVecf32Cos {}
impl G for SVecf32Dot {}
impl G for SVecf32L2 {}
impl G for Vecf16Cos {}
impl G for Vecf16Dot {}
impl G for Vecf16L2 {}
impl G for Vecf32Cos {}
impl G for Vecf32Dot {}
impl G for Vecf32L2 {}
impl G for BVecf32Cos {}
impl G for BVecf32Dot {}
impl G for BVecf32L2 {}
impl G for BVecf32Jaccard {}

impl G for Veci8Cos {}

impl G for Veci8Dot {}

impl G for Veci8L2 {}
