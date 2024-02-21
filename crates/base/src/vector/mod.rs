mod binary;
mod sparse_f32;

pub use binary::{BinaryVec, BinaryVecRef, BVEC_WIDTH};
pub use sparse_f32::{SparseF32, SparseF32Ref};

pub trait Vector {
    fn dims(&self) -> u16;
}

impl<T> Vector for Vec<T> {
    fn dims(&self) -> u16 {
        self.len().try_into().unwrap()
    }
}

impl<'a, T> Vector for &'a [T] {
    fn dims(&self) -> u16 {
        self.len().try_into().unwrap()
    }
}
