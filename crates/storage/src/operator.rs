use super::bvector::BVectorStorage;
use super::svec::SVecStorage;
use super::vec::VecStorage;
use super::veci8::Veci8Storage;
use crate::Storage;
use base::operator::*;
use base::scalar::*;

pub trait OperatorStorage: Operator {
    type Storage: Storage<VectorOwned = Self::VectorOwned>;
}

impl OperatorStorage for SVecf32Cos {
    type Storage = SVecStorage;
}

impl OperatorStorage for SVecf32Dot {
    type Storage = SVecStorage;
}

impl OperatorStorage for SVecf32L2 {
    type Storage = SVecStorage;
}

impl OperatorStorage for Vecf16Cos {
    type Storage = VecStorage<F16>;
}

impl OperatorStorage for Vecf16Dot {
    type Storage = VecStorage<F16>;
}

impl OperatorStorage for Vecf16L2 {
    type Storage = VecStorage<F16>;
}

impl OperatorStorage for Vecf32Cos {
    type Storage = VecStorage<F32>;
}

impl OperatorStorage for Vecf32Dot {
    type Storage = VecStorage<F32>;
}

impl OperatorStorage for Vecf32L2 {
    type Storage = VecStorage<F32>;
}

impl OperatorStorage for BVecf32Cos {
    type Storage = BVectorStorage;
}

impl OperatorStorage for BVecf32Dot {
    type Storage = BVectorStorage;
}

impl OperatorStorage for BVecf32L2 {
    type Storage = BVectorStorage;
}

impl OperatorStorage for BVecf32Jaccard {
    type Storage = BVectorStorage;
}

impl OperatorStorage for Veci8Cos {
    type Storage = Veci8Storage;
}

impl OperatorStorage for Veci8Dot {
    type Storage = Veci8Storage;
}

impl OperatorStorage for Veci8L2 {
    type Storage = Veci8Storage;
}
