mod bvector;
mod svec;
mod vec;
mod veci8;

use base::operator::*;
use base::scalar::*;
use base::search::*;
use std::path::Path;

pub trait Storage<O: Operator>: Vectors<O> {
    fn open(path: impl AsRef<Path>) -> Self;
    fn create(path: impl AsRef<Path>, vectors: &impl Vectors<O>) -> Self;
}

pub trait OperatorStorage: Operator {
    type Storage: Storage<Self> + Send + Sync;
}

impl OperatorStorage for SVecf32Cos {
    type Storage = svec::SVecStorage;
}

impl OperatorStorage for SVecf32Dot {
    type Storage = svec::SVecStorage;
}

impl OperatorStorage for SVecf32L2 {
    type Storage = svec::SVecStorage;
}

impl OperatorStorage for Vecf16Cos {
    type Storage = vec::VecStorage<F16>;
}

impl OperatorStorage for Vecf16Dot {
    type Storage = vec::VecStorage<F16>;
}

impl OperatorStorage for Vecf16L2 {
    type Storage = vec::VecStorage<F16>;
}

impl OperatorStorage for Vecf32Cos {
    type Storage = vec::VecStorage<F32>;
}

impl OperatorStorage for Vecf32Dot {
    type Storage = vec::VecStorage<F32>;
}

impl OperatorStorage for Vecf32L2 {
    type Storage = vec::VecStorage<F32>;
}

impl OperatorStorage for BVecf32Cos {
    type Storage = bvector::BVectorStorage;
}

impl OperatorStorage for BVecf32Dot {
    type Storage = bvector::BVectorStorage;
}

impl OperatorStorage for BVecf32L2 {
    type Storage = bvector::BVectorStorage;
}

impl OperatorStorage for BVecf32Jaccard {
    type Storage = bvector::BVectorStorage;
}

impl OperatorStorage for Veci8Cos {
    type Storage = veci8::Veci8Storage;
}

impl OperatorStorage for Veci8Dot {
    type Storage = veci8::Veci8Storage;
}

impl OperatorStorage for Veci8L2 {
    type Storage = veci8::Veci8Storage;
}
