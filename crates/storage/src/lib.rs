mod bvector;
mod svec;
mod vec;

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

impl OperatorStorage for SVecf32Dot {
    type Storage = svec::SVecStorage;
}

impl OperatorStorage for SVecf32L2 {
    type Storage = svec::SVecStorage;
}

impl OperatorStorage for Vecf16Dot {
    type Storage = vec::VecStorage<F16>;
}

impl OperatorStorage for Vecf16L2 {
    type Storage = vec::VecStorage<F16>;
}

impl OperatorStorage for Vecf32Dot {
    type Storage = vec::VecStorage<F32>;
}

impl OperatorStorage for Vecf32L2 {
    type Storage = vec::VecStorage<F32>;
}

impl OperatorStorage for BVectorDot {
    type Storage = bvector::BVectorStorage;
}

impl OperatorStorage for BVectorHamming {
    type Storage = bvector::BVectorStorage;
}

impl OperatorStorage for BVectorJaccard {
    type Storage = bvector::BVectorStorage;
}
