mod bvector;
mod svec;
mod vec;

use base::operator::*;
use base::search::*;
use base::simd::ScalarLike;
use base::vector::VectorOwned;
use std::path::Path;

pub trait Storage<V: VectorOwned>: Vectors<V> {
    fn open(path: impl AsRef<Path>) -> Self;
    fn create(path: impl AsRef<Path>, vectors: &impl Vectors<V>) -> Self;
}

pub trait OperatorStorage: Operator {
    type Storage: Storage<Self::Vector> + Send + Sync;
}

impl<S: ScalarLike> OperatorStorage for SVectDot<S> {
    type Storage = svec::SVecStorage<S>;
}

impl<S: ScalarLike> OperatorStorage for SVectL2<S> {
    type Storage = svec::SVecStorage<S>;
}

impl<S: ScalarLike> OperatorStorage for VectDot<S> {
    type Storage = vec::VecStorage<S>;
}

impl<S: ScalarLike> OperatorStorage for VectL2<S> {
    type Storage = vec::VecStorage<S>;
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
