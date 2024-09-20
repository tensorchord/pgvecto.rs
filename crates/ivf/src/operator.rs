use base::operator::*;
use base::scalar::impossible::Impossible;
use base::scalar::ScalarLike;
use base::search::Vectors;
use base::vector::*;
use common::vec2::Vec2;
use quantization::quantizer::Quantizer;
use storage::OperatorStorage;

pub trait OperatorIvf: OperatorStorage {
    type Scalar: ScalarLike;
    fn sample(vectors: &impl Vectors<Self::Vector>, nlist: u32) -> Vec2<Self::Scalar>;
    fn interpret(vector: Borrowed<'_, Self>) -> &[Self::Scalar];
    fn project<Q: Quantizer<Self>>(quantizer: &Q, slice: &[Self::Scalar]) -> Vec<Self::Scalar>;
    const SUPPORT_RESIDUAL: bool;
    fn residual(lhs: Borrowed<'_, Self>, rhs: &[Self::Scalar]) -> Self::Vector;
}

impl OperatorIvf for BVectorDot {
    type Scalar = Impossible;
    fn sample(_: &impl Vectors<Self::Vector>, _: u32) -> Vec2<Self::Scalar> {
        unimplemented!()
    }
    fn interpret(_: Borrowed<'_, Self>) -> &[Self::Scalar] {
        unimplemented!()
    }
    fn project<Q: Quantizer<Self>>(_: &Q, _: &[Self::Scalar]) -> Vec<Self::Scalar> {
        unimplemented!()
    }
    const SUPPORT_RESIDUAL: bool = false;
    fn residual(_lhs: Borrowed<'_, Self>, _rhs: &[Self::Scalar]) -> Self::Vector {
        unimplemented!()
    }
}

impl OperatorIvf for BVectorJaccard {
    type Scalar = Impossible;
    fn sample(_: &impl Vectors<Self::Vector>, _: u32) -> Vec2<Self::Scalar> {
        unimplemented!()
    }
    fn interpret(_: Borrowed<'_, Self>) -> &[Self::Scalar] {
        unimplemented!()
    }
    fn project<Q: Quantizer<Self>>(_: &Q, _: &[Self::Scalar]) -> Vec<Self::Scalar> {
        unimplemented!()
    }
    const SUPPORT_RESIDUAL: bool = false;
    fn residual(_lhs: Borrowed<'_, Self>, _rhs: &[Self::Scalar]) -> Self::Vector {
        unimplemented!()
    }
}

impl OperatorIvf for BVectorHamming {
    type Scalar = Impossible;
    fn sample(_: &impl Vectors<Self::Vector>, _: u32) -> Vec2<Self::Scalar> {
        unimplemented!()
    }
    fn interpret(_: Borrowed<'_, Self>) -> &[Self::Scalar] {
        unimplemented!()
    }
    fn project<Q: Quantizer<Self>>(_: &Q, _: &[Self::Scalar]) -> Vec<Self::Scalar> {
        unimplemented!()
    }
    const SUPPORT_RESIDUAL: bool = false;
    fn residual(_lhs: Borrowed<'_, Self>, _rhs: &[Self::Scalar]) -> Self::Vector {
        unimplemented!()
    }
}

impl OperatorIvf for SVectDot<f32> {
    type Scalar = Impossible;
    fn sample(_: &impl Vectors<Self::Vector>, _: u32) -> Vec2<Self::Scalar> {
        unimplemented!()
    }
    fn interpret(_: Borrowed<'_, Self>) -> &[Self::Scalar] {
        unimplemented!()
    }
    fn project<Q: Quantizer<Self>>(_: &Q, _: &[Self::Scalar]) -> Vec<Self::Scalar> {
        unimplemented!()
    }
    const SUPPORT_RESIDUAL: bool = false;
    fn residual(_lhs: Borrowed<'_, Self>, _rhs: &[Self::Scalar]) -> Self::Vector {
        unimplemented!()
    }
}

impl OperatorIvf for SVectL2<f32> {
    type Scalar = Impossible;
    fn sample(_: &impl Vectors<Self::Vector>, _: u32) -> Vec2<Self::Scalar> {
        unimplemented!()
    }
    fn interpret(_: Borrowed<'_, Self>) -> &[Self::Scalar] {
        unimplemented!()
    }
    fn project<Q: Quantizer<Self>>(_: &Q, _: &[Self::Scalar]) -> Vec<Self::Scalar> {
        unimplemented!()
    }
    const SUPPORT_RESIDUAL: bool = false;
    fn residual(_lhs: Borrowed<'_, Self>, _rhs: &[Self::Scalar]) -> Self::Vector {
        unimplemented!()
    }
}

impl<S: ScalarLike> OperatorIvf for VectDot<S> {
    type Scalar = S;
    fn sample(vectors: &impl Vectors<Self::Vector>, nlist: u32) -> Vec2<Self::Scalar> {
        common::sample::sample(
            vectors.len(),
            nlist.saturating_mul(256).min(1 << 20),
            vectors.dims(),
            |i| vectors.vector(i).slice(),
        )
    }
    fn interpret(x: Borrowed<'_, Self>) -> &[Self::Scalar] {
        x.slice()
    }
    fn project<Q: Quantizer<Self>>(quantizer: &Q, centroid: &[Self::Scalar]) -> Vec<Self::Scalar> {
        quantizer.project(VectBorrowed::new(centroid)).into_vec()
    }
    const SUPPORT_RESIDUAL: bool = false;
    fn residual(_lhs: Borrowed<'_, Self>, _rhs: &[S]) -> Self::Vector {
        unimplemented!()
    }
}

impl<S: ScalarLike> OperatorIvf for VectL2<S> {
    type Scalar = S;
    fn sample(vectors: &impl Vectors<Self::Vector>, nlist: u32) -> Vec2<Self::Scalar> {
        common::sample::sample(
            vectors.len(),
            nlist.saturating_mul(256).min(1 << 20),
            vectors.dims(),
            |i| vectors.vector(i).slice(),
        )
    }
    fn interpret(x: Borrowed<'_, Self>) -> &[Self::Scalar] {
        x.slice()
    }
    fn project<Q: Quantizer<Self>>(quantizer: &Q, vector: &[Self::Scalar]) -> Vec<Self::Scalar> {
        quantizer.project(VectBorrowed::new(vector)).into_vec()
    }
    const SUPPORT_RESIDUAL: bool = true;
    fn residual(lhs: Borrowed<'_, Self>, rhs: &[S]) -> Self::Vector {
        lhs.operator_sub(VectBorrowed::new(rhs))
    }
}
