use base::operator::*;
use base::scalar::impossible::Impossible;
use base::scalar::ScalarLike;
use base::search::Vectors;
use base::vector::*;
use common::vec2::Vec2;
use storage::OperatorStorage;

pub trait OperatorIvf: OperatorStorage {
    const SUPPORT: bool;
    type Scalar: ScalarLike;
    fn sample(
        vectors: &impl Vectors<Self::Vector>,
        nlist: u32,
    ) -> Vec2<<Self as OperatorIvf>::Scalar>;
    fn interpret(vector: Borrowed<'_, Self>) -> &[<Self as OperatorIvf>::Scalar];
    const SUPPORT_RESIDUAL: bool;
    fn residual(lhs: Borrowed<'_, Self>, rhs: &[<Self as OperatorIvf>::Scalar]) -> Owned<Self>;
}

impl OperatorIvf for BVectorDot {
    const SUPPORT: bool = false;
    type Scalar = Impossible;
    fn sample(_: &impl Vectors<Self::Vector>, _: u32) -> Vec2<<Self as OperatorIvf>::Scalar> {
        unimplemented!()
    }
    fn interpret(_: Borrowed<'_, Self>) -> &[<Self as OperatorIvf>::Scalar] {
        unimplemented!()
    }
    const SUPPORT_RESIDUAL: bool = false;
    fn residual(_lhs: Borrowed<'_, Self>, _rhs: &[<Self as OperatorIvf>::Scalar]) -> Owned<Self> {
        unimplemented!()
    }
}

impl OperatorIvf for BVectorJaccard {
    const SUPPORT: bool = false;
    type Scalar = Impossible;
    fn sample(_: &impl Vectors<Self::Vector>, _: u32) -> Vec2<<Self as OperatorIvf>::Scalar> {
        unimplemented!()
    }
    fn interpret(_: Borrowed<'_, Self>) -> &[<Self as OperatorIvf>::Scalar] {
        unimplemented!()
    }
    const SUPPORT_RESIDUAL: bool = false;
    fn residual(_lhs: Borrowed<'_, Self>, _rhs: &[<Self as OperatorIvf>::Scalar]) -> Owned<Self> {
        unimplemented!()
    }
}

impl OperatorIvf for BVectorHamming {
    const SUPPORT: bool = false;
    type Scalar = Impossible;
    fn sample(_: &impl Vectors<Self::Vector>, _: u32) -> Vec2<<Self as OperatorIvf>::Scalar> {
        unimplemented!()
    }
    fn interpret(_: Borrowed<'_, Self>) -> &[<Self as OperatorIvf>::Scalar] {
        unimplemented!()
    }
    const SUPPORT_RESIDUAL: bool = false;
    fn residual(_lhs: Borrowed<'_, Self>, _rhs: &[<Self as OperatorIvf>::Scalar]) -> Owned<Self> {
        unimplemented!()
    }
}

impl OperatorIvf for SVectDot<f32> {
    const SUPPORT: bool = false;
    type Scalar = Impossible;
    fn sample(_: &impl Vectors<Self::Vector>, _: u32) -> Vec2<<Self as OperatorIvf>::Scalar> {
        unimplemented!()
    }
    fn interpret(_: Borrowed<'_, Self>) -> &[<Self as OperatorIvf>::Scalar] {
        unimplemented!()
    }
    const SUPPORT_RESIDUAL: bool = false;
    fn residual(_lhs: Borrowed<'_, Self>, _rhs: &[<Self as OperatorIvf>::Scalar]) -> Owned<Self> {
        unimplemented!()
    }
}

impl OperatorIvf for SVectL2<f32> {
    const SUPPORT: bool = false;
    type Scalar = Impossible;
    fn sample(_: &impl Vectors<Self::Vector>, _: u32) -> Vec2<<Self as OperatorIvf>::Scalar> {
        unimplemented!()
    }
    fn interpret(_: Borrowed<'_, Self>) -> &[<Self as OperatorIvf>::Scalar] {
        unimplemented!()
    }
    const SUPPORT_RESIDUAL: bool = false;
    fn residual(_lhs: Borrowed<'_, Self>, _rhs: &[<Self as OperatorIvf>::Scalar]) -> Owned<Self> {
        unimplemented!()
    }
}

impl<S: ScalarLike> OperatorIvf for VectDot<S> {
    const SUPPORT: bool = true;
    type Scalar = S;
    fn sample(
        vectors: &impl Vectors<Self::Vector>,
        nlist: u32,
    ) -> Vec2<<Self as OperatorIvf>::Scalar> {
        common::sample::sample(
            vectors.len(),
            nlist.saturating_mul(256).min(1 << 20),
            vectors.dims(),
            |i| vectors.vector(i).slice(),
        )
    }
    fn interpret(x: Borrowed<'_, Self>) -> &[<Self as OperatorIvf>::Scalar] {
        x.slice()
    }
    const SUPPORT_RESIDUAL: bool = false;
    fn residual(_lhs: Borrowed<'_, Self>, _rhs: &[S]) -> Owned<Self> {
        unimplemented!()
    }
}

impl<S: ScalarLike> OperatorIvf for VectL2<S> {
    const SUPPORT: bool = true;
    type Scalar = S;
    fn sample(
        vectors: &impl Vectors<Self::Vector>,
        nlist: u32,
    ) -> Vec2<<Self as OperatorIvf>::Scalar> {
        common::sample::sample(
            vectors.len(),
            nlist.saturating_mul(256).min(1 << 20),
            vectors.dims(),
            |i| vectors.vector(i).slice(),
        )
    }
    fn interpret(x: Borrowed<'_, Self>) -> &[<Self as OperatorIvf>::Scalar] {
        x.slice()
    }
    const SUPPORT_RESIDUAL: bool = true;
    fn residual(lhs: Borrowed<'_, Self>, rhs: &[S]) -> Owned<Self> {
        lhs.operator_sub(VectBorrowed::new(rhs))
    }
}
