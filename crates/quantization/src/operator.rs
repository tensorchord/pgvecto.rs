use crate::product::operator::OperatorProductQuantization;
use crate::scalar::operator::OperatorScalarQuantization;
use crate::trivial::operator::OperatorTrivialQuantization;
use base::distance::Distance;
use base::operator::*;
use base::scalar::ScalarLike;

pub trait OperatorQuantizationProcess: Operator {
    type QuantizationPreprocessed;

    fn quantization_process(
        dims: u32,
        ratio: u32,
        bits: u32,
        preprocessed: &Self::QuantizationPreprocessed,
        rhs: impl Fn(usize) -> usize,
    ) -> Distance;

    const SUPPORT_FAST_SCAN: bool;
    fn fast_scan(preprocessed: &Self::QuantizationPreprocessed) -> Vec<f32>;
    fn fast_scan_resolve(x: f32) -> Distance;
}

macro_rules! unimpl_operator_quantization_process {
    ($t:ty) => {
        impl OperatorQuantizationProcess for $t {
            type QuantizationPreprocessed = std::convert::Infallible;

            fn quantization_process(
                _: u32,
                _: u32,
                _: u32,
                preprocessed: &Self::QuantizationPreprocessed,
                _: impl Fn(usize) -> usize,
            ) -> Distance {
                match *preprocessed {}
            }

            const SUPPORT_FAST_SCAN: bool = false;

            fn fast_scan(preprocessed: &Self::QuantizationPreprocessed) -> Vec<f32> {
                match *preprocessed {}
            }

            fn fast_scan_resolve(_: f32) -> Distance {
                unimplemented!()
            }
        }
    };
}

impl<S: ScalarLike> OperatorQuantizationProcess for VectDot<S> {
    type QuantizationPreprocessed = Vec<f32>;

    fn quantization_process(
        dims: u32,
        ratio: u32,
        bits: u32,
        preprocessed: &Self::QuantizationPreprocessed,
        rhs: impl Fn(usize) -> usize,
    ) -> Distance {
        let width = dims.div_ceil(ratio);
        let xy = {
            let mut xy = 0.0f32;
            for i in 0..width as usize {
                xy += preprocessed[i * (1 << bits) + rhs(i)];
            }
            xy
        };
        Distance::from(0.0f32 - xy)
    }

    const SUPPORT_FAST_SCAN: bool = true;

    fn fast_scan(preprocessed: &Self::QuantizationPreprocessed) -> Vec<f32> {
        preprocessed.clone()
    }

    fn fast_scan_resolve(x: f32) -> Distance {
        Distance::from(-x)
    }
}

impl<S: ScalarLike> OperatorQuantizationProcess for VectL2<S> {
    type QuantizationPreprocessed = Vec<f32>;

    fn quantization_process(
        dims: u32,
        ratio: u32,
        bits: u32,
        preprocessed: &Self::QuantizationPreprocessed,
        rhs: impl Fn(usize) -> usize,
    ) -> Distance {
        let width = dims.div_ceil(ratio);
        let mut d2 = 0.0f32;
        for i in 0..width as usize {
            d2 += preprocessed[i * (1 << bits) + rhs(i)];
        }
        Distance::from(d2)
    }

    const SUPPORT_FAST_SCAN: bool = true;

    fn fast_scan(preprocessed: &Self::QuantizationPreprocessed) -> Vec<f32> {
        preprocessed.clone()
    }

    fn fast_scan_resolve(x: f32) -> Distance {
        Distance::from(x)
    }
}

unimpl_operator_quantization_process!(BVectorDot);
unimpl_operator_quantization_process!(BVectorHamming);
unimpl_operator_quantization_process!(BVectorJaccard);

unimpl_operator_quantization_process!(SVectDot<f32>);
unimpl_operator_quantization_process!(SVectL2<f32>);

pub trait OperatorQuantization:
    OperatorQuantizationProcess
    + OperatorTrivialQuantization
    + OperatorScalarQuantization
    + OperatorProductQuantization
{
}

impl OperatorQuantization for BVectorDot {}
impl OperatorQuantization for BVectorJaccard {}
impl OperatorQuantization for BVectorHamming {}
impl OperatorQuantization for SVectDot<f32> {}
impl OperatorQuantization for SVectL2<f32> {}
impl<S: ScalarLike> OperatorQuantization for VectDot<S> {}
impl<S: ScalarLike> OperatorQuantization for VectL2<S> {}
