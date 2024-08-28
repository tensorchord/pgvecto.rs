use crate::product::operator::OperatorProductQuantization;
use crate::quantize::{dequantize, quantize};
use crate::scalar::operator::OperatorScalarQuantization;
use crate::trivial::operator::OperatorTrivialQuantization;
use base::distance::Distance;
use base::operator::*;
use base::scalar::ScalarLike;

pub trait OperatorQuantizationProcess: Operator {
    type QuantizationPreprocessed;

    fn process(
        dims: u32,
        ratio: u32,
        bits: u32,
        preprocessed: &Self::QuantizationPreprocessed,
        rhs: impl Fn(usize) -> usize,
    ) -> Distance;
    fn fscan_preprocess(preprocessed: &Self::QuantizationPreprocessed) -> (f32, f32, Vec<u8>);
    fn fscan_process(width: u32, k: f32, b: f32, x: u16) -> Distance;
}

impl<S: ScalarLike> OperatorQuantizationProcess for VectDot<S> {
    type QuantizationPreprocessed = Vec<f32>;

    fn process(
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

    fn fscan_preprocess(preprocessed: &Self::QuantizationPreprocessed) -> (f32, f32, Vec<u8>) {
        quantize::<255>(preprocessed)
    }

    fn fscan_process(width: u32, k: f32, b: f32, x: u16) -> Distance {
        Distance::from(-dequantize(width, k, b, x))
    }
}

impl<S: ScalarLike> OperatorQuantizationProcess for VectL2<S> {
    type QuantizationPreprocessed = Vec<f32>;

    fn process(
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

    fn fscan_preprocess(preprocessed: &Self::QuantizationPreprocessed) -> (f32, f32, Vec<u8>) {
        quantize::<255>(preprocessed)
    }

    fn fscan_process(width: u32, k: f32, b: f32, x: u16) -> Distance {
        Distance::from(dequantize(width, k, b, x))
    }
}

macro_rules! unimpl_operator_quantization_process {
    ($t:ty) => {
        impl OperatorQuantizationProcess for $t {
            type QuantizationPreprocessed = std::convert::Infallible;

            fn process(
                _: u32,
                _: u32,
                _: u32,
                _: &Self::QuantizationPreprocessed,
                _: impl Fn(usize) -> usize,
            ) -> Distance {
                unimplemented!()
            }

            fn fscan_preprocess(
                _: &Self::QuantizationPreprocessed,
            ) -> (f32, f32, Vec<u8>) {
                unimplemented!()
            }

            fn fscan_process(_: u32, _: f32, _: f32, _: u16) -> Distance {
                unimplemented!()
            }
        }
    };
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
