use crate::operator::OperatorQuantizationProcess;
use base::operator::*;
use base::scalar::impossible::Impossible;
use base::scalar::ScalarLike;

pub trait OperatorScalarQuantization: Operator + OperatorQuantizationProcess {
    type Scalar: ScalarLike;
    fn get(vector: Borrowed<'_, Self>, i: u32) -> Self::Scalar;
    fn scalar_quantization_preprocess(
        dims: u32,
        bits: u32,
        max: &[f32],
        min: &[f32],
        lhs: Borrowed<'_, Self>,
    ) -> Self::QuantizationPreprocessed;
}

impl<S: ScalarLike> OperatorScalarQuantization for VectDot<S> {
    type Scalar = S;
    fn get(vector: Borrowed<'_, Self>, i: u32) -> Self::Scalar {
        vector.slice()[i as usize]
    }
    fn scalar_quantization_preprocess(
        dims: u32,
        bits: u32,
        max: &[f32],
        min: &[f32],
        lhs: Borrowed<'_, Self>,
    ) -> Self::QuantizationPreprocessed {
        let mut xy = Vec::with_capacity(dims as _);
        for i in 0..dims {
            let bas = min[i as usize];
            let del = max[i as usize] - min[i as usize];
            xy.extend((0..1 << bits).map(|k| {
                let x = lhs.slice()[i as usize].to_f32();
                let val = k as f32 / ((1 << bits) - 1) as f32;
                let y = bas + val * del;
                x * y
            }));
        }
        xy
    }
}

impl<S: ScalarLike> OperatorScalarQuantization for VectL2<S> {
    type Scalar = S;
    fn get(vector: Borrowed<'_, Self>, i: u32) -> Self::Scalar {
        vector.slice()[i as usize]
    }
    fn scalar_quantization_preprocess(
        dims: u32,
        bits: u32,
        max: &[f32],
        min: &[f32],
        lhs: Borrowed<'_, Self>,
    ) -> Self::QuantizationPreprocessed {
        let mut d2 = Vec::with_capacity(dims as _);
        for i in 0..dims {
            let bas = min[i as usize];
            let del = max[i as usize] - min[i as usize];
            d2.extend((0..1 << bits).map(|k| {
                let x = lhs.slice()[i as usize].to_f32();
                let val = k as f32 / ((1 << bits) - 1) as f32;
                let y = bas + val * del;
                let d = x - y;
                d * d
            }));
        }
        d2
    }
}

macro_rules! unimpl_operator_scalar_quantization {
    ($t:ty) => {
        impl OperatorScalarQuantization for $t {
            type Scalar = Impossible;
            fn get(_: Borrowed<'_, Self>, _: u32) -> Self::Scalar {
                unimplemented!()
            }
            fn scalar_quantization_preprocess(
                _: u32,
                _: u32,
                _: &[f32],
                _: &[f32],
                _: Borrowed<'_, Self>,
            ) -> Self::QuantizationPreprocessed {
                unimplemented!()
            }
        }
    };
}

unimpl_operator_scalar_quantization!(BVectorDot);
unimpl_operator_scalar_quantization!(BVectorHamming);
unimpl_operator_scalar_quantization!(BVectorJaccard);

unimpl_operator_scalar_quantization!(SVectDot<f32>);
unimpl_operator_scalar_quantization!(SVectL2<f32>);
