use crate::operator::OperatorQuantizationProcess;
use base::operator::*;
use base::scalar::*;

pub trait OperatorScalarQuantization: Operator + OperatorQuantizationProcess {
    fn scalar_quantization_preprocess(
        dims: u32,
        bits: u32,
        max: &[Scalar<Self>],
        min: &[Scalar<Self>],
        lhs: Borrowed<'_, Self>,
    ) -> Self::QuantizationPreprocessed;
}

impl OperatorScalarQuantization for Vecf32Dot {
    fn scalar_quantization_preprocess(
        dims: u32,
        bits: u32,
        max: &[Scalar<Self>],
        min: &[Scalar<Self>],
        lhs: Borrowed<'_, Self>,
    ) -> Self::QuantizationPreprocessed {
        let mut xy = Vec::with_capacity(dims as _);
        for i in 0..dims {
            let bas = min[i as usize];
            let del = max[i as usize] - min[i as usize];
            xy.extend((0..1 << bits).map(|k| {
                let x = lhs.slice()[i as usize];
                let val = Scalar::<Self>::from_f(F32(k as f32 / ((1 << bits) - 1) as f32));
                let y = bas + val * del;
                x * y
            }));
        }
        xy
    }
}

impl OperatorScalarQuantization for Vecf32L2 {
    fn scalar_quantization_preprocess(
        dims: u32,
        bits: u32,
        max: &[Scalar<Self>],
        min: &[Scalar<Self>],
        lhs: Borrowed<'_, Self>,
    ) -> Self::QuantizationPreprocessed {
        let mut d2 = Vec::with_capacity(dims as _);
        for i in 0..dims {
            let bas = min[i as usize];
            let del = max[i as usize] - min[i as usize];
            d2.extend((0..1 << bits).map(|k| {
                let x = lhs.slice()[i as usize];
                let val = Scalar::<Self>::from_f(F32(k as f32 / ((1 << bits) - 1) as f32));
                let y = bas + val * del;
                let d = x - y;
                d * d
            }));
        }
        d2
    }
}

impl OperatorScalarQuantization for Vecf16Dot {
    fn scalar_quantization_preprocess(
        dims: u32,
        bits: u32,
        max: &[Scalar<Self>],
        min: &[Scalar<Self>],
        lhs: Borrowed<'_, Self>,
    ) -> Self::QuantizationPreprocessed {
        let mut xy = Vec::with_capacity(dims as _);
        for i in 0..dims {
            let bas = min[i as usize];
            let del = max[i as usize] - min[i as usize];
            xy.extend((0..1 << bits).map(|k| {
                let x = lhs.slice()[i as usize].to_f();
                let val = Scalar::<Self>::from_f(F32(k as f32 / ((1 << bits) - 1) as f32));
                let y = (bas + val * del).to_f32();
                x * y
            }));
        }
        xy
    }
}

impl OperatorScalarQuantization for Vecf16L2 {
    fn scalar_quantization_preprocess(
        dims: u32,
        bits: u32,
        max: &[Scalar<Self>],
        min: &[Scalar<Self>],
        lhs: Borrowed<'_, Self>,
    ) -> Self::QuantizationPreprocessed {
        let mut d2 = Vec::with_capacity(dims as _);
        for i in 0..dims {
            let bas = min[i as usize];
            let del = max[i as usize] - min[i as usize];
            d2.extend((0..1 << bits).map(|k| {
                let x = lhs.slice()[i as usize].to_f();
                let val = Scalar::<Self>::from_f(F32(k as f32 / ((1 << bits) - 1) as f32));
                let y = (bas + val * del).to_f32();
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
            fn scalar_quantization_preprocess(
                _: u32,
                _: u32,
                _: &[Scalar<Self>],
                _: &[Scalar<Self>],
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

unimpl_operator_scalar_quantization!(SVecf32Dot);
unimpl_operator_scalar_quantization!(SVecf32L2);
