use crate::operator::OperatorQuantizationProcess;
use base::scalar::impossible::Impossible;
use base::{operator::*, scalar::ScalarLike};

pub trait OperatorProductQuantization: OperatorQuantizationProcess {
    type Scalar: ScalarLike;
    fn subslice(vector: Borrowed<'_, Self>, start: u32, len: u32) -> &[Self::Scalar];
    fn product_quantization_preprocess(
        dims: u32,
        ratio: u32,
        bits: u32,
        centroids: &[Self::Scalar],
        lhs: Borrowed<'_, Self>,
    ) -> Self::QuantizationPreprocessed;
}

impl<S: ScalarLike> OperatorProductQuantization for VectDot<S> {
    type Scalar = S;
    fn subslice(vector: Borrowed<'_, Self>, start: u32, len: u32) -> &[Self::Scalar] {
        &vector.slice()[start as usize..][..len as usize]
    }
    fn product_quantization_preprocess(
        dims: u32,
        ratio: u32,
        bits: u32,
        centroids: &[Self::Scalar],
        lhs: Borrowed<'_, Self>,
    ) -> Self::QuantizationPreprocessed {
        let mut xy = Vec::with_capacity((dims.div_ceil(ratio) as usize) * (1 << bits));
        for p in 0..dims.div_ceil(ratio) {
            let subdims = std::cmp::min(ratio, dims - ratio * p);
            xy.extend((0_usize..1 << bits).map(|k| {
                let mut xy = 0.0f32;
                for i in ratio * p..ratio * p + subdims {
                    let x = lhs.slice()[i as usize].to_f32();
                    let y = centroids[(k as u32 * dims + i) as usize].to_f32();
                    xy += x * y;
                }
                xy
            }));
        }
        xy
    }
}

impl<S: ScalarLike> OperatorProductQuantization for VectL2<S> {
    type Scalar = S;
    fn subslice(vector: Borrowed<'_, Self>, start: u32, len: u32) -> &[Self::Scalar] {
        &vector.slice()[start as usize..][..len as usize]
    }
    fn product_quantization_preprocess(
        dims: u32,
        ratio: u32,
        bits: u32,
        centroids: &[Self::Scalar],
        lhs: Borrowed<'_, Self>,
    ) -> Self::QuantizationPreprocessed {
        let mut d2 = Vec::with_capacity((dims.div_ceil(ratio) as usize) * (1 << bits));
        for p in 0..dims.div_ceil(ratio) {
            let subdims = std::cmp::min(ratio, dims - ratio * p);
            d2.extend((0_usize..1 << bits).map(|k| {
                let mut d2 = 0.0f32;
                for i in ratio * p..ratio * p + subdims {
                    let x = lhs.slice()[i as usize].to_f32();
                    let y = centroids[(k as u32 * dims + i) as usize].to_f32();
                    let d = x - y;
                    d2 += d * d;
                }
                d2
            }));
        }
        d2
    }
}

macro_rules! unimpl_operator_product_quantization {
    ($t:ty) => {
        impl OperatorProductQuantization for $t {
            type Scalar = Impossible;
            fn subslice(_: Borrowed<'_, Self>, _: u32, _: u32) -> &[Self::Scalar] {
                unimplemented!()
            }
            fn product_quantization_preprocess(
                _: u32,
                _: u32,
                _: u32,
                _: &[Self::Scalar],
                _: Borrowed<'_, Self>,
            ) -> Self::QuantizationPreprocessed {
                unimplemented!()
            }
        }
    };
}

unimpl_operator_product_quantization!(BVectorDot);
unimpl_operator_product_quantization!(BVectorHamming);
unimpl_operator_product_quantization!(BVectorJaccard);

unimpl_operator_product_quantization!(SVectDot<f32>);
unimpl_operator_product_quantization!(SVectL2<f32>);
