use crate::operator::OperatorQuantizationProcess;
use base::operator::*;
use base::scalar::*;

pub trait OperatorProductQuantization: OperatorQuantizationProcess {
    fn product_quantization_preprocess(
        dims: u32,
        ratio: u32,
        bits: u32,
        centroids: &[Scalar<Self>],
        lhs: Borrowed<'_, Self>,
    ) -> Self::QuantizationPreprocessed;
}

impl OperatorProductQuantization for Vecf32Dot {
    fn product_quantization_preprocess(
        dims: u32,
        ratio: u32,
        bits: u32,
        centroids: &[Scalar<Self>],
        lhs: Borrowed<'_, Self>,
    ) -> Self::QuantizationPreprocessed {
        let mut xy = Vec::with_capacity((dims.div_ceil(ratio) as usize) * (1 << bits));
        for p in 0..dims.div_ceil(ratio) {
            let subdims = std::cmp::min(ratio, dims - ratio * p);
            xy.extend((0_usize..1 << bits).map(|k| {
                let mut xy = F32(0.0);
                for i in ratio * p..ratio * p + subdims {
                    let x = lhs.slice()[i as usize];
                    let y = centroids[(k as u32 * dims + i) as usize];
                    xy += x * y;
                }
                xy
            }));
        }
        xy
    }
}

impl OperatorProductQuantization for Vecf32L2 {
    fn product_quantization_preprocess(
        dims: u32,
        ratio: u32,
        bits: u32,
        centroids: &[Scalar<Self>],
        lhs: Borrowed<'_, Self>,
    ) -> Self::QuantizationPreprocessed {
        let mut d2 = Vec::with_capacity((dims.div_ceil(ratio) as usize) * (1 << bits));
        for p in 0..dims.div_ceil(ratio) {
            let subdims = std::cmp::min(ratio, dims - ratio * p);
            d2.extend((0_usize..1 << bits).map(|k| {
                let mut d2 = F32(0.0);
                for i in ratio * p..ratio * p + subdims {
                    let x = lhs.slice()[i as usize];
                    let y = centroids[(k as u32 * dims + i) as usize];
                    let d = x - y;
                    d2 += d * d;
                }
                d2
            }));
        }
        d2
    }
}

impl OperatorProductQuantization for Vecf16Dot {
    fn product_quantization_preprocess(
        dims: u32,
        ratio: u32,
        bits: u32,
        centroids: &[Scalar<Self>],
        lhs: Borrowed<'_, Self>,
    ) -> Self::QuantizationPreprocessed {
        let mut xy = Vec::with_capacity((dims.div_ceil(ratio) as usize) * (1 << bits));
        for p in 0..dims.div_ceil(ratio) {
            let subdims = std::cmp::min(ratio, dims - ratio * p);
            xy.extend((0_usize..1 << bits).map(|k| {
                let mut xy = F32(0.0);
                for i in ratio * p..ratio * p + subdims {
                    let x = lhs.slice()[i as usize].to_f();
                    let y = centroids[(k as u32 * dims + i) as usize].to_f();
                    xy += x * y;
                }
                xy
            }));
        }
        xy
    }
}

impl OperatorProductQuantization for Vecf16L2 {
    fn product_quantization_preprocess(
        dims: u32,
        ratio: u32,
        bits: u32,
        centroids: &[Scalar<Self>],
        lhs: Borrowed<'_, Self>,
    ) -> Self::QuantizationPreprocessed {
        let mut d2 = Vec::with_capacity((dims.div_ceil(ratio) as usize) * (1 << bits));
        for p in 0..dims.div_ceil(ratio) {
            let subdims = std::cmp::min(ratio, dims - ratio * p);
            d2.extend((0_usize..1 << bits).map(|k| {
                let mut d2 = F32(0.0);
                for i in ratio * p..ratio * p + subdims {
                    let x = lhs.slice()[i as usize].to_f();
                    let y = centroids[(k as u32 * dims + i) as usize].to_f();
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
            fn product_quantization_preprocess(
                _: u32,
                _: u32,
                _: u32,
                _: &[Scalar<Self>],
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

unimpl_operator_product_quantization!(SVecf32Dot);
unimpl_operator_product_quantization!(SVecf32L2);
