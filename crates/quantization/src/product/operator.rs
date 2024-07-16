use base::operator::*;
use base::scalar::*;
use num_traits::{Float, Zero};

pub trait OperatorProductQuantization: Operator {
    type ProductQuantizationPreprocessed;

    fn product_quantization_preprocess(
        dims: u32,
        ratio: u32,
        bits: u32,
        centroids: &[Scalar<Self>],
        lhs: Borrowed<'_, Self>,
    ) -> Self::ProductQuantizationPreprocessed;

    fn product_quantization_process(
        dims: u32,
        ratio: u32,
        bits: u32,
        preprocessed: &Self::ProductQuantizationPreprocessed,
        rhs: &[u8],
    ) -> F32;
}

impl OperatorProductQuantization for Vecf32Cos {
    type ProductQuantizationPreprocessed = (Vec<[F32; 256]>, F32, Vec<[F32; 256]>);

    fn product_quantization_preprocess(
        dims: u32,
        ratio: u32,
        _bits: u32,
        centroids: &[Scalar<Self>],
        lhs: Borrowed<'_, Self>,
    ) -> Self::ProductQuantizationPreprocessed {
        let mut xy = Vec::with_capacity(dims.div_ceil(ratio) as _);
        let mut x2 = F32(0.0);
        let mut y2 = Vec::with_capacity(dims.div_ceil(ratio) as _);
        for p in 0..dims.div_ceil(ratio) {
            let w = (dims - ratio * p).min(ratio);
            xy.push(std::array::from_fn(|k| {
                let mut xy = F32(0.0);
                for i in ratio * p..ratio * p + w {
                    let x = lhs.slice()[i as usize];
                    let y = centroids[(k as u32 * dims + i) as usize];
                    xy += x * y;
                }
                xy
            }));
            x2 += {
                let mut x2 = F32(0.0);
                for i in ratio * p..ratio * p + w {
                    let x = lhs.slice()[i as usize];
                    x2 += x * x;
                }
                x2
            };
            y2.push(std::array::from_fn(|k| {
                let mut y2 = F32(0.0);
                for i in ratio * p..ratio * p + w {
                    let y = centroids[(k as u32 * dims + i) as usize];
                    y2 += y * y;
                }
                y2
            }));
        }
        (xy, x2, y2)
    }

    fn product_quantization_process(
        _dims: u32,
        _ratio: u32,
        _bits: u32,
        preprocessed: &Self::ProductQuantizationPreprocessed,
        rhs: &[u8],
    ) -> F32 {
        assert_eq!(preprocessed.0.len(), rhs.len());
        assert_eq!(preprocessed.2.len(), rhs.len());
        let n = rhs.len();
        let xy = {
            let mut xy = F32::zero();
            for i in 0..n {
                xy += preprocessed.0[i][rhs[i] as usize];
            }
            xy
        };
        let x2 = preprocessed.1;
        let y2 = {
            let mut y2 = F32::zero();
            for i in 0..n {
                y2 += preprocessed.2[i][rhs[i] as usize];
            }
            y2
        };
        F32(1.0) - xy / (x2 * y2).sqrt()
    }
}

impl OperatorProductQuantization for Vecf32Dot {
    type ProductQuantizationPreprocessed = Vec<[F32; 256]>;

    fn product_quantization_preprocess(
        dims: u32,
        ratio: u32,
        _bits: u32,
        centroids: &[Scalar<Self>],
        lhs: Borrowed<'_, Self>,
    ) -> Self::ProductQuantizationPreprocessed {
        let mut xy = Vec::with_capacity(dims.div_ceil(ratio) as _);
        for p in 0..dims.div_ceil(ratio) {
            let w = (dims - ratio * p).min(ratio);
            xy.push(std::array::from_fn(|k| {
                let mut xy = F32(0.0);
                for i in ratio * p..ratio * p + w {
                    let x = lhs.slice()[i as usize];
                    let y = centroids[(k as u32 * dims + i) as usize];
                    xy += x * y;
                }
                xy
            }));
        }
        xy
    }

    fn product_quantization_process(
        _dims: u32,
        _ratio: u32,
        _bits: u32,
        preprocessed: &Self::ProductQuantizationPreprocessed,
        rhs: &[u8],
    ) -> F32 {
        assert_eq!(preprocessed.len(), rhs.len());
        let n = rhs.len();
        let xy = {
            let mut xy = F32::zero();
            for i in 0..n {
                xy += preprocessed[i][rhs[i] as usize];
            }
            xy
        };
        F32(0.0) - xy
    }
}

impl OperatorProductQuantization for Vecf32L2 {
    type ProductQuantizationPreprocessed = Vec<[F32; 256]>;

    fn product_quantization_preprocess(
        dims: u32,
        ratio: u32,
        _bits: u32,
        centroids: &[Scalar<Self>],
        lhs: Borrowed<'_, Self>,
    ) -> Self::ProductQuantizationPreprocessed {
        let mut d2 = Vec::with_capacity(dims.div_ceil(ratio) as _);
        for p in 0..dims.div_ceil(ratio) {
            let w = (dims - ratio * p).min(ratio);
            d2.push(std::array::from_fn(|k| {
                let mut d2 = F32(0.0);
                for i in ratio * p..ratio * p + w {
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

    fn product_quantization_process(
        _dims: u32,
        _ratio: u32,
        _bits: u32,
        preprocessed: &Self::ProductQuantizationPreprocessed,
        rhs: &[u8],
    ) -> F32 {
        assert_eq!(preprocessed.len(), rhs.len());
        let n = rhs.len();
        let mut d2 = F32::zero();
        for i in 0..n {
            d2 += preprocessed[i][rhs[i] as usize];
        }
        d2
    }
}

impl OperatorProductQuantization for Vecf16Cos {
    type ProductQuantizationPreprocessed = (Vec<[F32; 256]>, F32, Vec<[F32; 256]>);

    fn product_quantization_preprocess(
        dims: u32,
        ratio: u32,
        _bits: u32,
        centroids: &[Scalar<Self>],
        lhs: Borrowed<'_, Self>,
    ) -> Self::ProductQuantizationPreprocessed {
        let mut xy = Vec::with_capacity(dims.div_ceil(ratio) as _);
        let mut x2 = F32(0.0);
        let mut y2 = Vec::with_capacity(dims.div_ceil(ratio) as _);
        for p in 0..dims.div_ceil(ratio) {
            let w = (dims - ratio * p).min(ratio);
            xy.push(std::array::from_fn(|k| {
                let mut xy = F32(0.0);
                for i in ratio * p..ratio * p + w {
                    let x = lhs.slice()[i as usize].to_f();
                    let y = centroids[(k as u32 * dims + i) as usize].to_f();
                    xy += x * y;
                }
                xy
            }));
            x2 += {
                let mut x2 = F32(0.0);
                for i in ratio * p..ratio * p + w {
                    let x = lhs.slice()[i as usize].to_f();
                    x2 += x * x;
                }
                x2
            };
            y2.push(std::array::from_fn(|k| {
                let mut y2 = F32(0.0);
                for i in ratio * p..ratio * p + w {
                    let y = centroids[(k as u32 * dims + i) as usize].to_f();
                    y2 += y * y;
                }
                y2
            }));
        }
        (xy, x2, y2)
    }

    fn product_quantization_process(
        _dims: u32,
        _ratio: u32,
        _bits: u32,
        preprocessed: &Self::ProductQuantizationPreprocessed,
        rhs: &[u8],
    ) -> F32 {
        assert_eq!(preprocessed.0.len(), rhs.len());
        assert_eq!(preprocessed.2.len(), rhs.len());
        let n = rhs.len();
        let xy = {
            let mut xy = F32::zero();
            for i in 0..n {
                xy += preprocessed.0[i][rhs[i] as usize];
            }
            xy
        };
        let x2 = preprocessed.1;
        let y2 = {
            let mut y2 = F32::zero();
            for i in 0..n {
                y2 += preprocessed.2[i][rhs[i] as usize];
            }
            y2
        };
        F32(1.0) - xy / (x2 * y2).sqrt()
    }
}

impl OperatorProductQuantization for Vecf16Dot {
    type ProductQuantizationPreprocessed = Vec<[F32; 256]>;

    fn product_quantization_preprocess(
        dims: u32,
        ratio: u32,
        _bits: u32,
        centroids: &[Scalar<Self>],
        lhs: Borrowed<'_, Self>,
    ) -> Self::ProductQuantizationPreprocessed {
        let mut xy = Vec::with_capacity(dims.div_ceil(ratio) as _);
        for p in 0..dims.div_ceil(ratio) {
            let w = (dims - ratio * p).min(ratio);
            xy.push(std::array::from_fn(|k| {
                let mut xy = F32(0.0);
                for i in ratio * p..ratio * p + w {
                    let x = lhs.slice()[i as usize].to_f();
                    let y = centroids[(k as u32 * dims + i) as usize].to_f();
                    xy += x * y;
                }
                xy
            }));
        }
        xy
    }

    fn product_quantization_process(
        _dims: u32,
        _ratio: u32,
        _bits: u32,
        preprocessed: &Self::ProductQuantizationPreprocessed,
        rhs: &[u8],
    ) -> F32 {
        assert_eq!(preprocessed.len(), rhs.len());
        let n = rhs.len();
        let xy = {
            let mut xy = F32::zero();
            for i in 0..n {
                xy += preprocessed[i][rhs[i] as usize];
            }
            xy
        };
        F32(0.0) - xy
    }
}

impl OperatorProductQuantization for Vecf16L2 {
    type ProductQuantizationPreprocessed = Vec<[F32; 256]>;

    fn product_quantization_preprocess(
        dims: u32,
        ratio: u32,
        _bits: u32,
        centroids: &[Scalar<Self>],
        lhs: Borrowed<'_, Self>,
    ) -> Self::ProductQuantizationPreprocessed {
        let mut d2 = Vec::with_capacity(dims.div_ceil(ratio) as _);
        for p in 0..dims.div_ceil(ratio) {
            let w = (dims - ratio * p).min(ratio);
            d2.push(std::array::from_fn(|k| {
                let mut d2 = F32(0.0);
                for i in ratio * p..ratio * p + w {
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

    fn product_quantization_process(
        _dims: u32,
        _ratio: u32,
        _bits: u32,
        preprocessed: &Self::ProductQuantizationPreprocessed,
        rhs: &[u8],
    ) -> F32 {
        assert_eq!(preprocessed.len(), rhs.len());
        let n = rhs.len();
        let mut d2 = F32::zero();
        for i in 0..n {
            d2 += preprocessed[i][rhs[i] as usize];
        }
        d2
    }
}

macro_rules! unimpl_operator_product_quantization {
    ($t:ty, $l:ty) => {
        impl OperatorProductQuantization for $t {
            type ProductQuantizationPreprocessed = std::convert::Infallible;

            fn product_quantization_preprocess(
                _: u32,
                _: u32,
                _: u32,
                _: &[Scalar<Self>],
                _: Borrowed<'_, Self>,
            ) -> Self::ProductQuantizationPreprocessed {
                unimplemented!()
            }

            fn product_quantization_process(
                _: u32,
                _: u32,
                _: u32,
                preprocessed: &Self::ProductQuantizationPreprocessed,
                _: &[u8],
            ) -> F32 {
                match *preprocessed {}
            }
        }
    };
}

unimpl_operator_product_quantization!(BVecf32Cos, BVecf32L2);
unimpl_operator_product_quantization!(BVecf32Dot, BVecf32L2);
unimpl_operator_product_quantization!(BVecf32L2, BVecf32L2);
unimpl_operator_product_quantization!(BVecf32Jaccard, BVecf32L2);

unimpl_operator_product_quantization!(SVecf32Cos, SVecf32L2);
unimpl_operator_product_quantization!(SVecf32Dot, SVecf32L2);
unimpl_operator_product_quantization!(SVecf32L2, SVecf32L2);

unimpl_operator_product_quantization!(Veci8Cos, Veci8L2);
unimpl_operator_product_quantization!(Veci8Dot, Veci8L2);
unimpl_operator_product_quantization!(Veci8L2, Veci8L2);
