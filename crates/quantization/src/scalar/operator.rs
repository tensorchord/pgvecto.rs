use base::operator::*;
use base::scalar::*;
use num_traits::{Float, Zero};

pub trait OperatorScalarQuantization: Operator {
    type ScalarQuantizationPreprocessed;

    fn scalar_quantization_preprocess(
        dims: u32,
        bits: u32,
        max: &[Scalar<Self>],
        min: &[Scalar<Self>],
        lhs: Borrowed<'_, Self>,
    ) -> Self::ScalarQuantizationPreprocessed;

    fn scalar_quantization_process(
        dims: u32,
        bits: u32,
        preprocessed: &Self::ScalarQuantizationPreprocessed,
        rhs: &[u8],
    ) -> F32;
}

impl OperatorScalarQuantization for Vecf32Cos {
    type ScalarQuantizationPreprocessed = (Vec<[F32; 256]>, F32, Vec<[F32; 256]>);

    fn scalar_quantization_preprocess(
        dims: u32,
        bits: u32,
        max: &[Scalar<Self>],
        min: &[Scalar<Self>],
        lhs: Borrowed<'_, Self>,
    ) -> Self::ScalarQuantizationPreprocessed {
        let mut xy = Vec::with_capacity(dims as _);
        let mut x2 = F32(0.0);
        let mut y2 = Vec::with_capacity(dims as _);
        for i in 0..dims {
            let x = lhs.slice()[i as usize];
            xy.push(std::array::from_fn(|k| {
                let y = F32(k as f32) / F32((1 << bits) as f32)
                    * (max[i as usize] - min[i as usize])
                    + min[i as usize];
                x * y
            }));
            x2 += x * x;
            y2.push(std::array::from_fn(|k| {
                let y = F32(k as f32) / F32((1 << bits) as f32)
                    * (max[i as usize] - min[i as usize])
                    + min[i as usize];
                y * y
            }));
        }
        (xy, x2, y2)
    }

    fn scalar_quantization_process(
        _dims: u32,
        _bits: u32,
        preprocessed: &Self::ScalarQuantizationPreprocessed,
        rhs: &[u8],
    ) -> F32 {
        assert_eq!(preprocessed.0.len(), rhs.len());
        assert_eq!(preprocessed.2.len(), rhs.len());
        let n = rhs.len();
        let mut xy = F32::zero();
        let x2 = preprocessed.1;
        let mut y2 = F32::zero();
        for i in 0..n {
            xy += preprocessed.0[i][rhs[i] as usize];
            y2 += preprocessed.2[i][rhs[i] as usize];
        }
        F32(1.0) - xy / (x2 * y2).sqrt()
    }
}

impl OperatorScalarQuantization for Vecf32Dot {
    type ScalarQuantizationPreprocessed = Vec<[F32; 256]>;

    fn scalar_quantization_preprocess(
        dims: u32,
        bits: u32,
        max: &[Scalar<Self>],
        min: &[Scalar<Self>],
        lhs: Borrowed<'_, Self>,
    ) -> Self::ScalarQuantizationPreprocessed {
        let mut xy = Vec::with_capacity(dims as _);
        for i in 0..dims {
            xy.push(std::array::from_fn(|k| {
                let x = lhs.slice()[i as usize];
                let y = F32(k as f32) / F32((1 << bits) as f32)
                    * (max[i as usize] - min[i as usize])
                    + min[i as usize];
                x * y
            }));
        }
        xy
    }

    fn scalar_quantization_process(
        _dims: u32,
        _bits: u32,
        preprocessed: &Self::ScalarQuantizationPreprocessed,
        rhs: &[u8],
    ) -> F32 {
        assert_eq!(preprocessed.len(), rhs.len());
        let n = rhs.len();
        let mut xy = F32::zero();
        for i in 0..n {
            xy += preprocessed[i][rhs[i] as usize];
        }
        F32(0.0) - xy
    }
}

impl OperatorScalarQuantization for Vecf32L2 {
    type ScalarQuantizationPreprocessed = Vec<[F32; 256]>;

    fn scalar_quantization_preprocess(
        dims: u32,
        bits: u32,
        max: &[Scalar<Self>],
        min: &[Scalar<Self>],
        lhs: Borrowed<'_, Self>,
    ) -> Self::ScalarQuantizationPreprocessed {
        let mut d2 = Vec::with_capacity(dims as _);
        for i in 0..dims {
            d2.push(std::array::from_fn(|k| {
                let x = lhs.slice()[i as usize];
                let y = F32(k as f32) / F32((1 << bits) as f32)
                    * (max[i as usize] - min[i as usize])
                    + min[i as usize];
                let d = x - y;
                d * d
            }));
        }
        d2
    }

    fn scalar_quantization_process(
        _dims: u32,
        _bits: u32,
        preprocessed: &Self::ScalarQuantizationPreprocessed,
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

impl OperatorScalarQuantization for Vecf16Cos {
    type ScalarQuantizationPreprocessed = (Vec<[F32; 256]>, F32, Vec<[F32; 256]>);

    fn scalar_quantization_preprocess(
        dims: u32,
        bits: u32,
        max: &[Scalar<Self>],
        min: &[Scalar<Self>],
        lhs: Borrowed<'_, Self>,
    ) -> Self::ScalarQuantizationPreprocessed {
        let mut xy = Vec::with_capacity(dims as _);
        let mut x2 = F32(0.0);
        let mut y2 = Vec::with_capacity(dims as _);
        for i in 0..dims {
            let x = lhs.slice()[i as usize].to_f();
            xy.push(std::array::from_fn(|k| {
                let y = F32(k as f32) / F32((1 << bits) as f32)
                    * (max[i as usize].to_f() - min[i as usize].to_f())
                    + min[i as usize].to_f();
                x * y
            }));
            x2 += x * x;
            y2.push(std::array::from_fn(|k| {
                let y = F32(k as f32) / F32((1 << bits) as f32)
                    * (max[i as usize].to_f() - min[i as usize].to_f())
                    + min[i as usize].to_f();
                y * y
            }));
        }
        (xy, x2, y2)
    }

    fn scalar_quantization_process(
        _dims: u32,
        _bits: u32,
        preprocessed: &Self::ScalarQuantizationPreprocessed,
        rhs: &[u8],
    ) -> F32 {
        assert_eq!(preprocessed.0.len(), rhs.len());
        assert_eq!(preprocessed.2.len(), rhs.len());
        let n = rhs.len();
        let mut xy = F32::zero();
        let x2 = preprocessed.1;
        let mut y2 = F32::zero();
        for i in 0..n {
            xy += preprocessed.0[i][rhs[i] as usize];
            y2 += preprocessed.2[i][rhs[i] as usize];
        }
        F32(1.0) - xy / (x2 * y2).sqrt()
    }
}

impl OperatorScalarQuantization for Vecf16Dot {
    type ScalarQuantizationPreprocessed = Vec<[F32; 256]>;

    fn scalar_quantization_preprocess(
        dims: u32,
        bits: u32,
        max: &[Scalar<Self>],
        min: &[Scalar<Self>],
        lhs: Borrowed<'_, Self>,
    ) -> Self::ScalarQuantizationPreprocessed {
        let mut xy = Vec::with_capacity(dims as _);
        for i in 0..dims {
            xy.push(std::array::from_fn(|k| {
                let x = lhs.slice()[i as usize].to_f();
                let y = F32(k as f32) / F32((1 << bits) as f32)
                    * (max[i as usize].to_f() - min[i as usize].to_f())
                    + min[i as usize].to_f();
                x * y
            }));
        }
        xy
    }

    fn scalar_quantization_process(
        _dims: u32,
        _bits: u32,
        preprocessed: &Self::ScalarQuantizationPreprocessed,
        rhs: &[u8],
    ) -> F32 {
        assert_eq!(preprocessed.len(), rhs.len());
        let n = rhs.len();
        let mut xy = F32::zero();
        for i in 0..n {
            xy += preprocessed[i][rhs[i] as usize];
        }
        F32(0.0) - xy
    }
}

impl OperatorScalarQuantization for Vecf16L2 {
    type ScalarQuantizationPreprocessed = Vec<[F32; 256]>;

    fn scalar_quantization_preprocess(
        dims: u32,
        bits: u32,
        max: &[Scalar<Self>],
        min: &[Scalar<Self>],
        lhs: Borrowed<'_, Self>,
    ) -> Self::ScalarQuantizationPreprocessed {
        let mut d2 = Vec::with_capacity(dims as _);
        for i in 0..dims {
            d2.push(std::array::from_fn(|k| {
                let x = lhs.slice()[i as usize].to_f();
                let y = F32(k as f32) / F32((1 << bits) as f32)
                    * (max[i as usize].to_f() - min[i as usize].to_f())
                    + min[i as usize].to_f();
                let d = x - y;
                d * d
            }));
        }
        d2
    }

    fn scalar_quantization_process(
        _dims: u32,
        _bits: u32,
        preprocessed: &Self::ScalarQuantizationPreprocessed,
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

macro_rules! unimpl_operator_scalar_quantization {
    ($t:ty, $l:ty) => {
        impl OperatorScalarQuantization for $t {
            type ScalarQuantizationPreprocessed = std::convert::Infallible;

            fn scalar_quantization_preprocess(
                _: u32,
                _: u32,
                _: &[Scalar<Self>],
                _: &[Scalar<Self>],
                _: Borrowed<'_, Self>,
            ) -> Self::ScalarQuantizationPreprocessed {
                unimplemented!()
            }

            fn scalar_quantization_process(
                _: u32,
                _: u32,
                processed: &Self::ScalarQuantizationPreprocessed,
                _: &[u8],
            ) -> F32 {
                match *processed {}
            }
        }
    };
}

unimpl_operator_scalar_quantization!(BVecf32Cos, BVecf32L2);
unimpl_operator_scalar_quantization!(BVecf32Dot, BVecf32L2);
unimpl_operator_scalar_quantization!(BVecf32L2, BVecf32L2);
unimpl_operator_scalar_quantization!(BVecf32Jaccard, BVecf32L2);

unimpl_operator_scalar_quantization!(SVecf32Cos, SVecf32L2);
unimpl_operator_scalar_quantization!(SVecf32Dot, SVecf32L2);
unimpl_operator_scalar_quantization!(SVecf32L2, SVecf32L2);

unimpl_operator_scalar_quantization!(Veci8Cos, Veci8L2);
unimpl_operator_scalar_quantization!(Veci8Dot, Veci8L2);
unimpl_operator_scalar_quantization!(Veci8L2, Veci8L2);
