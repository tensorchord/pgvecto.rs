use crate::product::operator::OperatorProductQuantization;
use crate::rabitq::operator::OperatorRaBitQ;
use crate::scalar::operator::OperatorScalarQuantization;
use crate::trivial::operator::OperatorTrivialQuantization;
use base::operator::*;
use base::scalar::F32;
use num_traits::Zero;

pub trait OperatorQuantizationProcess: Operator {
    type QuantizationPreprocessed;

    fn quantization_process(
        dims: u32,
        ratio: u32,
        bits: u32,
        preprocessed: &Self::QuantizationPreprocessed,
        rhs: impl Fn(usize) -> usize,
    ) -> F32;

    const SUPPORT_FAST_SCAN: bool;
    fn fast_scan(preprocessed: &Self::QuantizationPreprocessed) -> Vec<F32>;
    fn fast_scan_resolve(x: F32) -> F32;
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
            ) -> F32 {
                match *preprocessed {}
            }

            const SUPPORT_FAST_SCAN: bool = false;

            fn fast_scan(preprocessed: &Self::QuantizationPreprocessed) -> Vec<F32> {
                match *preprocessed {}
            }

            fn fast_scan_resolve(_: F32) -> F32 {
                unimplemented!()
            }
        }
    };
}

impl OperatorQuantizationProcess for Vecf32Dot {
    type QuantizationPreprocessed = Vec<F32>;

    fn quantization_process(
        dims: u32,
        ratio: u32,
        bits: u32,
        preprocessed: &Self::QuantizationPreprocessed,
        rhs: impl Fn(usize) -> usize,
    ) -> F32 {
        let width = dims.div_ceil(ratio);
        let xy = {
            let mut xy = F32::zero();
            for i in 0..width as usize {
                xy += preprocessed[i * (1 << bits) + rhs(i)];
            }
            xy
        };
        F32(0.0) - xy
    }

    const SUPPORT_FAST_SCAN: bool = true;

    fn fast_scan(preprocessed: &Self::QuantizationPreprocessed) -> Vec<F32> {
        preprocessed.clone()
    }

    fn fast_scan_resolve(x: F32) -> F32 {
        x * F32(-1.0)
    }
}

impl OperatorQuantizationProcess for Vecf32L2 {
    type QuantizationPreprocessed = Vec<F32>;

    fn quantization_process(
        dims: u32,
        ratio: u32,
        bits: u32,
        preprocessed: &Self::QuantizationPreprocessed,
        rhs: impl Fn(usize) -> usize,
    ) -> F32 {
        let width = dims.div_ceil(ratio);
        let mut d2 = F32::zero();
        for i in 0..width as usize {
            d2 += preprocessed[i * (1 << bits) + rhs(i)];
        }
        d2
    }

    const SUPPORT_FAST_SCAN: bool = true;

    fn fast_scan(preprocessed: &Self::QuantizationPreprocessed) -> Vec<F32> {
        preprocessed.clone()
    }

    fn fast_scan_resolve(x: F32) -> F32 {
        x
    }
}

impl OperatorQuantizationProcess for Vecf16Dot {
    type QuantizationPreprocessed = Vec<F32>;

    fn quantization_process(
        dims: u32,
        ratio: u32,
        bits: u32,
        preprocessed: &Self::QuantizationPreprocessed,
        rhs: impl Fn(usize) -> usize,
    ) -> F32 {
        let width = dims.div_ceil(ratio);
        let xy = {
            let mut xy = F32::zero();
            for i in 0..width as usize {
                xy += preprocessed[i * (1 << bits) + rhs(i)];
            }
            xy
        };
        F32(0.0) - xy
    }

    const SUPPORT_FAST_SCAN: bool = true;

    fn fast_scan(preprocessed: &Self::QuantizationPreprocessed) -> Vec<F32> {
        preprocessed.clone()
    }

    fn fast_scan_resolve(x: F32) -> F32 {
        x * F32(-1.0)
    }
}

impl OperatorQuantizationProcess for Vecf16L2 {
    type QuantizationPreprocessed = Vec<F32>;

    fn quantization_process(
        dims: u32,
        ratio: u32,
        bits: u32,
        preprocessed: &Self::QuantizationPreprocessed,
        rhs: impl Fn(usize) -> usize,
    ) -> F32 {
        let width = dims.div_ceil(ratio);
        let mut d2 = F32::zero();
        for i in 0..width as usize {
            d2 += preprocessed[i * (1 << bits) + rhs(i)];
        }
        d2
    }

    const SUPPORT_FAST_SCAN: bool = true;

    fn fast_scan(preprocessed: &Self::QuantizationPreprocessed) -> Vec<F32> {
        preprocessed.clone()
    }

    fn fast_scan_resolve(x: F32) -> F32 {
        x
    }
}

unimpl_operator_quantization_process!(BVectorDot);
unimpl_operator_quantization_process!(BVectorHamming);
unimpl_operator_quantization_process!(BVectorJaccard);

unimpl_operator_quantization_process!(SVecf32Dot);
unimpl_operator_quantization_process!(SVecf32L2);

pub trait OperatorQuantization:
    OperatorQuantizationProcess
    + OperatorTrivialQuantization
    + OperatorScalarQuantization
    + OperatorProductQuantization
    + OperatorRaBitQ
{
}

impl OperatorQuantization for BVectorDot {}
impl OperatorQuantization for BVectorJaccard {}
impl OperatorQuantization for BVectorHamming {}
impl OperatorQuantization for SVecf32Dot {}
impl OperatorQuantization for SVecf32L2 {}
impl OperatorQuantization for Vecf16Dot {}
impl OperatorQuantization for Vecf16L2 {}
impl OperatorQuantization for Vecf32Dot {}
impl OperatorQuantization for Vecf32L2 {}
