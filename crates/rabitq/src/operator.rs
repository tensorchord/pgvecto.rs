use base::operator::Borrowed;
use base::operator::*;
use base::scalar::ScalarLike;
use base::search::Vectors;
use common::vec2::Vec2;
use half::f16;
use storage::OperatorStorage;

pub trait OperatorRabitq: OperatorStorage {
    const RESIDUAL: bool;
    fn sample(vectors: &impl Vectors<Self::Vector>) -> Vec2<f32>;
    fn cast(vector: Borrowed<'_, Self>) -> &[f32];
    fn residual(lhs: &[f32], rhs: &[f32]) -> Vec<f32>;

    fn proj(projection: &[Vec<f32>], vector: &[f32]) -> Vec<f32>;

    type QuantizationPreprocessed0;
    type QuantizationPreprocessed1;

    fn rabitq_quantization_preprocess(
        vector: &[f32],
    ) -> (
        Self::QuantizationPreprocessed0,
        Self::QuantizationPreprocessed1,
    );
    fn rabitq_quantization_process(
        dis_u_2: f32,
        factor_ppc: f32,
        factor_ip: f32,
        factor_err: f32,
        code: &[u8],
        p0: &Self::QuantizationPreprocessed0,
        p1: &Self::QuantizationPreprocessed1,
    ) -> (f32, f32);
    fn rabitq_quantization_process_1(
        dis_u_2: f32,
        factor_ppc: f32,
        factor_ip: f32,
        factor_err: f32,
        p0: &Self::QuantizationPreprocessed0,
        param: u16,
    ) -> (f32, f32);

    const SUPPORT_FAST_SCAN: bool;
    fn fast_scan(preprocessed: &Self::QuantizationPreprocessed1) -> Vec<u8>;
    fn fast_scan_resolve(x: f32) -> f32;
}

impl OperatorRabitq for VectL2<f32> {
    const RESIDUAL: bool = false;
    fn sample(vectors: &impl Vectors<Self::Vector>) -> Vec2<f32> {
        common::sample::sample(vectors.len(), vectors.dims(), |i| vectors.vector(i).slice())
    }
    fn cast(vector: Borrowed<'_, Self>) -> &[f32] {
        vector.slice()
    }
    fn residual(lhs: &[f32], rhs: &[f32]) -> Vec<f32> {
        f32::vector_sub(lhs, rhs)
    }

    type QuantizationPreprocessed0 = (f32, f32, f32, f32);
    type QuantizationPreprocessed1 = ((Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>), Vec<u8>);

    fn rabitq_quantization_preprocess(
        vector: &[f32],
    ) -> (
        (f32, f32, f32, f32),
        ((Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>), Vec<u8>),
    ) {
        let dis_v_2 = vector.iter().map(|&x| x * x).sum();
        let (k, b, qvector) = quantization::quantize::quantize::<15>(vector);
        let qvector_sum = qvector.iter().fold(0_u32, |x, &y| x + y as u32) as f32;
        let blut = binarize(&qvector);
        let lut = gen(&qvector);
        ((dis_v_2, b, k, qvector_sum), (blut, lut))
    }

    fn rabitq_quantization_process(
        dis_u_2: f32,
        factor_ppc: f32,
        factor_ip: f32,
        factor_err: f32,
        code: &[u8],
        p0: &(f32, f32, f32, f32),
        p1: &((Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>), Vec<u8>),
    ) -> (f32, f32) {
        rabitq_quantization_process(dis_u_2, factor_ppc, factor_ip, factor_err, code, *p0, p1)
    }

    fn rabitq_quantization_process_1(
        dis_u_2: f32,
        factor_ppc: f32,
        factor_ip: f32,
        factor_err: f32,
        p0: &Self::QuantizationPreprocessed0,
        param: u16,
    ) -> (f32, f32) {
        rabitq_quantization_process_1(dis_u_2, factor_ppc, factor_ip, factor_err, *p0, param)
    }

    fn proj(projection: &[Vec<f32>], vector: &[f32]) -> Vec<f32> {
        let dims = vector.len();
        assert_eq!(projection.len(), dims);
        (0..dims)
            .map(|i| f32::reduce_sum_of_xy(&projection[i], vector))
            .collect()
    }

    const SUPPORT_FAST_SCAN: bool = true;
    fn fast_scan(preprocessed: &((Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>), Vec<u8>)) -> Vec<u8> {
        preprocessed.1.clone()
    }
    fn fast_scan_resolve(x: f32) -> f32 {
        x
    }
}

macro_rules! unimpl_operator_rabitq {
    ($t:ty) => {
        impl OperatorRabitq for $t {
            const RESIDUAL: bool = false;

            fn sample(_: &impl Vectors<Self::Vector>) -> Vec2<f32> {
                unimplemented!()
            }

            fn cast(_: Borrowed<'_, Self>) -> &[f32] {
                unimplemented!()
            }

            fn residual(_: &[f32], _: &[f32]) -> Vec<f32> {
                unimplemented!()
            }

            fn proj(_: &[Vec<f32>], _: &[f32]) -> Vec<f32> {
                unimplemented!()
            }

            type QuantizationPreprocessed0 = std::convert::Infallible;
            type QuantizationPreprocessed1 = std::convert::Infallible;

            fn rabitq_quantization_preprocess(
                _: &[f32],
            ) -> (
                Self::QuantizationPreprocessed0,
                Self::QuantizationPreprocessed1,
            ) {
                unimplemented!()
            }

            fn rabitq_quantization_process(
                _: f32,
                _: f32,
                _: f32,
                _: f32,
                _: &[u8],
                _: &Self::QuantizationPreprocessed0,
                _: &Self::QuantizationPreprocessed1,
            ) -> (f32, f32) {
                unimplemented!()
            }

            fn rabitq_quantization_process_1(
                _: f32,
                _: f32,
                _: f32,
                _: f32,
                _: &Self::QuantizationPreprocessed0,
                _: u16,
            ) -> (f32, f32) {
                unimplemented!()
            }

            const SUPPORT_FAST_SCAN: bool = false;
            fn fast_scan(_: &Self::QuantizationPreprocessed1) -> Vec<u8> {
                unimplemented!()
            }
            fn fast_scan_resolve(_: f32) -> f32 {
                unimplemented!()
            }
        }
    };
}

unimpl_operator_rabitq!(VectDot<f32>);

unimpl_operator_rabitq!(VectDot<f16>);
unimpl_operator_rabitq!(VectL2<f16>);

unimpl_operator_rabitq!(BVectorDot);
unimpl_operator_rabitq!(BVectorHamming);
unimpl_operator_rabitq!(BVectorJaccard);

unimpl_operator_rabitq!(SVectDot<f32>);
unimpl_operator_rabitq!(SVectL2<f32>);

#[inline(always)]
pub fn rabitq_quantization_process(
    dis_u_2: f32,
    factor_ppc: f32,
    factor_ip: f32,
    factor_err: f32,
    code: &[u8],
    params: (f32, f32, f32, f32),
    (blut, _lut): &((Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>), Vec<u8>),
) -> (f32, f32) {
    let abdp = asymmetric_binary_dot_product(code, blut) as u16;
    rabitq_quantization_process_1(dis_u_2, factor_ppc, factor_ip, factor_err, params, abdp)
}

#[inline(always)]
pub fn rabitq_quantization_process_1(
    dis_u_2: f32,
    factor_ppc: f32,
    factor_ip: f32,
    factor_err: f32,
    (dis_v_2, b, k, qvector_sum): (f32, f32, f32, f32),
    abdp: u16,
) -> (f32, f32) {
    let rough =
        dis_u_2 + dis_v_2 + b * factor_ppc + ((2.0 * abdp as f32) - qvector_sum) * factor_ip * k;
    let err = factor_err * dis_v_2.sqrt();
    (rough, err)
}

fn binarize(vector: &[u8]) -> (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>) {
    let n = vector.len();
    let t0 = {
        let mut t = vec![0u8; n.div_ceil(8)];
        for i in 0..n {
            t[i / 8] |= ((vector[i] >> 0) & 1) << (i % 8);
        }
        t
    };
    let t1 = {
        let mut t = vec![0u8; n.div_ceil(8)];
        for i in 0..n {
            t[i / 8] |= ((vector[i] >> 1) & 1) << (i % 8);
        }
        t
    };
    let t2 = {
        let mut t = vec![0u8; n.div_ceil(8)];
        for i in 0..n {
            t[i / 8] |= ((vector[i] >> 2) & 1) << (i % 8);
        }
        t
    };
    let t3 = {
        let mut t = vec![0u8; n.div_ceil(8)];
        for i in 0..n {
            t[i / 8] |= ((vector[i] >> 3) & 1) << (i % 8);
        }
        t
    };
    (t0, t1, t2, t3)
}

fn gen(qvector: &[u8]) -> Vec<u8> {
    let dims = qvector.len() as u32;
    let t = dims.div_ceil(4);
    let mut lut = vec![0u8; t as usize * 16];
    for i in 0..t as usize {
        let t0 = qvector.get(4 * i + 0).copied().unwrap_or_default();
        let t1 = qvector.get(4 * i + 1).copied().unwrap_or_default();
        let t2 = qvector.get(4 * i + 2).copied().unwrap_or_default();
        let t3 = qvector.get(4 * i + 3).copied().unwrap_or_default();
        lut[16 * i + 0b0000] = 0;
        lut[16 * i + 0b0001] = t0;
        lut[16 * i + 0b0010] = t1;
        lut[16 * i + 0b0011] = t1 + t0;
        lut[16 * i + 0b0100] = t2;
        lut[16 * i + 0b0101] = t2 + t0;
        lut[16 * i + 0b0110] = t2 + t1;
        lut[16 * i + 0b0111] = t2 + t1 + t0;
        lut[16 * i + 0b1000] = t3;
        lut[16 * i + 0b1001] = t3 + t0;
        lut[16 * i + 0b1010] = t3 + t1;
        lut[16 * i + 0b1011] = t3 + t1 + t0;
        lut[16 * i + 0b1100] = t3 + t2;
        lut[16 * i + 0b1101] = t3 + t2 + t0;
        lut[16 * i + 0b1110] = t3 + t2 + t1;
        lut[16 * i + 0b1111] = t3 + t2 + t1 + t0;
    }
    lut
}

fn binary_dot_product(x: &[u8], y: &[u8]) -> u32 {
    assert_eq!(x.len(), y.len());
    let n = x.len();
    let mut res = 0;
    for i in 0..n {
        res += (x[i] & y[i]).count_ones();
    }
    res
}

fn asymmetric_binary_dot_product(x: &[u8], y: &(Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>)) -> u32 {
    let mut res = 0;
    res += binary_dot_product(x, &y.0) << 0;
    res += binary_dot_product(x, &y.1) << 1;
    res += binary_dot_product(x, &y.2) << 2;
    res += binary_dot_product(x, &y.3) << 3;
    res
}
