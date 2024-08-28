use base::distance::Distance;
use base::operator::Borrowed;
use base::operator::*;
use base::scalar::ScalarLike;
use base::search::Vectors;
use common::vec2::Vec2;
use half::f16;
use storage::OperatorStorage;

pub trait OperatorRabitq: OperatorStorage {
    fn sample(vectors: &impl Vectors<Self::Vector>) -> Vec2<f32>;
    fn cast(vector: Borrowed<'_, Self>) -> &[f32];
    fn residual(lhs: &[f32], rhs: &[f32]) -> Vec<f32>;
    fn proj(projection: &[Vec<f32>], vector: &[f32]) -> Vec<f32>;

    type Preprocessed0;
    type Preprocessed1;

    fn preprocess(vector: &[f32]) -> (Self::Preprocessed0, Self::Preprocessed1);
    fn process(
        dis_u_2: f32,
        factor_ppc: f32,
        factor_ip: f32,
        factor_err: f32,
        code: &[u8],
        p0: &Self::Preprocessed0,
        p1: &Self::Preprocessed1,
    ) -> Distance;
    fn process_lowerbound(
        dis_u_2: f32,
        factor_ppc: f32,
        factor_ip: f32,
        factor_err: f32,
        code: &[u8],
        p0: &Self::Preprocessed0,
        p1: &Self::Preprocessed1,
        epsilon: f32,
    ) -> Distance;
    fn fscan_preprocess(preprocessed: &Self::Preprocessed1) -> Vec<u8>;
    fn fscan_process_lowerbound(
        dis_u_2: f32,
        factor_ppc: f32,
        factor_ip: f32,
        factor_err: f32,
        p0: &Self::Preprocessed0,
        param: u16,
        epsilon: f32,
    ) -> Distance;
}

impl OperatorRabitq for VectL2<f32> {
    fn sample(vectors: &impl Vectors<Self::Vector>) -> Vec2<f32> {
        common::sample::sample(vectors.len(), vectors.dims(), |i| vectors.vector(i).slice())
    }
    fn cast(vector: Borrowed<'_, Self>) -> &[f32] {
        vector.slice()
    }
    fn residual(lhs: &[f32], rhs: &[f32]) -> Vec<f32> {
        f32::vector_sub(lhs, rhs)
    }
    fn proj(projection: &[Vec<f32>], vector: &[f32]) -> Vec<f32> {
        let dims = vector.len();
        assert_eq!(projection.len(), dims);
        (0..dims)
            .map(|i| f32::reduce_sum_of_xy(&projection[i], vector))
            .collect()
    }

    type Preprocessed0 = (f32, f32, f32, f32);
    type Preprocessed1 = ((Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>), Vec<u8>);

    fn preprocess(
        vector: &[f32],
    ) -> (
        (f32, f32, f32, f32),
        ((Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>), Vec<u8>),
    ) {
        use quantization::quantize;
        let dis_v_2 = f32::reduce_sum_of_x2(vector);
        let (k, b, qvector) = quantize::quantize::<15>(vector);
        let qvector_sum = quantize::reduce_sum_of_x(&qvector) as f32;
        let blut = binarize(&qvector);
        let lut = gen(qvector);
        ((dis_v_2, b, k, qvector_sum), (blut, lut))
    }

    fn process(
        dis_u_2: f32,
        factor_ppc: f32,
        factor_ip: f32,
        factor_err: f32,
        code: &[u8],
        p0: &(f32, f32, f32, f32),
        p1: &((Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>), Vec<u8>),
    ) -> Distance {
        let abdp = asymmetric_binary_dot_product(code, &p1.0) as u16;
        let (rough, _) = rabitq_l2(dis_u_2, factor_ppc, factor_ip, factor_err, *p0, abdp);
        Distance::from_f32(rough)
    }

    fn process_lowerbound(
        dis_u_2: f32,
        factor_ppc: f32,
        factor_ip: f32,
        factor_err: f32,
        code: &[u8],
        p0: &(f32, f32, f32, f32),
        p1: &((Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>), Vec<u8>),
        epsilon: f32,
    ) -> Distance {
        let abdp = asymmetric_binary_dot_product(code, &p1.0) as u16;
        let (rough, err) = rabitq_l2(dis_u_2, factor_ppc, factor_ip, factor_err, *p0, abdp);
        Distance::from_f32(rough - epsilon * err)
    }

    fn fscan_preprocess(preprocessed: &((Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>), Vec<u8>)) -> Vec<u8> {
        preprocessed.1.clone()
    }

    fn fscan_process_lowerbound(
        dis_u_2: f32,
        factor_ppc: f32,
        factor_ip: f32,
        factor_err: f32,
        p0: &Self::Preprocessed0,
        param: u16,
        epsilon: f32,
    ) -> Distance {
        let (rough, err) = rabitq_l2(dis_u_2, factor_ppc, factor_ip, factor_err, *p0, param);
        Distance::from_f32(rough - epsilon * err)
    }
}

macro_rules! unimpl_operator_rabitq {
    ($t:ty) => {
        impl OperatorRabitq for $t {
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

            type Preprocessed0 = std::convert::Infallible;
            type Preprocessed1 = std::convert::Infallible;

            fn preprocess(_: &[f32]) -> (Self::Preprocessed0, Self::Preprocessed1) {
                unimplemented!()
            }

            fn process(
                _: f32,
                _: f32,
                _: f32,
                _: f32,
                _: &[u8],
                _: &Self::Preprocessed0,
                _: &Self::Preprocessed1,
            ) -> Distance {
                unimplemented!()
            }

            fn process_lowerbound(
                _: f32,
                _: f32,
                _: f32,
                _: f32,
                _: &[u8],
                _: &Self::Preprocessed0,
                _: &Self::Preprocessed1,
                _: f32,
            ) -> Distance {
                unimplemented!()
            }

            fn fscan_preprocess(_: &Self::Preprocessed1) -> Vec<u8> {
                unimplemented!()
            }

            fn fscan_process_lowerbound(
                _: f32,
                _: f32,
                _: f32,
                _: f32,
                _: &Self::Preprocessed0,
                _: u16,
                _: f32,
            ) -> Distance {
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
pub fn rabitq_l2(
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

fn gen(mut qvector: Vec<u8>) -> Vec<u8> {
    let dims = qvector.len() as u32;
    let t = dims.div_ceil(4);
    qvector.resize(qvector.len().next_multiple_of(4), 0);
    let mut lut = vec![0u8; t as usize * 16];
    for i in 0..t as usize {
        unsafe {
            // this hint is used to skip bound checks
            std::hint::assert_unchecked(4 * i + 3 < qvector.len());
            std::hint::assert_unchecked(16 * i + 15 < lut.len());
        }
        let t0 = qvector[4 * i + 0];
        let t1 = qvector[4 * i + 1];
        let t2 = qvector[4 * i + 2];
        let t3 = qvector[4 * i + 3];
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
