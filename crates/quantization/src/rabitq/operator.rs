use base::operator::*;
use base::scalar::ScalarLike;
use base::scalar::F32;
use base::vector::VectorBorrowed;
use num_traits::Float;

pub trait OperatorRabitq: Operator {
    const SUPPORTED: bool;

    type RabitqQuantizationPreprocessed;

    fn rabit_quantization_preprocess(
        vector: Borrowed<'_, Self>,
        projection: &[Vec<F32>],
    ) -> Self::RabitqQuantizationPreprocessed;
    fn rabit_quantization_process(
        dis_u_2: F32,
        factor_ppc: F32,
        factor_ip: F32,
        factor_err: F32,
        code: &[u8],
        p: &Self::RabitqQuantizationPreprocessed,
    ) -> (F32, F32);
    fn proj(projection: &[Vec<F32>], vector: Borrowed<'_, Self>) -> Vec<F32>;
}

impl OperatorRabitq for Vecf32L2 {
    const SUPPORTED: bool = true;

    type RabitqQuantizationPreprocessed =
        ((F32, F32, F32, F32), (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>));

    fn rabit_quantization_preprocess(
        vector: Borrowed<'_, Self>,
        projection: &[Vec<F32>],
    ) -> Self::RabitqQuantizationPreprocessed {
        let vector = Self::proj(projection, vector);
        let dis_v_2 = vector.iter().map(|&x| x * x).sum();
        let (k, b, qvector) = crate::quantize::quantize_15(&vector);
        let qvector_sum = F32(qvector.iter().fold(0_u32, |x, &y| x + y as u32) as _);
        let lut = binarize(&qvector);
        ((dis_v_2, b, k, qvector_sum), lut)
    }

    fn rabit_quantization_process(
        dis_u_2: F32,
        factor_ppc: F32,
        factor_ip: F32,
        factor_err: F32,
        code: &[u8],
        (params, lut): &((F32, F32, F32, F32), (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>)),
    ) -> (F32, F32) {
        let &(dis_v_2, b, k, qvector_sum) = params;
        let abdp = asymmetric_binary_dot_product(code, lut);
        let rough = dis_u_2
            + dis_v_2
            + b * factor_ppc
            + (F32(2.0 * abdp as f32) - qvector_sum) * factor_ip * k;
        let err = factor_err * dis_v_2.sqrt();
        (rough, err)
    }

    fn proj(projection: &[Vec<F32>], vector: Borrowed<'_, Self>) -> Vec<F32> {
        let dims = vector.dims() as usize;
        let vector = vector.slice();
        assert_eq!(projection.len(), dims);
        (0..dims)
            .map(|i| {
                assert_eq!(projection[i].len(), dims);
                let mut xy = F32(0.0);
                for j in 0..dims {
                    xy += projection[i][j] * vector[j];
                }
                xy
            })
            .collect()
    }
}

impl OperatorRabitq for Vecf16L2 {
    const SUPPORTED: bool = true;

    type RabitqQuantizationPreprocessed =
        ((F32, F32, F32, F32), (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>));

    fn rabit_quantization_preprocess(
        vector: Borrowed<'_, Self>,
        projection: &[Vec<F32>],
    ) -> Self::RabitqQuantizationPreprocessed {
        let vector = Self::proj(projection, vector);
        let dis_v_2 = vector.iter().map(|&x| x * x).sum();
        let (k, b, qvector) = crate::quantize::quantize_15(&vector);
        let qvector_sum = F32(qvector.iter().fold(0_u32, |x, &y| x + y as u32) as _);
        let lut = binarize(&qvector);
        ((dis_v_2, b, k, qvector_sum), lut)
    }

    fn rabit_quantization_process(
        dis_u_2: F32,
        factor_ppc: F32,
        factor_ip: F32,
        factor_err: F32,
        code: &[u8],
        (params, lut): &((F32, F32, F32, F32), (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>)),
    ) -> (F32, F32) {
        let &(dis_v_2, b, k, qvector_sum) = params;
        let abdp = asymmetric_binary_dot_product(code, lut);
        let rough = dis_u_2
            + dis_v_2
            + b * factor_ppc
            + (F32(2.0 * abdp as f32) - qvector_sum) * factor_ip * k;
        let err = factor_err * dis_v_2.sqrt();
        (rough, err)
    }

    fn proj(projection: &[Vec<F32>], vector: Borrowed<'_, Self>) -> Vec<F32> {
        let dims = vector.dims() as usize;
        let vector = vector.slice();
        assert_eq!(projection.len(), dims);
        (0..dims)
            .map(|i| {
                assert_eq!(projection[i].len(), dims);
                let mut xy = F32(0.0);
                for j in 0..dims {
                    xy += projection[i][j] * vector[j].to_f();
                }
                xy
            })
            .collect()
    }
}

macro_rules! unimpl_operator_rabitq {
    ($t:ty) => {
        impl OperatorRabitq for $t {
            const SUPPORTED: bool = false;

            type RabitqQuantizationPreprocessed = std::convert::Infallible;

            fn rabit_quantization_preprocess(
                _: Borrowed<'_, Self>,
                _: &[Vec<F32>],
            ) -> Self::RabitqQuantizationPreprocessed {
                unimplemented!()
            }

            fn rabit_quantization_process(
                _: F32,
                _: F32,
                _: F32,
                _: F32,
                _: &[u8],
                _: &Self::RabitqQuantizationPreprocessed,
            ) -> (F32, F32) {
                unimplemented!()
            }

            fn proj(_: &[Vec<F32>], _: Borrowed<'_, Self>) -> Vec<F32> {
                unimplemented!()
            }
        }
    };
}

unimpl_operator_rabitq!(Vecf32Cos);
unimpl_operator_rabitq!(Vecf32Dot);

unimpl_operator_rabitq!(Vecf16Cos);
unimpl_operator_rabitq!(Vecf16Dot);

unimpl_operator_rabitq!(BVecf32Cos);
unimpl_operator_rabitq!(BVecf32Dot);
unimpl_operator_rabitq!(BVecf32L2);
unimpl_operator_rabitq!(BVecf32Jaccard);

unimpl_operator_rabitq!(SVecf32Cos);
unimpl_operator_rabitq!(SVecf32Dot);
unimpl_operator_rabitq!(SVecf32L2);

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
