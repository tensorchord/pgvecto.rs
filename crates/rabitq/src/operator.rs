use base::operator::Borrowed;
use base::operator::*;
use base::scalar::F32;
use common::aligned_array::AlignedArray;
use common::aligned_bytes::AlignBytes;
use num_traits::Float;
use storage::OperatorStorage;

pub trait OperatorRabitq: OperatorStorage {
    const RESIDUAL: bool;
    fn cast(vector: Borrowed<'_, Self>) -> &[F32];
    fn residual(lhs: &[F32], rhs: &[F32]) -> Vec<F32>;

    fn proj(projection: &[Vec<F32>], vector: &[F32]) -> Vec<F32>;

    type QuantizationPreprocessed0;
    type QuantizationPreprocessed1;

    fn rabitq_quantization_preprocess(
        vector: &[F32],
    ) -> (
        Self::QuantizationPreprocessed0,
        Self::QuantizationPreprocessed1,
    );
    fn rabitq_quantization_process_1(
        dis_u_2: F32,
        factor_ppc: F32,
        factor_ip: F32,
        factor_err: F32,
        p0: &Self::QuantizationPreprocessed0,
        param: u16,
    ) -> F32;
    fn rabitq_quantization_process_1_parallel(
        dis_u_2: &[F32; 32],
        factor_ppc: &[F32; 32],
        factor_ip: &[F32; 32],
        factor_err: &[F32; 32],
        p0: &Self::QuantizationPreprocessed0,
        param: &AlignedArray<u16, 32>,
    ) -> AlignedArray<F32, 32>;

    const SUPPORT_FAST_SCAN: bool;
    fn fast_scan(preprocessed: &Self::QuantizationPreprocessed1) -> AlignBytes<64>;
    fn fast_scan_resolve(x: F32) -> F32;
}

impl OperatorRabitq for Vecf32L2 {
    const RESIDUAL: bool = false;
    fn cast(vector: Borrowed<'_, Self>) -> &[F32] {
        vector.slice()
    }
    fn residual(lhs: &[F32], rhs: &[F32]) -> Vec<F32> {
        assert_eq!(lhs.len(), rhs.len());
        let n = lhs.len();
        (0..n).map(|i| lhs[i] - rhs[i]).collect()
    }

    type QuantizationPreprocessed0 = (F32, F32, F32, F32);
    type QuantizationPreprocessed1 = AlignBytes<64>;

    fn rabitq_quantization_preprocess(vector: &[F32]) -> ((F32, F32, F32, F32), AlignBytes<64>) {
        let dis_v_2 = vector.iter().map(|&x| x * x).sum();
        let (k, b, qvector) = quantization::quantize::quantize_15(vector);
        let qvector_sum = F32(qvector.iter().fold(0_u32, |x, &y| x + y as u32) as _);
        let lut = gen(&qvector);
        ((dis_v_2, b, k, qvector_sum), lut)
    }

    #[inline(always)]
    fn rabitq_quantization_process_1(
        dis_u_2: F32,
        factor_ppc: F32,
        factor_ip: F32,
        factor_err: F32,
        p0: &Self::QuantizationPreprocessed0,
        param: u16,
    ) -> F32 {
        rabitq_quantization_process_1(dis_u_2, factor_ppc, factor_ip, factor_err, *p0, param)
    }

    #[inline(always)]
    fn rabitq_quantization_process_1_parallel(
        dis_u_2: &[F32; 32],
        factor_ppc: &[F32; 32],
        factor_ip: &[F32; 32],
        factor_err: &[F32; 32],
        p0: &Self::QuantizationPreprocessed0,
        param: &AlignedArray<u16, 32>,
    ) -> AlignedArray<F32, 32> {
        rabitq_quantization_process_1_parallel(
            dis_u_2, factor_ppc, factor_ip, factor_err, *p0, param,
        )
    }

    fn proj(projection: &[Vec<F32>], vector: &[F32]) -> Vec<F32> {
        let dims = vector.len();
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

    const SUPPORT_FAST_SCAN: bool = true;
    fn fast_scan(preprocessed: &AlignBytes<64>) -> AlignBytes<64> {
        preprocessed.clone()
    }
    fn fast_scan_resolve(x: F32) -> F32 {
        x
    }
}

macro_rules! unimpl_operator_rabitq {
    ($t:ty) => {
        impl OperatorRabitq for $t {
            const RESIDUAL: bool = false;
            fn cast(_: Borrowed<'_, Self>) -> &[F32] {
                unimplemented!()
            }

            fn residual(_: &[F32], _: &[F32]) -> Vec<F32> {
                unimplemented!()
            }

            fn proj(_: &[Vec<F32>], _: &[F32]) -> Vec<F32> {
                unimplemented!()
            }

            type QuantizationPreprocessed0 = std::convert::Infallible;
            type QuantizationPreprocessed1 = std::convert::Infallible;

            fn rabitq_quantization_preprocess(
                _: &[F32],
            ) -> (
                Self::QuantizationPreprocessed0,
                Self::QuantizationPreprocessed1,
            ) {
                unimplemented!()
            }

            fn rabitq_quantization_process_1(
                _: F32,
                _: F32,
                _: F32,
                _: F32,
                _: &Self::QuantizationPreprocessed0,
                _: u16,
            ) -> F32 {
                unimplemented!()
            }

            #[inline(always)]
            fn rabitq_quantization_process_1_parallel(
                _: &[F32; 32],
                _: &[F32; 32],
                _: &[F32; 32],
                _: &[F32; 32],
                _: &Self::QuantizationPreprocessed0,
                _: &AlignedArray<u16, 32>,
            ) -> AlignedArray<F32, 32> {
                unimplemented!()
            }

            const SUPPORT_FAST_SCAN: bool = false;
            fn fast_scan(_: &Self::QuantizationPreprocessed1) -> AlignBytes<64> {
                unimplemented!()
            }
            fn fast_scan_resolve(_: F32) -> F32 {
                unimplemented!()
            }
        }
    };
}

unimpl_operator_rabitq!(Vecf32Dot);

unimpl_operator_rabitq!(Vecf16Dot);
unimpl_operator_rabitq!(Vecf16L2);

unimpl_operator_rabitq!(BVectorDot);
unimpl_operator_rabitq!(BVectorHamming);
unimpl_operator_rabitq!(BVectorJaccard);

unimpl_operator_rabitq!(SVecf32Dot);
unimpl_operator_rabitq!(SVecf32L2);

#[inline(always)]
pub fn rabitq_quantization_process_1(
    dis_u_2: F32,
    factor_ppc: F32,
    factor_ip: F32,
    factor_err: F32,
    (dis_v_2, b, k, qvector_sum): (F32, F32, F32, F32),
    abdp: u16,
) -> F32 {
    let rough =
        dis_u_2 + dis_v_2 + b * factor_ppc + (F32(2.0 * abdp as f32) - qvector_sum) * factor_ip * k;
    let err = factor_err * dis_v_2.sqrt();
    rough - F32(1.9) * err
}

#[inline(always)]
pub fn rabitq_quantization_process_1_parallel(
    dis_u_2: &[F32; 32],
    factor_ppc: &[F32; 32],
    factor_ip: &[F32; 32],
    factor_err: &[F32; 32],
    (dis_v_2, b, k, qvector_sum): (F32, F32, F32, F32),
    abdp: &AlignedArray<u16, 32>,
) -> AlignedArray<F32, 32> {
    unsafe {
        // todo: fix
        rabitq_quantization_process_1_parallel_avx2(
            std::mem::transmute(dis_u_2),
            std::mem::transmute(factor_ppc),
            std::mem::transmute(factor_ip),
            std::mem::transmute(factor_err),
            (dis_v_2, b, k, qvector_sum),
            std::mem::transmute(abdp),
        )
    }
}

#[detect::target_cpu(enable = "v3")]
pub unsafe fn rabitq_quantization_process_1_parallel_avx2(
    dis_u_2: &AlignedArray<F32, 32>,
    factor_ppc: &AlignedArray<F32, 32>,
    factor_ip: &AlignedArray<F32, 32>,
    factor_err: &AlignedArray<F32, 32>,
    (dis_v_2, b, k, qvector_sum): (F32, F32, F32, F32),
    abdp: &AlignedArray<u16, 32>,
) -> AlignedArray<F32, 32> {
    unsafe {
        use core::arch::x86_64::*;
        let mut result = AlignedArray::<F32, 32>([F32(0.0); 32]);
        let dis_v_2 = _mm256_set1_ps(dis_v_2.0);
        let b = _mm256_set1_ps(b.0);
        let k = _mm256_set1_ps(k.0);
        let qvector_sum = _mm256_set1_ps(qvector_sum.0);
        let epsilon = _mm256_set1_ps(1.9);
        for i in (0_usize..32).step_by(8) {
            let dis_u_2 = _mm256_load_ps(dis_u_2.0.as_ptr().add(i).cast());
            let factor_ppc = _mm256_load_ps(factor_ppc.0.as_ptr().add(i).cast());
            let factor_ip = _mm256_load_ps(factor_ip.0.as_ptr().add(i).cast());
            let factor_err = _mm256_load_ps(factor_err.0.as_ptr().add(i).cast());
            let abdp = _mm256_cvtepi32_ps(_mm256_cvtepi16_epi32(_mm_load_si128(
                abdp.0.as_ptr().add(i).cast(),
            )));
            // dis_u_2 + dis_v_2 + b * factor_ppc + (F32(2.0 * abdp as f32) - qvector_sum) * factor_ip * k - factor_err * dis_v_2.sqrt() * epsilon
            let part_2 = _mm256_mul_ps(b, factor_ppc);
            let part_3_left = _mm256_sub_ps(_mm256_mul_ps(_mm256_set1_ps(2.0), abdp), qvector_sum);
            let part_3_right = _mm256_mul_ps(factor_ip, k);
            let part_4 = _mm256_mul_ps(factor_err, _mm256_sqrt_ps(dis_v_2));
            let full = _mm256_sub_ps(
                _mm256_add_ps(
                    _mm256_fmadd_ps(part_3_left, part_3_right, part_2),
                    _mm256_add_ps(dis_v_2, dis_u_2),
                ),
                _mm256_mul_ps(part_4, epsilon),
            );
            _mm256_store_ps(result.0.as_mut_ptr().add(i).cast(), full);
        }
        result
    }
}

fn gen(qvector: &[u8]) -> AlignBytes<64> {
    let dims = qvector.len() as u32;
    let t = dims.div_ceil(4);
    let mut lut = AlignBytes::new_zeroed(t as usize * 16);
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
