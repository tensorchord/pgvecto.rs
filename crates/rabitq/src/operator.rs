use std::ops::Index;

use base::distance::Distance;
use base::operator::Borrowed;
use base::operator::*;
use base::scalar::ScalarLike;
use base::search::Vectors;
use common::vec2::Vec2;
use half::f16;
use storage::OperatorStorage;

pub trait OperatorRabitq: OperatorStorage {
    fn sample(vectors: &impl Vectors<Self::Vector>, nlist: u32) -> Vec2<f32>;
    fn cast(vector: Borrowed<'_, Self>) -> &[f32];
    fn residual(lhs: &[f32], rhs: &[f32]) -> Vec<f32>;
    fn proj(projection: &[Vec<f32>], vector: &[f32]) -> Vec<f32>;

    type VectorParams: IntoIterator<Item = f32>;
    type QvectorParams;
    type QvectorLookup;

    fn train_encode(dims: u32, vector: Vec<f32>, centroid_dot_dis: f32) -> Self::VectorParams;
    fn train_decode<T: Index<usize, Output = f32> + ?Sized>(u: u32, meta: &T)
        -> Self::VectorParams;
    fn preprocess(
        trans_vector: &[f32],
        centroid_dot_dis: f32,
        original_square: f32,
        centroids_square: f32,
    ) -> (Self::QvectorParams, Self::QvectorLookup);
    fn process(
        vector_params: &Self::VectorParams,
        qvector_code: &[u8],
        qvector_params: &Self::QvectorParams,
        qvector_lookup: &Self::QvectorLookup,
    ) -> Distance;
    fn process_lowerbound(
        vector_params: &Self::VectorParams,
        qvector_code: &[u8],
        qvector_params: &Self::QvectorParams,
        qvector_lookup: &Self::QvectorLookup,
        epsilon: f32,
    ) -> Distance;
    fn fscan_preprocess(
        trans_vector: &[f32],
        centroid_dot_dis: f32,
        original_square: f32,
        centroids_square: f32,
    ) -> (Self::QvectorParams, Vec<u8>);
    fn fscan_process_lowerbound(
        vector_params: &Self::VectorParams,
        qvector_params: &Self::QvectorParams,
        binary_prod: u16,
        epsilon: f32,
    ) -> Distance;
}

impl OperatorRabitq for VectL2<f32> {
    fn sample(vectors: &impl Vectors<Self::Vector>, nlist: u32) -> Vec2<f32> {
        common::sample::sample(
            vectors.len(),
            nlist.saturating_mul(256).min(1 << 20),
            vectors.dims(),
            |i| vectors.vector(i).slice(),
        )
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

    // [dis_u_2, factor_ppc, factor_ip, factor_err]
    type VectorParams = [f32; 4];
    // (dis_v_2, lower_bound, delta, qvector_sum)
    type QvectorParams = (f32, f32, f32, f32);
    type QvectorLookup = ((Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>), Vec<u8>);

    fn train_encode(dims: u32, vector: Vec<f32>, _centroid_dot_dis: f32) -> Self::VectorParams {
        let sum_of_abs_x = f32::reduce_sum_of_abs_x(&vector);
        let dis_u_2 = f32::reduce_sum_of_x2(&vector);
        let dis_u = dis_u_2.sqrt();
        let x0 = sum_of_abs_x / (dis_u_2 * (dims as f32)).sqrt();
        let x_x0 = dis_u / x0;
        let fac_norm = (dims as f32).sqrt();
        let max_x1 = 1.0f32 / (dims as f32 - 1.0).sqrt();
        let factor_err = 2.0f32 * max_x1 * (x_x0 * x_x0 - dis_u * dis_u).sqrt();
        let factor_ip = -2.0f32 / fac_norm * x_x0;
        let cnt_pos = vector
            .iter()
            .map(|x| x.is_sign_positive() as i32)
            .sum::<i32>();
        let cnt_neg = vector
            .iter()
            .map(|x| x.is_sign_negative() as i32)
            .sum::<i32>();
        let factor_ppc = factor_ip * (cnt_pos - cnt_neg) as f32;
        [dis_u_2, factor_ppc, factor_ip, factor_err]
    }

    fn train_decode<T: Index<usize, Output = f32> + ?Sized>(
        u: u32,
        meta: &T,
    ) -> Self::VectorParams {
        let dis_u_2 = meta[4 * u as usize + 0];
        let factor_ppc = meta[4 * u as usize + 1];
        let factor_ip = meta[4 * u as usize + 2];
        let factor_err = meta[4 * u as usize + 3];
        [dis_u_2, factor_ppc, factor_ip, factor_err]
    }

    fn preprocess(
        trans_vector: &[f32],
        centroid_dot_dis: f32,
        original_square: f32,
        centroids_square: f32,
    ) -> (Self::QvectorParams, Self::QvectorLookup) {
        use quantization::quantize;
        let dis_v_2 = original_square + centroids_square + 2.0 * centroid_dot_dis;
        let (delta, lower_bound, qvector) = quantize::quantize::<15>(trans_vector);
        let qvector_sum = if trans_vector.len() <= 4369 {
            quantize::reduce_sum_of_x_as_u16(&qvector) as f32
        } else {
            quantize::reduce_sum_of_x_as_u32(&qvector) as f32
        };

        let blut = binarize(&qvector);
        let lut = gen(qvector);
        ((dis_v_2, lower_bound, delta, qvector_sum), (blut, lut))
    }

    fn process(
        vector_params: &Self::VectorParams,
        qvector_code: &[u8],
        qvector_params: &Self::QvectorParams,
        qvector_lookup: &Self::QvectorLookup,
    ) -> Distance {
        let (blut, _) = qvector_lookup;
        let binary_prod = asymmetric_binary_dot_product(qvector_code, blut) as u16;
        let (dis_u_2, factor_ppc, factor_ip, factor_err) = match vector_params {
            [a, b, c, d] => (*a, *b, *c, *d),
        };
        let (rough, _) = rabitq_l2(
            dis_u_2,
            factor_ppc,
            factor_ip,
            factor_err,
            *qvector_params,
            binary_prod,
        );
        Distance::from_f32(rough)
    }

    fn process_lowerbound(
        vector_params: &Self::VectorParams,
        qvector_code: &[u8],
        qvector_params: &Self::QvectorParams,
        qvector_lookup: &Self::QvectorLookup,
        epsilon: f32,
    ) -> Distance {
        let (blut, _) = qvector_lookup;
        let binary_prod = asymmetric_binary_dot_product(qvector_code, blut) as u16;
        let (dis_u_2, factor_ppc, factor_ip, factor_err) = match vector_params {
            [a, b, c, d] => (*a, *b, *c, *d),
        };
        let (rough, err) = rabitq_l2(
            dis_u_2,
            factor_ppc,
            factor_ip,
            factor_err,
            *qvector_params,
            binary_prod,
        );
        Distance::from_f32(rough - epsilon * err)
    }

    fn fscan_preprocess(
        vector: &[f32],
        centroid_dot_dis: f32,
        original_square: f32,
        centroids_square: f32,
    ) -> (Self::QvectorParams, Vec<u8>) {
        use quantization::quantize;
        let dis_v_2 = original_square + centroids_square + 2.0 * centroid_dot_dis;
        let (k, b, qvector) = quantize::quantize::<15>(vector);
        let qvector_sum = if vector.len() <= 4369 {
            quantize::reduce_sum_of_x_as_u16(&qvector) as f32
        } else {
            quantize::reduce_sum_of_x_as_u32(&qvector) as f32
        };
        let lut = gen(qvector);
        ((dis_v_2, b, k, qvector_sum), lut)
    }

    fn fscan_process_lowerbound(
        vector_params: &Self::VectorParams,
        qvector_params: &Self::QvectorParams,
        binary_prod: u16,
        epsilon: f32,
    ) -> Distance {
        let (dis_u_2, factor_ppc, factor_ip, factor_err) = match vector_params {
            [a, b, c, d] => (*a, *b, *c, *d),
        };
        let (rough, err) = rabitq_l2(
            dis_u_2,
            factor_ppc,
            factor_ip,
            factor_err,
            *qvector_params,
            binary_prod,
        );
        Distance::from_f32(rough - epsilon * err)
    }
}

impl OperatorRabitq for VectDot<f32> {
    fn sample(vectors: &impl Vectors<Self::Vector>, nlist: u32) -> Vec2<f32> {
        VectL2::<f32>::sample(vectors, nlist)
    }
    fn cast(vector: Borrowed<'_, Self>) -> &[f32] {
        VectL2::<f32>::cast(vector)
    }
    fn residual(lhs: &[f32], rhs: &[f32]) -> Vec<f32> {
        VectL2::<f32>::residual(lhs, rhs)
    }
    fn proj(projection: &[Vec<f32>], vector: &[f32]) -> Vec<f32> {
        VectL2::<f32>::proj(projection, vector)
    }

    // [centroid_dot_dis, factor_ppc, factor_ip, factor_err]
    type VectorParams = [f32; 4];
    // (dis_v_2, centroid_dot_vector, lower_bound, delta, qvector_sum)
    type QvectorParams = (f32, f32, f32, f32, f32);
    type QvectorLookup = ((Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>), Vec<u8>);

    fn train_encode(dims: u32, vector: Vec<f32>, centroid_dot_dis: f32) -> Self::VectorParams {
        let (factor_ppc, factor_ip, factor_err) =
            match VectL2::<f32>::train_encode(dims, vector, centroid_dot_dis) {
                [_, b, c, d] => (b, c, d),
            };

        [centroid_dot_dis, factor_ppc, factor_ip, factor_err]
    }

    fn train_decode<T: Index<usize, Output = f32> + ?Sized>(
        u: u32,
        meta: &T,
    ) -> Self::VectorParams {
        let dis_u_c_dot = meta[4 * u as usize + 0];
        let factor_ppc = meta[4 * u as usize + 1];
        let factor_ip = meta[4 * u as usize + 2];
        let factor_err = meta[4 * u as usize + 3];
        [dis_u_c_dot, factor_ppc, factor_ip, factor_err]
    }

    fn preprocess(
        trans_vector: &[f32],
        centroid_dot_dis: f32,
        original_square: f32,
        centroids_square: f32,
    ) -> (Self::QvectorParams, Self::QvectorLookup) {
        // centroid_dot_vector = <c, c-q_r> = |c| - <c, q_r>
        let centroid_dot_vector = centroids_square + centroid_dot_dis;
        let ((dis_v_2, lower_bound, delta, qvector_sum), lookup) = VectL2::<f32>::preprocess(
            trans_vector,
            centroid_dot_dis,
            original_square,
            centroids_square,
        );
        (
            (
                dis_v_2,
                centroid_dot_vector,
                lower_bound,
                delta,
                qvector_sum,
            ),
            lookup,
        )
    }

    fn process(
        vector_params: &Self::VectorParams,
        qvector_code: &[u8],
        qvector_params: &Self::QvectorParams,
        qvector_lookup: &Self::QvectorLookup,
    ) -> Distance {
        let (blut, _) = qvector_lookup;
        let binary_prod = asymmetric_binary_dot_product(qvector_code, blut) as u16;
        let (dis_u_2, factor_ppc, factor_ip, factor_err) = match vector_params {
            [a, b, c, d] => (*a, *b, *c, *d),
        };
        let (rough, _) = rabitq_dot(
            dis_u_2,
            factor_ppc,
            factor_ip,
            factor_err,
            *qvector_params,
            binary_prod,
        );
        Distance::from_f32(rough)
    }

    fn process_lowerbound(
        vector_params: &Self::VectorParams,
        qvector_code: &[u8],
        qvector_params: &Self::QvectorParams,
        qvector_lookup: &Self::QvectorLookup,
        epsilon: f32,
    ) -> Distance {
        let (blut, _) = qvector_lookup;
        let binary_prod = asymmetric_binary_dot_product(qvector_code, blut) as u16;
        let (dis_u_c_dot, factor_ppc, factor_ip, factor_err) = match vector_params {
            [a, b, c, d] => (*a, *b, *c, *d),
        };
        let (rough, err) = rabitq_dot(
            dis_u_c_dot,
            factor_ppc,
            factor_ip,
            factor_err,
            *qvector_params,
            binary_prod,
        );
        Distance::from_f32(rough - epsilon * err)
    }
    fn fscan_preprocess(
        vector: &[f32],
        centroid_dot_dis: f32,
        original_square: f32,
        centroids_square: f32,
    ) -> (Self::QvectorParams, Vec<u8>) {
        use quantization::quantize;
        let dis_v_2 = original_square + centroids_square + 2.0 * centroid_dot_dis;
        // centroid_dot_vector = <c, c-q_r> = |c| - <c, q_r>
        let centroid_dot_vector = centroids_square + centroid_dot_dis;
        let (k, b, qvector) = quantize::quantize::<15>(vector);
        let qvector_sum = if vector.len() <= 4369 {
            quantize::reduce_sum_of_x_as_u16(&qvector) as f32
        } else {
            quantize::reduce_sum_of_x_as_u32(&qvector) as f32
        };
        let lut = gen(qvector);
        ((dis_v_2, centroid_dot_vector, b, k, qvector_sum), lut)
    }

    fn fscan_process_lowerbound(
        vector_params: &Self::VectorParams,
        qvector_params: &Self::QvectorParams,
        binary_prod: u16,
        epsilon: f32,
    ) -> Distance {
        let (dis_u_c_dot, factor_ppc, factor_ip, factor_err) = match vector_params {
            [a, b, c, d] => (*a, *b, *c, *d),
        };
        let (rough, err) = rabitq_dot(
            dis_u_c_dot,
            factor_ppc,
            factor_ip,
            factor_err,
            *qvector_params,
            binary_prod,
        );
        Distance::from_f32(rough - epsilon * err)
    }
}

macro_rules! unimpl_operator_rabitq {
    ($t:ty) => {
        impl OperatorRabitq for $t {
            fn sample(_: &impl Vectors<Self::Vector>, _: u32) -> Vec2<f32> {
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

            type VectorParams = [f32; 0];
            type QvectorParams = std::convert::Infallible;
            type QvectorLookup = std::convert::Infallible;

            fn train_encode(_: u32, _: Vec<f32>, _: f32) -> Self::VectorParams {
                unimplemented!()
            }

            fn train_decode<T: Index<usize, Output = f32> + ?Sized>(
                _: u32,
                _: &T,
            ) -> Self::VectorParams {
                unimplemented!()
            }

            fn preprocess(
                _: &[f32],
                _: f32,
                _: f32,
                _: f32,
            ) -> (Self::QvectorParams, Self::QvectorLookup) {
                unimplemented!()
            }

            fn process(
                _: &Self::VectorParams,
                _: &[u8],
                _: &Self::QvectorParams,
                _: &Self::QvectorLookup,
            ) -> Distance {
                unimplemented!()
            }

            fn process_lowerbound(
                _: &Self::VectorParams,
                _: &[u8],
                _: &Self::QvectorParams,
                _: &Self::QvectorLookup,
                _: f32,
            ) -> Distance {
                unimplemented!()
            }

            fn fscan_preprocess(
                _: &[f32],
                _: f32,
                _: f32,
                _: f32,
            ) -> (Self::QvectorLookup, Vec<u8>) {
                unimplemented!()
            }
            fn fscan_process_lowerbound(
                _: &Self::VectorParams,
                _: &Self::QvectorParams,
                _: u16,
                _: f32,
            ) -> Distance {
                unimplemented!()
            }
        }
    };
}

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
    (dis_v_2, lower_bound, delta, qvector_sum): (f32, f32, f32, f32),
    binary_prod: u16,
) -> (f32, f32) {
    let rough = dis_u_2
        + dis_v_2
        + lower_bound * factor_ppc
        + ((2.0 * binary_prod as f32) - qvector_sum) * factor_ip * delta;
    let err = factor_err * dis_v_2.sqrt();
    (rough, err)
}

#[inline(always)]
pub fn rabitq_dot(
    dis_u_c_dot: f32,
    factor_ppc: f32,
    factor_ip: f32,
    factor_err: f32,
    (dis_v_2, centroid_dot_vector, lower_bound, delta, qvector_sum): (f32, f32, f32, f32, f32),
    binary_prod: u16,
) -> (f32, f32) {
    let rough = dis_u_c_dot
        + centroid_dot_vector
        + 0.5 * lower_bound * factor_ppc
        + 0.5 * ((2.0 * binary_prod as f32) - qvector_sum) * factor_ip * delta;
    let err = factor_err * dis_v_2.sqrt() * 0.5;
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

#[cfg(test)]
mod test {
    use super::*;
    use common::mmap_array::MmapArray;
    use quantization::utils::InfiniteByteChunks;
    use rand::{thread_rng, Rng};
    use std::{env, sync::LazyLock};

    const EPSILON: f32 = 2.9;
    const LENGTH: usize = 128;
    const ATTEMPTS: usize = 10000;

    struct Case {
        original: Vec<f32>,
        centroid: Vec<f32>,
        trans_vector: Vec<f32>,
        centroid_dot_dis: f32,
        centroids_square: f32,
    }

    static PREPROCESS_O: LazyLock<Case> = LazyLock::new(|| {
        let original: Vec<f32> = [(); LENGTH]
            .into_iter()
            .map(|_| thread_rng().gen_range((-1.0 * LENGTH as f32)..(LENGTH as f32)))
            .collect();
        let centroid: Vec<f32> = vec![0.0; LENGTH].into_iter().collect();
        Case {
            original: original.clone(),
            centroid: centroid.clone(),
            trans_vector: VectL2::<f32>::residual(&original, &centroid),
            centroid_dot_dis: -f32::reduce_sum_of_xy(&original, &centroid),
            centroids_square: f32::reduce_sum_of_x2(&centroid),
        }
    });

    #[test]
    fn vector_f32l2_encode_decode() {
        let path = env::temp_dir().join("meta_l2");
        let _ = std::fs::remove_file(path.clone());
        let case = &*PREPROCESS_O;

        let meta = VectL2::<f32>::train_encode(
            case.trans_vector.len() as u32,
            case.trans_vector.clone(),
            case.centroid_dot_dis,
        );
        let mmap = MmapArray::create(path.clone(), Box::new(meta.into_iter()));
        let params = VectL2::<f32>::train_decode(0, &mmap);
        assert_eq!(
            meta, params,
            "Vecf32L2 encode and decode failed {:?} != {:?}",
            meta, params
        );
        std::fs::remove_file(path.clone()).unwrap();
    }

    #[test]
    fn vector_f32dot_encode_decode() {
        let path = env::temp_dir().join("meta_dot");
        let _ = std::fs::remove_file(path.clone());
        let case = &*PREPROCESS_O;

        let meta = VectDot::<f32>::train_encode(
            case.trans_vector.len() as u32,
            case.trans_vector.clone(),
            case.centroid_dot_dis,
        );
        let mmap = MmapArray::create(path.clone(), Box::new(meta.into_iter()));
        let params = VectDot::<f32>::train_decode(0, &mmap);
        assert_eq!(
            meta, params,
            "Vecf32Dot encode and decode failed {:?} != {:?}",
            meta, params
        );
        std::fs::remove_file(path.clone()).unwrap();
    }

    #[test]
    fn vector_f32l2_estimate() {
        let mut bad: usize = 0;
        let case = &*PREPROCESS_O;
        for _ in 0..ATTEMPTS {
            let (query, trans_vector, centroid_dot_dis, original_square, codes, estimate_failed) =
                estimate_prepare_query(&case.centroid);

            let vector_params = VectL2::<f32>::train_encode(
                case.trans_vector.len() as u32,
                case.trans_vector.clone(),
                case.centroid_dot_dis,
            );
            let (qvector_params, qvector_lookup) = VectL2::<f32>::preprocess(
                &trans_vector,
                centroid_dot_dis,
                original_square,
                case.centroids_square,
            );
            let est =
                VectL2::<f32>::process(&vector_params, &codes, &qvector_params, &qvector_lookup);
            let lower_bound = VectL2::<f32>::process_lowerbound(
                &vector_params,
                &codes,
                &qvector_params,
                &qvector_lookup,
                EPSILON,
            );

            let real = f32::reduce_sum_of_d2(&query, &case.original);
            if estimate_failed(est.to_f32(), lower_bound.to_f32(), real) {
                bad += 1;
            }
        }
        let error_rate = (bad as f32) / (ATTEMPTS as f32);
        assert!(
            error_rate < 0.02,
            "too many errors: {} in {}",
            bad,
            ATTEMPTS,
        );
    }

    #[test]
    fn vector_f32dot_estimate() {
        let mut bad: usize = 0;
        let case = &*PREPROCESS_O;
        for _ in 0..ATTEMPTS {
            let (query, trans_vector, centroid_dot_dis, original_square, codes, estimate_failed) =
                estimate_prepare_query(&case.centroid);

            let vector_params = VectDot::<f32>::train_encode(
                case.trans_vector.len() as u32,
                case.trans_vector.clone(),
                case.centroid_dot_dis,
            );
            let (qvector_params, qvector_lookup) = VectDot::<f32>::preprocess(
                &trans_vector,
                centroid_dot_dis,
                original_square,
                case.centroids_square,
            );
            let est =
                VectDot::<f32>::process(&vector_params, &codes, &qvector_params, &qvector_lookup);
            let lower_bound = VectDot::<f32>::process_lowerbound(
                &vector_params,
                &codes,
                &qvector_params,
                &qvector_lookup,
                EPSILON,
            );

            let real = -f32::reduce_sum_of_xy(&query, &case.original);
            if estimate_failed(est.to_f32(), lower_bound.to_f32(), real) {
                bad += 1;
            }
        }
        let error_rate = (bad as f32) / (ATTEMPTS as f32);
        assert!(
            error_rate < 0.02,
            "too many errors: {} in {}",
            bad,
            ATTEMPTS,
        );
    }

    fn estimate_prepare_query(
        centroid: &Vec<f32>,
    ) -> (
        Vec<f32>,
        Vec<f32>,
        f32,
        f32,
        Vec<u8>,
        impl Fn(f32, f32, f32) -> bool,
    ) {
        fn merge_8([b0, b1, b2, b3, b4, b5, b6, b7]: [u8; 8]) -> u8 {
            b0 | (b1 << 1) | (b2 << 2) | (b3 << 3) | (b4 << 4) | (b5 << 5) | (b6 << 6) | (b7 << 7)
        }
        let query: Vec<f32> = [(); LENGTH]
            .into_iter()
            .map(|_| thread_rng().gen_range((-1.0 * LENGTH as f32)..(LENGTH as f32)))
            .collect();
        let trans_vector = VectL2::<f32>::residual(&query, centroid);
        let centroid_dot_dis = -f32::reduce_sum_of_xy(&query, centroid);
        let original_square = f32::reduce_sum_of_x2(centroid);
        let codes =
            InfiniteByteChunks::new(trans_vector.iter().map(|e| e.is_sign_positive() as u8))
                .map(merge_8)
                .take(trans_vector.len().div_ceil(8))
                .collect();
        fn estimate_failed(est: f32, lower_bound: f32, real: f32) -> bool {
            let upper_bound = 2.0 * est - lower_bound;
            lower_bound <= real && upper_bound >= real
        }
        (
            query,
            trans_vector,
            centroid_dot_dis,
            original_square,
            codes,
            estimate_failed,
        )
    }
}
