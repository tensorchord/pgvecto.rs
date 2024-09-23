use crate::fast_scan::b4::fast_scan_b4;
use crate::fast_scan::b4::pack;
use crate::quantizer::Quantizer;
use crate::reranker::error::ErrorFlatReranker;
use crate::reranker::graph_2::Graph2Reranker;
use crate::utils::InfiniteByteChunks;
use base::always_equal::AlwaysEqual;
use base::distance::Distance;
use base::index::*;
use base::operator::*;
use base::scalar::impossible::Impossible;
use base::scalar::ScalarLike;
use base::search::*;
use base::vector::VectOwned;
use base::vector::VectorBorrowed;
use base::vector::VectorOwned;
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::marker::PhantomData;
use std::ops::Range;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct RabitqQuantizer<O: OperatorRabitqQuantization> {
    dims: u32,
    projection: Vec<Vec<O::Scalar>>,
    _maker: PhantomData<O>,
}

impl<O: OperatorRabitqQuantization> Quantizer<O> for RabitqQuantizer<O> {
    fn train(
        vector_options: VectorOptions,
        _: Option<QuantizationOptions>,
        _: &(impl Vectors<O::Vector> + Sync),
        _: impl Fn(Borrowed<'_, O>) -> O::Vector + Copy + Sync,
    ) -> Self {
        let dims = vector_options.dims;
        let projection = {
            use nalgebra::{DMatrix, QR};
            use rand::{Rng, SeedableRng};
            use rand_chacha::ChaCha12Rng;
            use rand_distr::StandardNormal;
            let mut rng = ChaCha12Rng::from_seed([7; 32]);
            let matrix = DMatrix::from_fn(dims as usize, dims as usize, |_, _| {
                rng.sample(StandardNormal)
            });
            let qr = QR::new(matrix);
            let q = qr.q();
            let mut projection = Vec::new();
            for v in q.row_iter() {
                let vector = v.iter().copied().collect::<Vec<_>>();
                projection.push(O::Scalar::vector_from_f32(&vector));
            }
            projection
        };
        Self {
            dims,
            projection,
            _maker: PhantomData,
        }
    }

    fn encode(&self, vector: Borrowed<'_, O>) -> Vec<u8> {
        let dims = self.dims;
        let (a, b, c, d, e) = O::code(vector);
        let mut result = Vec::with_capacity(size_of::<f32>() * 4);
        result.extend(a.to_ne_bytes());
        result.extend(b.to_ne_bytes());
        result.extend(c.to_ne_bytes());
        result.extend(d.to_ne_bytes());
        for x in InfiniteByteChunks::<_, 64>::new(e.into_iter()).take(dims.div_ceil(64) as usize) {
            let mut r = 0_u64;
            for i in 0..64 {
                r |= (x[i] as u64) << i;
            }
            result.extend(r.to_ne_bytes().into_iter());
        }
        result
    }

    fn fscan_encode(&self, vectors: [O::Vector; 32]) -> Vec<u8> {
        let dims = self.dims;
        let coded = vectors.map(|vector| O::code(vector.as_borrowed()));
        let codes = coded.clone().map(|(_, _, _, _, e)| {
            InfiniteByteChunks::new(e.into_iter())
                .map(|[b0, b1, b2, b3]| b0 | b1 << 1 | b2 << 2 | b3 << 3)
                .take(dims.div_ceil(4) as usize)
                .collect()
        });
        let mut result = Vec::with_capacity(size_of::<f32>() * 128);
        for i in 0..32 {
            result.extend(coded[i].0.to_ne_bytes());
        }
        for i in 0..32 {
            result.extend(coded[i].1.to_ne_bytes());
        }
        for i in 0..32 {
            result.extend(coded[i].2.to_ne_bytes());
        }
        for i in 0..32 {
            result.extend(coded[i].3.to_ne_bytes());
        }
        result.extend(pack(dims.div_ceil(4), codes));
        result
    }

    fn code_size(&self) -> u32 {
        size_of::<f32>() as u32 * 4 + size_of::<u64>() as u32 * self.dims.div_ceil(64)
    }

    fn fcode_size(&self) -> u32 {
        size_of::<f32>() as u32 * 128 + self.dims.div_ceil(4) * 16
    }

    type Lut = O::Lut;

    fn preprocess(&self, vector: Borrowed<'_, O>) -> Self::Lut {
        O::preprocess(vector)
    }

    fn process(&self, lut: &Self::Lut, code: &[u8], _: Borrowed<'_, O>) -> Distance {
        let c = parse_code(code);
        O::process(lut, c)
    }

    fn project(&self, vector: Borrowed<'_, O>) -> O::Vector {
        O::project(&self.projection, vector)
    }

    type FLut = O::FLut;

    fn fscan_preprocess(&self, vector: Borrowed<'_, O>) -> Self::FLut {
        O::fscan_preprocess(vector)
    }

    fn fscan_process(&self, flut: &Self::FLut, code: &[u8]) -> [Distance; 32] {
        let c = parses_codes(code);
        O::fscan_process(self.dims, flut, c)
    }

    type FlatRerankVec = Vec<(Reverse<Distance>, AlwaysEqual<u32>)>;

    fn flat_rerank_start() -> Self::FlatRerankVec {
        Vec::new()
    }

    fn flat_rerank_preprocess(
        &self,
        vector: Borrowed<'_, O>,
        opts: &SearchOptions,
    ) -> Result<Self::FLut, Self::Lut> {
        if opts.rq_fast_scan {
            Ok(self.fscan_preprocess(vector))
        } else {
            Err(self.preprocess(vector))
        }
    }

    fn flat_rerank_continue<C>(
        &self,
        locate_0: impl Fn(u32) -> C,
        locate_1: impl Fn(u32) -> C,
        frlut: &Result<Self::FLut, Self::Lut>,
        range: Range<u32>,
        heap: &mut Self::FlatRerankVec,
    ) where
        C: AsRef<[u8]>,
    {
        match frlut {
            Ok(flut) => {
                fn divide(r: Range<u32>) -> (Option<u32>, Range<u32>, Option<u32>) {
                    if r.start > r.end {
                        return (None, r.start / 32..r.end / 32, None);
                    }
                    if r.start / 32 == r.end / 32 {
                        return (Some(r.start / 32), 0..0, None);
                    };
                    let left = if r.start % 32 == 0 {
                        (None, r.start / 32)
                    } else {
                        (Some(r.start / 32), r.start / 32 + 1)
                    };
                    let right = if r.end % 32 == 0 {
                        (r.end / 32, None)
                    } else {
                        (r.end / 32, Some(r.end / 32))
                    };
                    (left.0, left.1..right.0, right.1)
                }
                let (left, main, right) = divide(range.clone());
                if let Some(i) = left {
                    let c = locate_1(i);
                    let c = parses_codes(c.as_ref());
                    let r = O::fscan_process_lowerbound(self.dims, flut, c, 1.9);
                    for j in 0..32 {
                        if range.contains(&(i * 32 + j)) {
                            heap.push((Reverse(r[j as usize]), AlwaysEqual(i * 32 + j)));
                        }
                    }
                }
                for i in main {
                    let c = locate_1(i);
                    let c = parses_codes(c.as_ref());
                    let r = O::fscan_process_lowerbound(self.dims, flut, c, 1.9);
                    for j in 0..32 {
                        heap.push((Reverse(r[j as usize]), AlwaysEqual(i * 32 + j)));
                    }
                }
                if let Some(i) = right {
                    let c = locate_1(i);
                    let c = parses_codes(c.as_ref());
                    let r = O::fscan_process_lowerbound(self.dims, flut, c, 1.9);
                    for j in 0..32 {
                        if range.contains(&(i * 32 + j)) {
                            heap.push((Reverse(r[j as usize]), AlwaysEqual(i * 32 + j)));
                        }
                    }
                }
            }
            Err(lut) => {
                for j in range {
                    let c = locate_0(j);
                    let c = parse_code(c.as_ref());
                    let r = O::process_lowerbound(lut, c, 1.9);
                    heap.push((Reverse(r), AlwaysEqual(j)));
                }
            }
        }
    }

    fn flat_rerank_break<'a, T: 'a, R>(
        &'a self,
        heap: Self::FlatRerankVec,
        rerank: R,
        _: &SearchOptions,
    ) -> impl RerankerPop<T> + 'a
    where
        R: Fn(u32) -> (Distance, T) + 'a,
    {
        ErrorFlatReranker::new(heap, rerank)
    }

    fn graph_rerank<'a, T, R, C>(
        &'a self,
        lut: Self::Lut,
        locate: impl Fn(u32) -> C + 'a,
        rerank: R,
    ) -> impl RerankerPush + RerankerPop<T> + 'a
    where
        T: 'a,
        R: Fn(u32) -> (Distance, T) + 'a,
        C: AsRef<[u8]>,
    {
        Graph2Reranker::new(
            move |u| O::process(&lut, parse_code(locate(u).as_ref())),
            rerank,
        )
    }
}

pub trait OperatorRabitqQuantization: Operator {
    type Scalar: ScalarLike;

    fn code(vector: Borrowed<'_, Self>) -> (f32, f32, f32, f32, Vec<u8>);

    fn project(projection: &[Vec<Self::Scalar>], vector: Borrowed<'_, Self>) -> Self::Vector;

    type Lut;
    fn preprocess(vector: Borrowed<'_, Self>) -> Self::Lut;
    fn process(lut: &Self::Lut, code: (f32, f32, f32, f32, &[u64])) -> Distance;
    fn process_lowerbound(
        lut: &Self::Lut,
        code: (f32, f32, f32, f32, &[u64]),
        epsilon: f32,
    ) -> Distance;

    type FLut;
    fn fscan_preprocess(vector: Borrowed<'_, Self>) -> Self::FLut;
    fn fscan_process(
        dims: u32,
        lut: &Self::FLut,
        code: (&[f32; 32], &[f32; 32], &[f32; 32], &[f32; 32], &[u8]),
    ) -> [Distance; 32];
    fn fscan_process_lowerbound(
        dims: u32,
        lut: &Self::FLut,
        code: (&[f32; 32], &[f32; 32], &[f32; 32], &[f32; 32], &[u8]),
        epsilon: f32,
    ) -> [Distance; 32];
}

impl<S: ScalarLike> OperatorRabitqQuantization for VectL2<S> {
    type Scalar = S;

    fn code(vector: Borrowed<'_, Self>) -> (f32, f32, f32, f32, Vec<u8>) {
        let dims = vector.dims();
        let vector = vector.slice();
        let sum_of_abs_x = S::reduce_sum_of_abs_x(vector);
        let sum_of_x2 = S::reduce_sum_of_x2(vector);
        let dis_u = sum_of_x2.sqrt();
        let x0 = sum_of_abs_x / (sum_of_x2 * (dims as f32)).sqrt();
        let x_x0 = dis_u / x0;
        let fac_norm = (dims as f32).sqrt();
        let max_x1 = 1.0f32 / (dims as f32 - 1.0).sqrt();
        let factor_err = 2.0f32 * max_x1 * (x_x0 * x_x0 - dis_u * dis_u).sqrt();
        let factor_ip = -2.0f32 / fac_norm * x_x0;
        let cnt_pos = vector
            .iter()
            .map(|x| x.scalar_is_sign_positive() as i32)
            .sum::<i32>();
        let cnt_neg = vector
            .iter()
            .map(|x| x.scalar_is_sign_negative() as i32)
            .sum::<i32>();
        let factor_ppc = factor_ip * (cnt_pos - cnt_neg) as f32;
        let mut code = Vec::new();
        for i in 0..dims {
            code.push(vector[i as usize].scalar_is_sign_positive() as u8);
        }
        (sum_of_x2, factor_ppc, factor_ip, factor_err, code)
    }

    fn project(projection: &[Vec<Self::Scalar>], vector: Borrowed<'_, Self>) -> Self::Vector {
        let slice = (0..projection.len())
            .map(|i| S::from_f32(S::reduce_sum_of_xy(&projection[i], vector.slice())))
            .collect();
        VectOwned::new(slice)
    }

    type Lut = (f32, f32, f32, f32, (Vec<u64>, Vec<u64>, Vec<u64>, Vec<u64>));

    fn preprocess(vector: Borrowed<'_, Self>) -> Self::Lut {
        use crate::quantize;
        let vector = vector.slice();
        let dis_v_2 = S::reduce_sum_of_x2(vector);
        let (k, b, qvector) = quantize::quantize::<15>(S::vector_to_f32_borrowed(vector).as_ref());
        let qvector_sum = if vector.len() <= 4369 {
            quantize::reduce_sum_of_x_as_u16(&qvector) as f32
        } else {
            quantize::reduce_sum_of_x_as_u32(&qvector) as f32
        };
        let lut = binarize(&qvector);
        (dis_v_2, b, k, qvector_sum, lut)
    }

    fn process(
        lut: &Self::Lut,
        (dis_u_2, factor_ppc, factor_ip, _, t): (f32, f32, f32, f32, &[u64]),
    ) -> Distance {
        let &(dis_v_2, b, k, qvector_sum, ref s) = lut;
        let value = asymmetric_binary_dot_product(t, s) as u16;
        let rough = dis_u_2
            + dis_v_2
            + b * factor_ppc
            + ((2.0 * value as f32) - qvector_sum) * factor_ip * k;
        Distance::from_f32(rough)
    }

    fn process_lowerbound(
        lut: &Self::Lut,
        (dis_u_2, factor_ppc, factor_ip, factor_err, t): (f32, f32, f32, f32, &[u64]),
        epsilon: f32,
    ) -> Distance {
        let &(dis_v_2, b, k, qvector_sum, ref s) = lut;
        let value = asymmetric_binary_dot_product(t, s) as u16;
        let rough = dis_u_2
            + dis_v_2
            + b * factor_ppc
            + ((2.0 * value as f32) - qvector_sum) * factor_ip * k;
        let err = factor_err * dis_v_2.sqrt();
        Distance::from_f32(rough - epsilon * err)
    }

    type FLut = (f32, f32, f32, f32, Vec<u8>);

    fn fscan_preprocess(vector: Borrowed<'_, Self>) -> Self::FLut {
        use crate::quantize;
        let vector = vector.slice();
        let dis_v_2 = S::reduce_sum_of_x2(vector);
        let (k, b, qvector) = quantize::quantize::<15>(S::vector_to_f32_borrowed(vector).as_ref());
        let qvector_sum = if vector.len() <= 4369 {
            quantize::reduce_sum_of_x_as_u16(&qvector) as f32
        } else {
            quantize::reduce_sum_of_x_as_u32(&qvector) as f32
        };
        let lut = gen(qvector);
        (dis_v_2, b, k, qvector_sum, lut)
    }

    fn fscan_process(
        dims: u32,
        lut: &Self::FLut,
        (dis_u_2, factor_ppc, factor_ip, _, t): (
            &[f32; 32],
            &[f32; 32],
            &[f32; 32],
            &[f32; 32],
            &[u8],
        ),
    ) -> [Distance; 32] {
        let &(dis_v_2, b, k, qvector_sum, ref s) = lut;
        let r = fast_scan_b4(dims.div_ceil(4), t, s);
        std::array::from_fn(|i| {
            let rough = dis_u_2[i]
                + dis_v_2
                + b * factor_ppc[i]
                + ((2.0 * r[i] as f32) - qvector_sum) * factor_ip[i] * k;
            Distance::from_f32(rough)
        })
    }

    fn fscan_process_lowerbound(
        dims: u32,
        lut: &Self::FLut,
        (dis_u_2, factor_ppc, factor_ip, factor_err, t): (
            &[f32; 32],
            &[f32; 32],
            &[f32; 32],
            &[f32; 32],
            &[u8],
        ),
        epsilon: f32,
    ) -> [Distance; 32] {
        let &(dis_v_2, b, k, qvector_sum, ref s) = lut;
        let r = fast_scan_b4(dims.div_ceil(4), t, s);
        std::array::from_fn(|i| {
            let rough = dis_u_2[i]
                + dis_v_2
                + b * factor_ppc[i]
                + ((2.0 * r[i] as f32) - qvector_sum) * factor_ip[i] * k;
            let err = factor_err[i] * dis_v_2.sqrt();
            Distance::from_f32(rough - epsilon * err)
        })
    }
}

impl<S: ScalarLike> OperatorRabitqQuantization for VectDot<S> {
    type Scalar = S;

    fn code(vector: Borrowed<'_, Self>) -> (f32, f32, f32, f32, Vec<u8>) {
        let dims = vector.dims();
        let vector = vector.slice();
        let sum_of_abs_x = S::reduce_sum_of_abs_x(vector);
        let sum_of_x2 = S::reduce_sum_of_x2(vector);
        let dis_u = sum_of_x2.sqrt();
        let x0 = sum_of_abs_x / (sum_of_x2 * (dims as f32)).sqrt();
        let x_x0 = dis_u / x0;
        let fac_norm = (dims as f32).sqrt();
        let max_x1 = 1.0f32 / (dims as f32 - 1.0).sqrt();
        let factor_err = 2.0f32 * max_x1 * (x_x0 * x_x0 - dis_u * dis_u).sqrt();
        let factor_ip = -2.0f32 / fac_norm * x_x0;
        let cnt_pos = vector
            .iter()
            .map(|x| x.scalar_is_sign_positive() as i32)
            .sum::<i32>();
        let cnt_neg = vector
            .iter()
            .map(|x| x.scalar_is_sign_negative() as i32)
            .sum::<i32>();
        let factor_ppc = factor_ip * (cnt_pos - cnt_neg) as f32;
        let mut code = Vec::new();
        for i in 0..dims {
            code.push(vector[i as usize].scalar_is_sign_positive() as u8);
        }
        (sum_of_x2, factor_ppc, factor_ip, factor_err, code)
    }

    fn project(projection: &[Vec<Self::Scalar>], vector: Borrowed<'_, Self>) -> Self::Vector {
        let slice = (0..projection.len())
            .map(|i| S::from_f32(S::reduce_sum_of_xy(&projection[i], vector.slice())))
            .collect();
        VectOwned::new(slice)
    }

    type Lut = (f32, f32, f32, f32, (Vec<u64>, Vec<u64>, Vec<u64>, Vec<u64>));

    fn preprocess(vector: Borrowed<'_, Self>) -> Self::Lut {
        use crate::quantize;
        let vector = vector.slice();
        let dis_v_2 = S::reduce_sum_of_x2(vector);
        let (k, b, qvector) = quantize::quantize::<15>(S::vector_to_f32_borrowed(vector).as_ref());
        let qvector_sum = if vector.len() <= 4369 {
            quantize::reduce_sum_of_x_as_u16(&qvector) as f32
        } else {
            quantize::reduce_sum_of_x_as_u32(&qvector) as f32
        };
        let lut = binarize(&qvector);
        (dis_v_2, b, k, qvector_sum, lut)
    }

    fn process(
        lut: &Self::Lut,
        (_, factor_ppc, factor_ip, _, t): (f32, f32, f32, f32, &[u64]),
    ) -> Distance {
        let &(_, b, k, qvector_sum, ref s) = lut;
        let value = asymmetric_binary_dot_product(t, s) as u16;
        let rough =
            0.5 * b * factor_ppc + 0.5 * ((2.0 * value as f32) - qvector_sum) * factor_ip * k;
        Distance::from_f32(rough)
    }

    fn process_lowerbound(
        lut: &Self::Lut,
        (_, factor_ppc, factor_ip, factor_err, t): (f32, f32, f32, f32, &[u64]),
        epsilon: f32,
    ) -> Distance {
        let &(dis_v_2, b, k, qvector_sum, ref s) = lut;
        let value = asymmetric_binary_dot_product(t, s) as u16;
        let rough =
            0.5 * b * factor_ppc + 0.5 * ((2.0 * value as f32) - qvector_sum) * factor_ip * k;
        let err = 0.5 * factor_err * dis_v_2.sqrt();
        Distance::from_f32(rough - epsilon * err)
    }

    type FLut = (f32, f32, f32, f32, Vec<u8>);

    fn fscan_preprocess(vector: Borrowed<'_, Self>) -> Self::FLut {
        use crate::quantize;
        let vector = vector.slice();
        let dis_v_2 = S::reduce_sum_of_x2(vector);
        let (k, b, qvector) = quantize::quantize::<15>(S::vector_to_f32_borrowed(vector).as_ref());
        let qvector_sum = if vector.len() <= 4369 {
            quantize::reduce_sum_of_x_as_u16(&qvector) as f32
        } else {
            quantize::reduce_sum_of_x_as_u32(&qvector) as f32
        };
        let lut = gen(qvector);
        (dis_v_2, b, k, qvector_sum, lut)
    }

    fn fscan_process(
        dims: u32,
        lut: &Self::FLut,
        (_, factor_ppc, factor_ip, _, t): (&[f32; 32], &[f32; 32], &[f32; 32], &[f32; 32], &[u8]),
    ) -> [Distance; 32] {
        let &(_, b, k, qvector_sum, ref s) = lut;
        let r = fast_scan_b4(dims.div_ceil(4), t, s);
        std::array::from_fn(|i| {
            let rough = 0.5 * b * factor_ppc[i]
                + 0.5 * ((2.0 * r[i] as f32) - qvector_sum) * factor_ip[i] * k;
            Distance::from_f32(rough)
        })
    }

    fn fscan_process_lowerbound(
        dims: u32,
        lut: &Self::FLut,
        (_, factor_ppc, factor_ip, factor_err, t): (
            &[f32; 32],
            &[f32; 32],
            &[f32; 32],
            &[f32; 32],
            &[u8],
        ),
        epsilon: f32,
    ) -> [Distance; 32] {
        let &(dis_v_2, b, k, qvector_sum, ref s) = lut;
        let r = fast_scan_b4(dims.div_ceil(4), t, s);
        std::array::from_fn(|i| {
            let rough = 0.5 * b * factor_ppc[i]
                + 0.5 * ((2.0 * r[i] as f32) - qvector_sum) * factor_ip[i] * k;
            let err = 0.5 * factor_err[i] * dis_v_2.sqrt();
            Distance::from_f32(rough - epsilon * err)
        })
    }
}

macro_rules! unimpl_operator_rabitq_quantization {
    ($t:ty) => {
        impl OperatorRabitqQuantization for $t {
            type Scalar = Impossible;

            fn code(_: Borrowed<'_, Self>) -> (f32, f32, f32, f32, Vec<u8>) {
                unimplemented!()
            }

            fn project(_: &[Vec<Self::Scalar>], _: Borrowed<'_, Self>) -> Self::Vector {
                unimplemented!()
            }

            type Lut = std::convert::Infallible;
            fn preprocess(_: Borrowed<'_, Self>) -> Self::Lut {
                unimplemented!()
            }
            fn process(_: &Self::Lut, _: (f32, f32, f32, f32, &[u64])) -> Distance {
                unimplemented!()
            }
            fn process_lowerbound(
                _: &Self::Lut,
                _: (f32, f32, f32, f32, &[u64]),
                _: f32,
            ) -> Distance {
                unimplemented!()
            }

            type FLut = std::convert::Infallible;
            fn fscan_preprocess(_: Borrowed<'_, Self>) -> Self::FLut {
                unimplemented!()
            }
            fn fscan_process(
                _: u32,
                _: &Self::Lut,
                _: (&[f32; 32], &[f32; 32], &[f32; 32], &[f32; 32], &[u8]),
            ) -> [Distance; 32] {
                unimplemented!()
            }
            fn fscan_process_lowerbound(
                _: u32,
                _: &Self::Lut,
                _: (&[f32; 32], &[f32; 32], &[f32; 32], &[f32; 32], &[u8]),
                _: f32,
            ) -> [Distance; 32] {
                unimplemented!()
            }
        }
    };
}

unimpl_operator_rabitq_quantization!(BVectorDot);
unimpl_operator_rabitq_quantization!(BVectorHamming);
unimpl_operator_rabitq_quantization!(BVectorJaccard);

unimpl_operator_rabitq_quantization!(SVectDot<f32>);
unimpl_operator_rabitq_quantization!(SVectL2<f32>);

fn parse_code(code: &[u8]) -> (f32, f32, f32, f32, &[u64]) {
    assert!(code.len() > size_of::<f32>() * 4, "length is incorrect");
    assert!(code.len() % size_of::<u64>() == 0, "length is incorrect");
    assert!(code.as_ptr() as usize % 8 == 0, "pointer is not aligned");
    unsafe {
        let a = code.as_ptr().add(0).cast::<f32>().read();
        let b = code.as_ptr().add(4).cast::<f32>().read();
        let c = code.as_ptr().add(8).cast::<f32>().read();
        let d = code.as_ptr().add(12).cast::<f32>().read();
        let e = std::slice::from_raw_parts(code[16..].as_ptr().cast(), code[16..].len() / 8);
        (a, b, c, d, e)
    }
}

fn parses_codes(code: &[u8]) -> (&[f32; 32], &[f32; 32], &[f32; 32], &[f32; 32], &[u8]) {
    assert!(code.len() > size_of::<f32>() * 128, "length is incorrect");
    assert!(code.as_ptr() as usize % 4 == 0, "pointer is not aligned");
    unsafe {
        let a = &*code.as_ptr().add(0).cast::<[f32; 32]>();
        let b = &*code.as_ptr().add(128).cast::<[f32; 32]>();
        let c = &*code.as_ptr().add(256).cast::<[f32; 32]>();
        let d = &*code.as_ptr().add(384).cast::<[f32; 32]>();
        let e = &code[512..];
        (a, b, c, d, e)
    }
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

fn binarize(vector: &[u8]) -> (Vec<u64>, Vec<u64>, Vec<u64>, Vec<u64>) {
    let n = vector.len();
    let mut t0 = vec![0u64; n.div_ceil(64)];
    let mut t1 = vec![0u64; n.div_ceil(64)];
    let mut t2 = vec![0u64; n.div_ceil(64)];
    let mut t3 = vec![0u64; n.div_ceil(64)];
    for i in 0..n {
        t0[i / 64] |= (((vector[i] >> 0) & 1) as u64) << (i % 64);
        t1[i / 64] |= (((vector[i] >> 1) & 1) as u64) << (i % 64);
        t2[i / 64] |= (((vector[i] >> 2) & 1) as u64) << (i % 64);
        t3[i / 64] |= (((vector[i] >> 3) & 1) as u64) << (i % 64);
    }
    (t0, t1, t2, t3)
}

#[detect::multiversion(v2, fallback)]
fn asymmetric_binary_dot_product(x: &[u64], y: &(Vec<u64>, Vec<u64>, Vec<u64>, Vec<u64>)) -> u32 {
    assert_eq!(x.len(), y.0.len());
    assert_eq!(x.len(), y.1.len());
    assert_eq!(x.len(), y.2.len());
    assert_eq!(x.len(), y.3.len());
    let n = x.len();
    let (mut t0, mut t1, mut t2, mut t3) = (0, 0, 0, 0);
    for i in 0..n {
        t0 += (x[i] & y.0[i]).count_ones();
    }
    for i in 0..n {
        t1 += (x[i] & y.1[i]).count_ones();
    }
    for i in 0..n {
        t2 += (x[i] & y.2[i]).count_ones();
    }
    for i in 0..n {
        t3 += (x[i] & y.3[i]).count_ones();
    }
    (t0 << 0) + (t1 << 1) + (t2 << 2) + (t3 << 3)
}
