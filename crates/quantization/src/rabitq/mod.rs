use self::operator::OperatorRabitq;
use crate::reranker::error::ErrorFlatReranker;
use crate::reranker::window_0::Window0GraphReranker;
use base::index::{RabitqQuantizationOptions, VectorOptions};
use base::operator::{Borrowed, Owned};
use base::scalar::F32;
use base::search::{RerankerPop, RerankerPush, Vectors};
use base::vector::VectorBorrowed;
use num_traits::Float;
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::marker::PhantomData;
use std::ops::Range;

pub mod operator;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct RabitqQuantizer<O: OperatorRabitq> {
    dims: u32,
    projection: Vec<Vec<F32>>,
    _maker: PhantomData<fn(O) -> O>,
}

impl<O: OperatorRabitq> RabitqQuantizer<O> {
    pub fn train(
        vector_options: VectorOptions,
        _: RabitqQuantizationOptions,
        _: &impl Vectors<O>,
        _: impl Fn(Borrowed<'_, O>) -> Owned<O> + Copy + Send + Sync,
    ) -> Self {
        use nalgebra::debug::RandomOrthogonal;
        use nalgebra::{Dim, Dyn};
        let dims = vector_options.dims;
        let projection =
            RandomOrthogonal::<f32, Dyn>::new(Dim::from_usize(dims as _), rand::random)
                .unwrap()
                .row_iter()
                .map(|r| r.iter().map(|&x| F32(x)).collect())
                .collect();
        Self {
            dims,
            projection,
            _maker: PhantomData,
        }
    }

    pub fn bits(&self) -> u32 {
        1
    }

    pub fn bytes(&self) -> u32 {
        self.dims.div_ceil(8)
    }

    pub fn dims(&self) -> u32 {
        self.dims
    }

    pub fn width(&self) -> u32 {
        self.dims
    }

    pub fn encode(&self, vector: Borrowed<'_, O>) -> (F32, F32, F32, F32, Vec<u8>) {
        let dis_u = vector.length();
        let vector = O::proj(&self.projection, vector);
        let sum_of_abs_x = vector.iter().map(|x| x.abs()).sum::<F32>();
        let sum_of_x_2 = vector.iter().map(|&x| x * x).sum::<F32>();
        let x0 = sum_of_abs_x / (sum_of_x_2 * F32(self.dims as _)).sqrt();
        let x_x0 = dis_u / x0;
        let fac_norm = F32(self.dims as f32).sqrt();
        let max_x1 = F32(1.0) / F32((self.dims as f32 - 1.0).sqrt());
        let factor_err = F32(2.0) * max_x1 * (x_x0 * x_x0 - dis_u * dis_u).sqrt();
        let factor_ip = F32(-2.0) / fac_norm * x_x0;
        let factor_ppc = factor_ip * vector.iter().map(|x| x.signum()).sum::<F32>();
        let mut codes = Vec::new();
        for i in 0..self.dims {
            codes.push(vector[i as usize].is_sign_positive() as u8);
        }
        (dis_u * dis_u, factor_ppc, factor_ip, factor_err, codes)
    }

    pub fn preprocess(
        &self,
        lhs: Borrowed<'_, O>,
    ) -> (O::QuantizationPreprocessed0, O::QuantizationPreprocessed1) {
        O::rabitq_quantization_preprocess(lhs, &self.projection)
    }

    pub fn process(
        &self,
        p0: &O::QuantizationPreprocessed0,
        p1: &O::QuantizationPreprocessed1,
        (a, b, c, d, e): (F32, F32, F32, F32, &[u8]),
    ) -> F32 {
        let (est, _) = O::rabitq_quantization_process(a, b, c, d, e, p0, p1);
        est
    }

    pub fn process_lowerbound(
        &self,
        p0: &O::QuantizationPreprocessed0,
        p1: &O::QuantizationPreprocessed1,
        (a, b, c, d, e): (F32, F32, F32, F32, &[u8]),
        epsilon: F32,
    ) -> F32 {
        let (est, err) = O::rabitq_quantization_process(a, b, c, d, e, p0, p1);
        est - err * epsilon
    }

    pub fn push_batch(
        &self,
        (p0, p1): &(O::QuantizationPreprocessed0, O::QuantizationPreprocessed1),
        rhs: Range<u32>,
        heap: &mut Vec<(Reverse<F32>, u32)>,
        codes: &[u8],
        packed_codes: &[u8],
        meta: &[F32],
        epsilon: F32,
        fast_scan: bool,
    ) {
        if fast_scan && O::SUPPORT_FAST_SCAN && crate::fast_scan::b4::is_supported() {
            use crate::fast_scan::b4::{fast_scan, BLOCK_SIZE};
            let s = rhs.start.next_multiple_of(BLOCK_SIZE);
            let e = (rhs.end + 1 - BLOCK_SIZE).next_multiple_of(BLOCK_SIZE);
            heap.extend((rhs.start..s).map(|u| {
                (
                    Reverse(self.process_lowerbound(
                        p0,
                        p1,
                        {
                            let bytes = self.bytes() as usize;
                            let start = u as usize * bytes;
                            let end = start + bytes;
                            let a = meta[4 * u as usize + 0];
                            let b = meta[4 * u as usize + 1];
                            let c = meta[4 * u as usize + 2];
                            let d = meta[4 * u as usize + 3];
                            (a, b, c, d, &codes[start..end])
                        },
                        epsilon,
                    )),
                    u,
                )
            }));
            let lut = O::fast_scan(p1);
            for i in (s..e).step_by(BLOCK_SIZE as _) {
                let t = self.dims.div_ceil(4);
                let bytes = (t * 16) as usize;
                let start = (i / BLOCK_SIZE) as usize * bytes;
                let end = start + bytes;
                heap.extend({
                    let res = fast_scan(t, &packed_codes[start..end], &lut);
                    (i..i + BLOCK_SIZE)
                        .map(|u| {
                            (
                                Reverse({
                                    let a = meta[4 * u as usize + 0];
                                    let b = meta[4 * u as usize + 1];
                                    let c = meta[4 * u as usize + 2];
                                    let d = meta[4 * u as usize + 3];
                                    let param = res[(u - i) as usize];
                                    let (est, err) =
                                        O::rabitq_quantization_process_1(a, b, c, d, p0, param);
                                    est - err * epsilon
                                }),
                                u,
                            )
                        })
                        .collect::<Vec<_>>()
                });
            }
            heap.extend((e..rhs.end).map(|u| {
                (
                    Reverse(self.process_lowerbound(
                        p0,
                        p1,
                        {
                            let bytes = self.bytes() as usize;
                            let start = u as usize * bytes;
                            let end = start + bytes;
                            let a = meta[4 * u as usize + 0];
                            let b = meta[4 * u as usize + 1];
                            let c = meta[4 * u as usize + 2];
                            let d = meta[4 * u as usize + 3];
                            (a, b, c, d, &codes[start..end])
                        },
                        epsilon,
                    )),
                    u,
                )
            }));
            return;
        }
        heap.extend(rhs.map(|u| {
            (
                Reverse(self.process_lowerbound(
                    p0,
                    p1,
                    {
                        let bytes = self.bytes() as usize;
                        let start = u as usize * bytes;
                        let end = start + bytes;
                        let a = meta[4 * u as usize + 0];
                        let b = meta[4 * u as usize + 1];
                        let c = meta[4 * u as usize + 2];
                        let d = meta[4 * u as usize + 3];
                        (a, b, c, d, &codes[start..end])
                    },
                    epsilon,
                )),
                u,
            )
        }));
    }

    pub fn flat_rerank<'a, T: 'a>(
        &'a self,
        heap: Vec<(Reverse<F32>, u32)>,
        r: impl Fn(u32) -> (F32, T) + 'a,
    ) -> impl RerankerPop<T> + 'a {
        ErrorFlatReranker::new(heap, r)
    }

    pub fn graph_rerank<'a, T: 'a, C: Fn(u32) -> (F32, F32, F32, F32, &'a [u8]) + 'a>(
        &'a self,
        vector: Borrowed<'a, O>,
        c: C,
        r: impl Fn(u32) -> (F32, T) + 'a,
    ) -> impl RerankerPop<T> + RerankerPush + 'a {
        let (p0, p1) = O::rabitq_quantization_preprocess(vector, &self.projection);
        Window0GraphReranker::new(move |u| self.process(&p0, &p1, c(u)), r)
    }
}
