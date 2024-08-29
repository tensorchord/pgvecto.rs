use super::error::ErrorFlatReranker;
use crate::operator::OperatorRabitq;
use base::always_equal::AlwaysEqual;
use base::distance::Distance;
use base::index::VectorOptions;
use base::scalar::ScalarLike;
use base::search::RerankerPop;
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::marker::PhantomData;
use std::ops::Range;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct RabitqQuantizer<O: OperatorRabitq> {
    dims: u32,
    _maker: PhantomData<fn(O) -> O>,
}

impl<O: OperatorRabitq> RabitqQuantizer<O> {
    pub fn train(vector_options: VectorOptions) -> Self {
        let dims = vector_options.dims;
        Self {
            dims,
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

    pub fn encode(&self, vector: &[f32]) -> (f32, f32, f32, f32, Vec<u8>) {
        // FIXME: add manually-implemented SIMD version
        let sum_of_abs_x = vector.iter().map(|x| x.abs()).sum::<f32>();
        let sum_of_x_2 = f32::reduce_sum_of_x2(vector);
        let dis_u = sum_of_x_2.sqrt();
        let x0 = sum_of_abs_x / (sum_of_x_2 * (self.dims as f32)).sqrt();
        let x_x0 = dis_u / x0;
        let fac_norm = (self.dims as f32).sqrt();
        let max_x1 = 1.0f32 / (self.dims as f32 - 1.0).sqrt();
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
        let mut codes = Vec::new();
        for i in 0..self.dims {
            codes.push(vector[i as usize].is_sign_positive() as u8);
        }
        (dis_u * dis_u, factor_ppc, factor_ip, factor_err, codes)
    }

    pub fn preprocess(&self, lhs: &[f32]) -> (O::Preprocessed0, O::Preprocessed1) {
        O::preprocess(lhs)
    }

    pub fn process(
        &self,
        p0: &O::Preprocessed0,
        p1: &O::Preprocessed1,
        (a, b, c, d, e): (f32, f32, f32, f32, &[u8]),
    ) -> Distance {
        O::process(a, b, c, d, e, p0, p1)
    }

    pub fn process_lowerbound(
        &self,
        p0: &O::Preprocessed0,
        p1: &O::Preprocessed1,
        (a, b, c, d, e): (f32, f32, f32, f32, &[u8]),
        epsilon: f32,
    ) -> Distance {
        O::process_lowerbound(a, b, c, d, e, p0, p1, epsilon)
    }

    pub fn push_batch(
        &self,
        (p0, p1): &(O::Preprocessed0, O::Preprocessed1),
        rhs: Range<u32>,
        heap: &mut Vec<(Reverse<Distance>, AlwaysEqual<u32>)>,
        codes: &[u8],
        packed_codes: &[u8],
        meta: &[f32],
        epsilon: f32,
        fast_scan: bool,
    ) {
        if fast_scan {
            use quantization::fast_scan::b4::{fast_scan_b4, BLOCK_SIZE};
            let s = rhs.start.next_multiple_of(BLOCK_SIZE);
            let e = (rhs.end + 1 - BLOCK_SIZE).next_multiple_of(BLOCK_SIZE);
            let lut = O::fscan_preprocess(p1);
            if rhs.start != s {
                let i = s - BLOCK_SIZE;
                let t = self.dims.div_ceil(4);
                let bytes = (t * 16) as usize;
                let start = (i / BLOCK_SIZE) as usize * bytes;
                let end = start + bytes;
                let res = fast_scan_b4(t, &packed_codes[start..end], &lut);
                heap.extend({
                    (rhs.start..s).map(|u| {
                        (
                            Reverse({
                                let a = meta[4 * u as usize + 0];
                                let b = meta[4 * u as usize + 1];
                                let c = meta[4 * u as usize + 2];
                                let d = meta[4 * u as usize + 3];
                                let param = res[(u - i) as usize];
                                O::fscan_process_lowerbound(a, b, c, d, p0, param, epsilon)
                            }),
                            AlwaysEqual(u),
                        )
                    })
                });
            }
            for i in (s..e).step_by(BLOCK_SIZE as _) {
                let t = self.dims.div_ceil(4);
                let bytes = (t * 16) as usize;
                let start = (i / BLOCK_SIZE) as usize * bytes;
                let end = start + bytes;
                let res = fast_scan_b4(t, &packed_codes[start..end], &lut);
                heap.extend({
                    (i..i + BLOCK_SIZE).map(|u| {
                        (
                            Reverse({
                                let a = meta[4 * u as usize + 0];
                                let b = meta[4 * u as usize + 1];
                                let c = meta[4 * u as usize + 2];
                                let d = meta[4 * u as usize + 3];
                                let param = res[(u - i) as usize];
                                O::fscan_process_lowerbound(a, b, c, d, p0, param, epsilon)
                            }),
                            AlwaysEqual(u),
                        )
                    })
                });
            }
            if e != rhs.end {
                let i = e;
                let t = self.dims.div_ceil(4);
                let bytes = (t * 16) as usize;
                let start = (i / BLOCK_SIZE) as usize * bytes;
                let end = start + bytes;
                let res = fast_scan_b4(t, &packed_codes[start..end], &lut);
                heap.extend({
                    (e..rhs.end).map(|u| {
                        (
                            Reverse({
                                let a = meta[4 * u as usize + 0];
                                let b = meta[4 * u as usize + 1];
                                let c = meta[4 * u as usize + 2];
                                let d = meta[4 * u as usize + 3];
                                let param = res[(u - i) as usize];
                                O::fscan_process_lowerbound(a, b, c, d, p0, param, epsilon)
                            }),
                            AlwaysEqual(u),
                        )
                    })
                });
            }
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
                AlwaysEqual(u),
            )
        }));
    }

    pub fn rerank<'a, T: 'a>(
        &'a self,
        heap: Vec<(Reverse<Distance>, AlwaysEqual<u32>)>,
        r: impl Fn(u32) -> (Distance, T) + 'a,
    ) -> impl RerankerPop<T> + 'a {
        ErrorFlatReranker::new(heap, r)
    }
}
