use super::error_based::ErrorBasedReranker;
use crate::operator::OperatorRabitq;
use base::always_equal::AlwaysEqual;
use base::distance::Distance;
use base::index::VectorOptions;
use base::search::RerankerPop;
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::marker::PhantomData;
use std::ops::Range;

pub const EPSILON: f32 = 1.9;
pub const THETA_LOG_DIM: u32 = 4;
pub const DEFAULT_X_DOT_PRODUCT: f32 = 0.8;

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
        let dis_u = vector.iter().map(|&x| x * x).sum::<f32>().sqrt();
        let sum_of_abs_x = vector.iter().map(|x| x.abs()).sum::<f32>();
        let sum_of_x_2 = vector.iter().map(|&x| x * x).sum::<f32>();
        let x0 = sum_of_abs_x / (sum_of_x_2 * (self.dims as f32)).sqrt();
        let x_x0 = dis_u / x0;
        let fac_norm = (self.dims as f32).sqrt();
        let max_x1 = 1.0f32 / (self.dims as f32 - 1.0).sqrt();
        let factor_err = 2.0f32 * max_x1 * (x_x0 * x_x0 - dis_u * dis_u).sqrt();
        let factor_ip = -2.0f32 / fac_norm * x_x0;
        let factor_ppc = factor_ip * vector.iter().map(|x| x.signum()).sum::<f32>();
        let mut codes = Vec::new();
        for i in 0..self.dims {
            codes.push(vector[i as usize].is_sign_positive() as u8);
        }
        (dis_u * dis_u, factor_ppc, factor_ip, factor_err, codes)
    }

    pub fn preprocess(
        &self,
        lhs: &[f32],
    ) -> (O::QuantizationPreprocessed0, O::QuantizationPreprocessed1) {
        O::rabitq_quantization_preprocess(lhs)
    }

    pub fn process(
        &self,
        p0: &O::QuantizationPreprocessed0,
        p1: &O::QuantizationPreprocessed1,
        (a, b, c, d, e): (f32, f32, f32, f32, &[u8]),
    ) -> Distance {
        let (est, _) = O::rabitq_quantization_process(a, b, c, d, e, p0, p1);
        Distance::from(est)
    }

    pub fn process_lowerbound(
        &self,
        p0: &O::QuantizationPreprocessed0,
        p1: &O::QuantizationPreprocessed1,
        (a, b, c, d, e): (f32, f32, f32, f32, &[u8]),
        epsilon: f32,
    ) -> Distance {
        let (est, err) = O::rabitq_quantization_process(a, b, c, d, e, p0, p1);
        Distance::from(est - err * epsilon)
    }

    pub fn push_batch(
        &self,
        (p0, p1): &(O::QuantizationPreprocessed0, O::QuantizationPreprocessed1),
        rhs: Range<u32>,
        heap: &mut Vec<(Reverse<Distance>, AlwaysEqual<u32>)>,
        codes: &[u8],
        packed_codes: &[u8],
        meta: &[f32],
        epsilon: f32,
        fast_scan: bool,
    ) {
        if fast_scan && O::SUPPORT_FAST_SCAN && quantization::fast_scan::b4::is_supported() {
            use quantization::fast_scan::b4::{fast_scan, BLOCK_SIZE};
            let s = rhs.start.next_multiple_of(BLOCK_SIZE);
            let e = (rhs.end + 1 - BLOCK_SIZE).next_multiple_of(BLOCK_SIZE);
            let lut = O::fast_scan(p1);
            if rhs.start != s {
                let i = s - BLOCK_SIZE;
                let t = self.dims.div_ceil(4);
                let bytes = (t * 16) as usize;
                let start = (i / BLOCK_SIZE) as usize * bytes;
                let end = start + bytes;
                let res = fast_scan(t, &packed_codes[start..end], &lut);
                heap.extend({
                    (rhs.start..s).map(|u| {
                        (
                            Reverse({
                                let a = meta[4 * u as usize + 0];
                                let b = meta[4 * u as usize + 1];
                                let c = meta[4 * u as usize + 2];
                                let d = meta[4 * u as usize + 3];
                                let param = res[(u - i) as usize];
                                let (est, err) =
                                    O::rabitq_quantization_process_1(a, b, c, d, p0, param);
                                Distance::from(est - err * epsilon)
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
                let res = fast_scan(t, &packed_codes[start..end], &lut);
                heap.extend({
                    (i..i + BLOCK_SIZE).map(|u| {
                        (
                            Reverse({
                                let a = meta[4 * u as usize + 0];
                                let b = meta[4 * u as usize + 1];
                                let c = meta[4 * u as usize + 2];
                                let d = meta[4 * u as usize + 3];
                                let param = res[(u - i) as usize];
                                let (est, err) =
                                    O::rabitq_quantization_process_1(a, b, c, d, p0, param);
                                Distance::from(est - err * epsilon)
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
                let res = fast_scan(t, &packed_codes[start..end], &lut);
                heap.extend({
                    (e..rhs.end).map(|u| {
                        (
                            Reverse({
                                let a = meta[4 * u as usize + 0];
                                let b = meta[4 * u as usize + 1];
                                let c = meta[4 * u as usize + 2];
                                let d = meta[4 * u as usize + 3];
                                let param = res[(u - i) as usize];
                                let (est, err) =
                                    O::rabitq_quantization_process_1(a, b, c, d, p0, param);
                                Distance::from(est - err * epsilon)
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
        ErrorBasedReranker::new(heap, r)
    }
}
