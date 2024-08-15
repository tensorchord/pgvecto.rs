use super::error_based::ErrorBasedFlatReranker;
use crate::operator::OperatorRabitq;
use base::always_equal::AlwaysEqual;
use base::index::VectorOptions;
use base::scalar::F32;
use base::search::RerankerPop;
use num_traits::Float;
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
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

    pub fn encode(&self, vector: &[F32]) -> (F32, F32, F32, F32, Vec<u8>) {
        let dis_u = vector.iter().map(|&x| x * x).sum::<F32>().sqrt();
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
        lhs: &[F32],
    ) -> (O::QuantizationPreprocessed0, O::QuantizationPreprocessed1) {
        O::rabitq_quantization_preprocess(lhs)
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
    ) -> F32 {
        let (est, err) = O::rabitq_quantization_process(a, b, c, d, e, p0, p1);
        est - err * F32(1.9)
    }

    pub fn push_batch(
        &self,
        (p0, p1): &(O::QuantizationPreprocessed0, O::QuantizationPreprocessed1),
        rhs: Range<u32>,
        heap: &mut Vec<(Reverse<F32>, AlwaysEqual<u32>)>,
        result: &mut BinaryHeap<(F32, AlwaysEqual<u32>, ())>,
        rerank: impl Fn(u32) -> (F32, ()),
        codes: &[u8],
        packed_codes: &[u8],
        meta_a: &[F32],
        meta_b: &[F32],
        meta_c: &[F32],
        meta_d: &[F32],
        fast_scan: bool,
    ) {
        if fast_scan && O::SUPPORT_FAST_SCAN && quantization::fast_scan::b4::is_supported() {
            use quantization::fast_scan::b4::{fast_scan, BLOCK_SIZE};
            let lut = O::fast_scan(p1);
            let s = rhs.start.next_multiple_of(BLOCK_SIZE);
            let e = (rhs.end + 1 - BLOCK_SIZE).next_multiple_of(BLOCK_SIZE);
            if rhs.start != s {
                let i = rhs.start / BLOCK_SIZE * BLOCK_SIZE;
                let t = self.dims.div_ceil(4);
                let bytes = (t * 16) as usize;
                let start = (i / BLOCK_SIZE) as usize * bytes;
                let end = start + bytes;
                let res = fast_scan(t, &packed_codes[start..end], &lut);
                heap.extend((rhs.start..s).map(|u| {
                    (
                        Reverse({
                            let a = meta_a[u as usize];
                            let b = meta_b[u as usize];
                            let c = meta_c[u as usize];
                            let d = meta_d[u as usize];
                            let param = res[(u - i) as usize];
                            let (est, err) =
                                O::rabitq_quantization_process_1(a, b, c, d, p0, param);
                            est - err * 1.9
                        }),
                        AlwaysEqual(u),
                    )
                }));
                (rhs.start..s)
                    .map(|u| {
                        (
                            Reverse({
                                let a = meta_a[u as usize];
                                let b = meta_b[u as usize];
                                let c = meta_c[u as usize];
                                let d = meta_d[u as usize];
                                let param = res[(u - i) as usize];
                                let (est, err) =
                                    O::rabitq_quantization_process_1(a, b, c, d, p0, param);
                                est - err * 1.9
                            }),
                            AlwaysEqual(u),
                        )
                    })
                    .for_each(|(Reverse(low_u), AlwaysEqual(u))| {
                        if result.len() < 10 || low_u < result.peek().unwrap().0 {
                            let (dis_u, ()) = rerank(u);
                            result.push((dis_u, AlwaysEqual(u), ()));
                        }
                        while result.len() > 10 {
                            result.pop();
                        }
                    });
            }
            for i in (s..e).step_by(BLOCK_SIZE as _) {
                let t = self.dims.div_ceil(4);
                let bytes = (t * 16) as usize;
                let start = (i / BLOCK_SIZE) as usize * bytes;
                let end = start + bytes;
                let res = fast_scan(t, &packed_codes[start..end], &lut);
                let meta_a = &meta_a[i as usize..][..BLOCK_SIZE as usize];
                let meta_b = &meta_b[i as usize..][..BLOCK_SIZE as usize];
                let meta_c = &meta_c[i as usize..][..BLOCK_SIZE as usize];
                let meta_d = &meta_d[i as usize..][..BLOCK_SIZE as usize];
                let temp = O::rabitq_quantization_process_1_parallel(
                    meta_a.try_into().unwrap(),
                    meta_b.try_into().unwrap(),
                    meta_c.try_into().unwrap(),
                    meta_d.try_into().unwrap(),
                    p0,
                    &res,
                );
                (0..BLOCK_SIZE)
                    .map(|index| (Reverse(temp[index as usize]), AlwaysEqual(i + index)))
                    .for_each(|(Reverse(low_u), AlwaysEqual(u))| {
                        if result.len() < 10 || low_u < result.peek().unwrap().0 {
                            let (dis_u, ()) = rerank(u);
                            result.push((dis_u, AlwaysEqual(u), ()));
                        }
                        while result.len() > 10 {
                            result.pop();
                        }
                    });
            }
            if e != rhs.end {
                let i = e / BLOCK_SIZE * BLOCK_SIZE;
                let t = self.dims.div_ceil(4);
                let bytes = (t * 16) as usize;
                let start = (i / BLOCK_SIZE) as usize * bytes;
                let end = start + bytes;
                let res = fast_scan(t, &packed_codes[start..end], &lut);
                (e..rhs.end)
                    .map(|u| {
                        (
                            Reverse({
                                let a = meta_a[u as usize];
                                let b = meta_b[u as usize];
                                let c = meta_c[u as usize];
                                let d = meta_d[u as usize];
                                let param = res[(u - i) as usize];
                                let (est, err) =
                                    O::rabitq_quantization_process_1(a, b, c, d, p0, param);
                                est - err * 1.9
                            }),
                            AlwaysEqual(u),
                        )
                    })
                    .for_each(|(Reverse(low_u), AlwaysEqual(u))| {
                        if result.len() < 10 || low_u < result.peek().unwrap().0 {
                            let (dis_u, ()) = rerank(u);
                            result.push((dis_u, AlwaysEqual(u), ()));
                        }
                        while result.len() > 10 {
                            result.pop();
                        }
                    });
            }
            return;
        }
        heap.extend(rhs.map(|u| {
            (
                Reverse(self.process_lowerbound(p0, p1, {
                    let bytes = self.bytes() as usize;
                    let start = u as usize * bytes;
                    let end = start + bytes;
                    let a = meta_a[u as usize];
                    let b = meta_b[u as usize];
                    let c = meta_c[u as usize];
                    let d = meta_d[u as usize];
                    (a, b, c, d, &codes[start..end])
                })),
                AlwaysEqual(u),
            )
        }));
    }

    pub fn rerank<'a, T: 'a>(
        &'a self,
        heap: Vec<(Reverse<F32>, AlwaysEqual<u32>)>,
        r: impl Fn(u32) -> (F32, T) + 'a,
    ) -> impl RerankerPop<T> + 'a {
        ErrorBasedFlatReranker::new(heap, r)
    }
}
