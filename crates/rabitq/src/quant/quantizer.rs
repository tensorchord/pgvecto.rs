use super::error::ErrorFlatReranker;
use crate::operator::OperatorRabitq;
use base::always_equal::AlwaysEqual;
use base::distance::Distance;
use base::index::VectorOptions;
use base::search::RerankerPop;
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::marker::PhantomData;
use std::ops::Range;

pub enum RabitqLookup<O: OperatorRabitq> {
    FastScan(Vec<u8>),
    Trivial(O::QvectorLookup),
}

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

    pub fn encode(&self, vector: &[f32]) -> Vec<u8> {
        let mut codes = Vec::new();
        for i in 0..self.dims {
            codes.push(vector[i as usize].is_sign_positive() as u8);
        }
        codes
    }

    pub fn preprocess(
        &self,
        trans_vector: &[f32],
        centroid_dot_dis: f32,
        original_square: f32,
        centroids_square: f32,
    ) -> (O::QvectorParams, O::QvectorLookup) {
        O::preprocess(
            trans_vector,
            centroid_dot_dis,
            original_square,
            centroids_square,
        )
    }

    pub fn fscan_preprocess(
        &self,
        trans_vector: &[f32],
        centroid_dot_dis: f32,
        original_square: f32,
        centroids_square: f32,
    ) -> (O::QvectorParams, Vec<u8>) {
        O::fscan_preprocess(
            trans_vector,
            centroid_dot_dis,
            original_square,
            centroids_square,
        )
    }

    pub fn process(
        &self,
        vector_params: &O::VectorParams,
        qvector_params: &O::QvectorParams,
        qvector_lookup: &O::QvectorLookup,
        qvector_code: &[u8],
    ) -> Distance {
        O::process(vector_params, qvector_code, qvector_params, qvector_lookup)
    }

    pub fn process_lowerbound(
        &self,
        vector_params: &O::VectorParams,
        qvector_params: &O::QvectorParams,
        qvector_lookup: &O::QvectorLookup,
        qvector_code: &[u8],
        epsilon: f32,
    ) -> Distance {
        O::process_lowerbound(
            vector_params,
            qvector_code,
            qvector_params,
            qvector_lookup,
            epsilon,
        )
    }

    pub fn push_batch(
        &self,
        alpha: &O::Params,
        beta: &Result<O::Preprocessed, Vec<u8>>,
        range: Range<u32>,
        heap: &mut Vec<(Reverse<Distance>, AlwaysEqual<u32>)>,
        codes: &[u8],
        packed_codes: &[u8],
        meta: &[f32],
        epsilon: f32,
    ) {
        match lookup {
            RabitqLookup::FastScan(lut) => {
                self.push_back_fscan(qvector_params, &lut, range, heap, packed_codes, meta, epsilon);
            }
            RabitqLookup::Trivial(blut) => {
                self.push_back_trivial(qvector_params, blut, range, heap, codes, meta, epsilon);
            }
        }
    }

    #[inline]
    fn push_back_fscan(
        &self,
        qvector_params: &O::QvectorParams,
        lut: &[u8],
        rhs: Range<u32>,
        heap: &mut Vec<(Reverse<Distance>, AlwaysEqual<u32>)>,
        packed_codes: &[u8],
        meta: &[f32],
        epsilon: f32,
    ) {
        use quantization::fast_scan::b4::{fast_scan_b4, BLOCK_SIZE};
        let s = rhs.start.next_multiple_of(BLOCK_SIZE);
        let e = (rhs.end + 1 - BLOCK_SIZE).next_multiple_of(BLOCK_SIZE);
        if rhs.start != s {
            let i = s - BLOCK_SIZE;
            let t = self.dims.div_ceil(4);
            let bytes = (t * 16) as usize;
            let start = (i / BLOCK_SIZE) as usize * bytes;
            let end = start + bytes;
            let all_binary_product = fast_scan_b4(t, &packed_codes[start..end], lut);
            heap.extend({
                (rhs.start..s).map(|u| {
                    (
                        Reverse({
                            let params = &O::train_decode(u, meta);
                            let binary_prod = all_binary_product[(u - i) as usize];
                            O::fscan_process_lowerbound(
                                params,
                                qvector_params,
                                binary_prod,
                                epsilon,
                            )
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
            let all_binary_product = fast_scan_b4(t, &packed_codes[start..end], lut);
            heap.extend({
                (i..i + BLOCK_SIZE).map(|u| {
                    (
                        Reverse({
                            let params = &O::train_decode(u, meta);
                            let binary_prod = all_binary_product[(u - i) as usize];
                            O::fscan_process_lowerbound(
                                params,
                                qvector_params,
                                binary_prod,
                                epsilon,
                            )
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
            let all_binary_product = fast_scan_b4(t, &packed_codes[start..end], lut);
            heap.extend({
                (e..rhs.end).map(|u| {
                    (
                        Reverse({
                            let params = &O::train_decode(u, meta);
                            let binary_prod = all_binary_product[(u - i) as usize];
                            O::fscan_process_lowerbound(
                                params,
                                qvector_params,
                                binary_prod,
                                epsilon,
                            )
                        }),
                        AlwaysEqual(u),
                    )
                })
            });
        }
    }

    #[inline]
    fn push_back_trivial(
        &self,
        qvector_params: &O::QvectorParams,
        qvector_lookup: &O::QvectorLookup,
        rhs: Range<u32>,
        heap: &mut Vec<(Reverse<Distance>, AlwaysEqual<u32>)>,
        codes: &[u8],
        meta: &[f32],
        epsilon: f32,
    ) {
        heap.extend(rhs.map(|u| {
            (
                Reverse(self.process_lowerbound(
                    &O::train_decode(u, meta),
                    qvector_params,
                    qvector_lookup,
                    {
                        let bytes = self.bytes() as usize;
                        let start = u as usize * bytes;
                        let end = start + bytes;
                        &codes[start..end]
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
        rerank: impl Fn(u32) -> (Distance, T) + 'a,
    ) -> impl RerankerPop<T> + 'a {
        ErrorFlatReranker::new(heap, rerank)
    }
}
