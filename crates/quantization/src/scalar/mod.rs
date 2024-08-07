pub mod operator;

use self::operator::OperatorScalarQuantization;
use crate::reranker::window::WindowFlatReranker;
use crate::reranker::window_0::Window0GraphReranker;
use base::index::*;
use base::operator::*;
use base::scalar::*;
use base::search::RerankerPop;
use base::search::RerankerPush;
use base::search::Vectors;
use base::vector::*;
use common::vec2::Vec2;
use num_traits::Float;
use serde::Deserialize;
use serde::Serialize;
use std::cmp::Reverse;
use std::ops::Range;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct ScalarQuantizer<O: OperatorScalarQuantization> {
    dims: u32,
    bits: u32,
    max: Vec<Scalar<O>>,
    min: Vec<Scalar<O>>,
    centroids: Vec2<Scalar<O>>,
}

impl<O: OperatorScalarQuantization> ScalarQuantizer<O> {
    pub fn train(
        vector_options: VectorOptions,
        scalar_quantization_options: ScalarQuantizationOptions,
        vectors: &impl Vectors<O>,
        transform: impl Fn(Borrowed<'_, O>) -> Owned<O> + Copy,
    ) -> Self {
        let dims = vector_options.dims;
        let bits = scalar_quantization_options.bits;
        let mut max = vec![Scalar::<O>::neg_infinity(); dims as usize];
        let mut min = vec![Scalar::<O>::infinity(); dims as usize];
        let n = vectors.len();
        for i in 0..n {
            let vector = transform(vectors.vector(i)).as_borrowed().to_vec();
            for j in 0..dims as usize {
                max[j] = std::cmp::max(max[j], vector[j]);
                min[j] = std::cmp::min(min[j], vector[j]);
            }
        }
        let mut centroids = Vec2::zeros((1 << bits, dims as usize));
        for p in 0..dims {
            let bas = min[p as usize];
            let del = max[p as usize] - min[p as usize];
            for j in 0_usize..(1 << bits) {
                let val = Scalar::<O>::from_f(F32(j as f32 / ((1 << bits) - 1) as f32));
                centroids[(j, p as usize)] = bas + val * del;
            }
        }
        Self {
            dims,
            bits,
            max,
            min,
            centroids,
        }
    }

    pub fn bits(&self) -> u32 {
        self.bits
    }

    pub fn bytes(&self) -> u32 {
        (self.dims * self.bits).div_ceil(8)
    }

    pub fn width(&self) -> u32 {
        self.dims
    }

    pub fn encode(&self, vector: &[Scalar<O>]) -> Vec<u8> {
        let dims = self.dims;
        let bits = self.bits;
        let mut codes = Vec::with_capacity(dims as usize);
        for i in 0..dims as usize {
            let del = self.max[i] - self.min[i];
            let w =
                (((vector[i] - self.min[i]) / del).to_f32() * (((1 << bits) - 1) as f32)) as u32;
            codes.push(w.clamp(0, 255) as u8);
        }
        codes
    }

    pub fn preprocess(&self, lhs: Borrowed<'_, O>) -> O::QuantizationPreprocessed {
        O::scalar_quantization_preprocess(self.dims, self.bits, &self.max, &self.min, lhs)
    }

    pub fn process(&self, preprocessed: &O::QuantizationPreprocessed, rhs: &[u8]) -> F32 {
        let dims = self.dims;
        match self.bits {
            1 => O::quantization_process(dims, 1, 1, preprocessed, |i| {
                ((rhs[i >> 3] >> ((i & 7) << 0)) & 1) as usize
            }),
            2 => O::quantization_process(dims, 1, 2, preprocessed, |i| {
                ((rhs[i >> 2] >> ((i & 3) << 1)) & 3) as usize
            }),
            4 => O::quantization_process(dims, 1, 4, preprocessed, |i| {
                ((rhs[i >> 1] >> ((i & 1) << 2)) & 15) as usize
            }),
            8 => O::quantization_process(dims, 1, 8, preprocessed, |i| rhs[i] as usize),
            _ => unreachable!(),
        }
    }

    pub fn push_batch(
        &self,
        preprocessed: &O::QuantizationPreprocessed,
        rhs: Range<u32>,
        heap: &mut Vec<(Reverse<F32>, u32)>,
        codes: &[u8],
        packed_codes: &[u8],
        fast_scan: bool,
    ) {
        let dims = self.dims;
        let width = dims;
        if fast_scan
            && O::SUPPORT_FAST_SCAN
            && self.bits == 4
            && crate::fast_scan::b4::is_supported()
        {
            use crate::fast_scan::b4::{fast_scan, BLOCK_SIZE};
            use crate::fast_scan::quantize::{dequantize, quantize};
            let s = rhs.start.next_multiple_of(BLOCK_SIZE);
            let e = (rhs.end + 1 - BLOCK_SIZE).next_multiple_of(BLOCK_SIZE);
            heap.extend((rhs.start..s).map(|u| {
                (
                    Reverse(self.process(preprocessed, {
                        let bytes = self.bytes() as usize;
                        let start = u as usize * bytes;
                        let end = start + bytes;
                        &codes[start..end]
                    })),
                    u,
                )
            }));
            let (k, b, lut) = quantize(&O::fast_scan(preprocessed));
            for i in (s..e).step_by(BLOCK_SIZE as _) {
                let bytes = width as usize * 16;
                let start = (i / BLOCK_SIZE) as usize * bytes;
                let end = start + bytes;
                heap.extend({
                    let res = fast_scan(width, &packed_codes[start..end], &lut);
                    let r = res.map(|x| O::fast_scan_resolve(dequantize(width, k, b, x)));
                    (i..i + BLOCK_SIZE)
                        .map(|u| (Reverse(r[(u - i) as usize]), u))
                        .collect::<Vec<_>>()
                });
            }
            heap.extend((e..rhs.end).map(|u| {
                (
                    Reverse(self.process(preprocessed, {
                        let bytes = self.bytes() as usize;
                        let start = u as usize * bytes;
                        let end = start + bytes;
                        &codes[start..end]
                    })),
                    u,
                )
            }));
            return;
        }
        heap.extend(rhs.map(|u| {
            (
                Reverse(self.process(preprocessed, {
                    let bytes = self.bytes() as usize;
                    let start = u as usize * bytes;
                    let end = start + bytes;
                    &codes[start..end]
                })),
                u,
            )
        }));
    }

    pub fn flat_rerank<'a, T: 'a, R: Fn(u32) -> (F32, T) + 'a>(
        &'a self,
        heap: Vec<(Reverse<F32>, u32)>,
        r: R,
        rerank_size: u32,
    ) -> impl RerankerPop<T> + 'a {
        WindowFlatReranker::new(heap, r, rerank_size)
    }

    pub fn graph_rerank<'a, T: 'a, C: Fn(u32) -> &'a [u8] + 'a, R: Fn(u32) -> (F32, T) + 'a>(
        &'a self,
        vector: Borrowed<'a, O>,
        c: C,
        r: R,
    ) -> impl RerankerPush + RerankerPop<T> + 'a {
        let p =
            O::scalar_quantization_preprocess(self.dims, self.bits, &self.max, &self.min, vector);
        Window0GraphReranker::new(move |u| self.process(&p, c(u)), r)
    }
}
