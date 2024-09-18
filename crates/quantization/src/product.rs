use crate::fast_scan::b4::fast_scan_b4;
use crate::fast_scan::b4::pack;
use crate::quantize::quantize;
use crate::quantizer::Quantizer;
use crate::reranker::flat::WindowFlatReranker;
use crate::reranker::graph_2::Graph2Reranker;
use crate::utils::merge_2;
use crate::utils::merge_4;
use crate::utils::merge_8;
use crate::utils::InfiniteByteChunks;
use base::always_equal::AlwaysEqual;
use base::distance::Distance;
use base::index::*;
use base::operator::*;
use base::scalar::impossible::Impossible;
use base::scalar::ScalarLike;
use base::search::*;
use base::vector::VectorOwned;
use common::sample::sample;
use common::vec2::Vec2;
use k_means::k_means;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use serde::Deserialize;
use serde::Serialize;
use std::cmp::Reverse;
use std::ops::Range;
use stoppable_rayon as rayon;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct ProductQuantizer<O: OperatorProductQuantization> {
    dims: u32,
    ratio: u32,
    bits: u32,
    originals: Vec<Vec2<O::Scalar>>,
    centroids: Vec2<O::Scalar>,
}

impl<O: OperatorProductQuantization> Quantizer<O> for ProductQuantizer<O> {
    fn train(
        vector_options: VectorOptions,
        options: Option<QuantizationOptions>,
        vectors: &(impl Vectors<Owned<O>> + Sync),
        transform: impl Fn(Borrowed<'_, O>) -> Owned<O> + Copy + Sync,
    ) -> Self {
        let dims = vector_options.dims;
        let options = if let Some(QuantizationOptions::Product(x)) = options {
            x
        } else {
            panic!("inconsistent parameters: options and generics")
        };
        let ratio = options.ratio;
        let bits = options.bits;
        let width = dims.div_ceil(ratio);
        let originals = (0..width)
            .into_par_iter()
            .map(|p| {
                let subdims = std::cmp::min(ratio, dims - ratio * p);
                let start = p * ratio;
                let end = start + subdims;
                let subsamples = sample(vectors.len(), 65536, end - start, |i| {
                    O::subslice(
                        transform(vectors.vector(i)).as_borrowed(),
                        start,
                        end - start,
                    )
                    .to_vec()
                });
                k_means(1 << bits, subsamples, false, false, true)
            })
            .collect::<Vec<_>>();
        let mut centroids = Vec2::zeros((1 << bits, dims as usize));
        for p in 0..width {
            let subdims = std::cmp::min(ratio, dims - ratio * p);
            for j in 0_usize..(1 << bits) {
                centroids[(j,)][(p * ratio) as usize..][..subdims as usize]
                    .copy_from_slice(&originals[p as usize][(j,)]);
            }
        }
        Self {
            dims,
            ratio,
            bits,
            originals,
            centroids,
        }
    }

    fn encode(&self, vector: Borrowed<'_, O>) -> Vec<u8> {
        let dims = self.dims;
        let ratio = self.ratio;
        let bits = self.bits;
        let width = dims.div_ceil(ratio);
        let bytes = (dims.div_ceil(ratio) * bits).div_ceil(8);
        let mut code = Vec::with_capacity(width.div_ceil(bits) as usize);
        for p in 0..width {
            let subdims = std::cmp::min(ratio, dims - ratio * p);
            let left = O::subslice(vector, p * ratio, subdims);
            let target = k_means::k_means_lookup(left, &self.originals[p as usize]);
            code.push(target as u8);
        }
        match bits {
            1 => InfiniteByteChunks::new(code.into_iter())
                .map(merge_8)
                .take(bytes as usize)
                .collect(),
            2 => InfiniteByteChunks::new(code.into_iter())
                .map(merge_4)
                .take(bytes as usize)
                .collect(),
            4 => InfiniteByteChunks::new(code.into_iter())
                .map(merge_2)
                .take(bytes as usize)
                .collect(),
            8 => code,
            _ => unreachable!(),
        }
    }

    fn fscan_encode(&self, vectors: [Owned<O>; 32]) -> Vec<u8> {
        let dims = self.dims;
        let ratio = self.ratio;
        let bits = self.bits;
        if bits == 4 {
            let width = dims.div_ceil(ratio);
            let codes = vectors.map(|vector| {
                let mut code = Vec::with_capacity(width.div_ceil(bits) as usize);
                for p in 0..width {
                    let subdims = std::cmp::min(ratio, dims - ratio * p);
                    let left = O::subslice(vector.as_borrowed(), p * ratio, subdims);
                    let target = k_means::k_means_lookup(left, &self.originals[p as usize]);
                    code.push(target as u8);
                }
                code
            });
            pack(width, codes).collect()
        } else {
            Vec::new()
        }
    }

    fn code_size(&self) -> u32 {
        (self.dims * self.bits).div_ceil(8)
    }

    fn fcode_size(&self) -> u32 {
        if self.bits == 4 {
            self.dims.div_ceil(self.ratio) * 16
        } else {
            0
        }
    }

    type Lut = Vec<f32>;

    fn preprocess(&self, vector: Borrowed<'_, O>) -> Self::Lut {
        O::preprocess(
            self.dims,
            self.ratio,
            self.bits,
            self.centroids.as_slice(),
            vector,
        )
    }

    fn process(&self, lut: &Self::Lut, code: &[u8], _: Borrowed<'_, O>) -> Distance {
        O::process(self.dims, self.ratio, self.bits, lut, code)
    }

    type FLut = (
        /* width */ u32,
        /* k */ f32,
        /* b */ f32,
        Vec<u8>,
    );

    fn fscan_preprocess(&self, vector: Borrowed<'_, O>) -> Self::FLut {
        O::fscan_preprocess(
            self.dims,
            self.ratio,
            self.bits,
            self.centroids.as_slice(),
            vector,
        )
    }

    fn fscan_process(flut: &Self::FLut, code: &[u8]) -> [Distance; 32] {
        O::fscan_process(flut, code)
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
        if opts.pq_fast_scan && self.bits == 4 {
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
        heap: &mut Vec<(Reverse<Distance>, AlwaysEqual<u32>)>,
    ) where
        C: AsRef<[u8]>,
    {
        match frlut {
            Ok(flut) => {
                fn divide(r: Range<u32>) -> (Option<u32>, Range<u32>, Option<u32>) {
                    if r.start > r.end || r.start % 32 == 0 && r.end % 32 == 0 {
                        (None, r.start / 32..r.end / 32, None)
                    } else if r.start / 32 == r.end / 32 {
                        (Some(r.start / 32), 0..0, None)
                    } else {
                        let left = (r.start % 32 != 0).then_some(r.start / 32);
                        let right = (r.end % 32 != 0).then_some(r.end / 32);
                        (left, r.start / 32 + 1..r.end / 32, right)
                    }
                }
                let (left, main, right) = divide(range.clone());
                if let Some(i) = left {
                    let r = Self::fscan_process(flut, locate_1(i).as_ref());
                    for j in 0..32 {
                        if range.contains(&(i * 32 + j)) {
                            heap.push((Reverse(r[j as usize]), AlwaysEqual(i * 32 + j)));
                        }
                    }
                }
                for i in main {
                    let r = Self::fscan_process(flut, locate_1(i).as_ref());
                    for j in 0..32 {
                        heap.push((Reverse(r[j as usize]), AlwaysEqual(i * 32 + j)));
                    }
                }
                if let Some(i) = right {
                    let r = Self::fscan_process(flut, locate_1(i).as_ref());
                    for j in 0..32 {
                        if range.contains(&(i * 32 + j)) {
                            heap.push((Reverse(r[j as usize]), AlwaysEqual(i * 32 + j)));
                        }
                    }
                }
            }
            Err(lut) => {
                for j in range {
                    let r = O::process(self.dims, self.ratio, self.bits, lut, locate_0(j).as_ref());
                    heap.push((Reverse(r), AlwaysEqual(j)));
                }
            }
        }
    }

    fn flat_rerank_break<'a, T: 'a, R>(
        &'a self,
        heap: Vec<(Reverse<Distance>, AlwaysEqual<u32>)>,
        rerank: R,
        opts: &SearchOptions,
    ) -> impl RerankerPop<T> + 'a
    where
        R: Fn(u32) -> (Distance, T) + 'a,
    {
        WindowFlatReranker::new(heap, rerank, opts.pq_rerank_size)
    }

    fn graph_rerank<'a, T, R, C>(
        &'a self,
        locate: impl Fn(u32) -> C + 'a,
        vector: Borrowed<'a, O>,
        rerank: R,
    ) -> impl RerankerPush + RerankerPop<T> + 'a
    where
        T: 'a,
        R: Fn(u32) -> (Distance, T) + 'a,
        C: AsRef<[u8]>,
    {
        let lut = self.preprocess(vector);
        Graph2Reranker::new(
            move |u| self.process(&lut, locate(u).as_ref(), vector),
            rerank,
        )
    }
}

pub trait OperatorProductQuantization: Operator {
    type Scalar: ScalarLike;
    fn subslice(vector: Borrowed<'_, Self>, start: u32, len: u32) -> &[Self::Scalar];

    fn preprocess(
        dims: u32,
        ratio: u32,
        bits: u32,
        centroids: &[Self::Scalar],
        vector: Borrowed<'_, Self>,
    ) -> Vec<f32>;
    fn process(dims: u32, ratio: u32, bits: u32, lut: &[f32], code: &[u8]) -> Distance;
    fn fscan_preprocess(
        dims: u32,
        ratio: u32,
        bits: u32,
        centroids: &[Self::Scalar],
        vector: Borrowed<'_, Self>,
    ) -> (u32, f32, f32, Vec<u8>);
    fn fscan_process(flut: &(u32, f32, f32, Vec<u8>), code: &[u8]) -> [Distance; 32];
}

impl<S: ScalarLike> OperatorProductQuantization for VectDot<S> {
    type Scalar = S;
    fn subslice(vector: Borrowed<'_, Self>, start: u32, len: u32) -> &[Self::Scalar] {
        &vector.slice()[start as usize..][..len as usize]
    }

    fn preprocess(
        dims: u32,
        ratio: u32,
        bits: u32,
        centroids: &[Self::Scalar],
        vector: Borrowed<'_, Self>,
    ) -> Vec<f32> {
        let mut xy = Vec::with_capacity((dims.div_ceil(ratio) as usize) * (1 << bits));
        for p in 0..dims.div_ceil(ratio) {
            let subdims = std::cmp::min(ratio, dims - ratio * p);
            xy.extend((0_usize..1 << bits).map(|k| {
                let mut xy = 0.0f32;
                for i in ratio * p..ratio * p + subdims {
                    let x = vector.slice()[i as usize].to_f32();
                    let y = centroids[(k as u32 * dims + i) as usize].to_f32();
                    xy += x * y;
                }
                xy
            }));
        }
        xy
    }
    fn process(dims: u32, ratio: u32, bits: u32, lut: &[f32], code: &[u8]) -> Distance {
        fn internal(
            dims: u32,
            ratio: u32,
            bits: u32,
            t: &[f32],
            f: impl Fn(usize) -> usize,
        ) -> Distance {
            let width = dims.div_ceil(ratio);
            let xy = {
                let mut xy = 0.0f32;
                for i in 0..width as usize {
                    xy += t[i * (1 << bits) + f(i)];
                }
                xy
            };
            Distance::from(-xy)
        }
        match bits {
            1 => internal(dims, ratio, bits, lut, |i| {
                ((code[i >> 3] >> ((i & 7) << 0)) & 1u8) as usize
            }),
            2 => internal(dims, ratio, bits, lut, |i| {
                ((code[i >> 2] >> ((i & 3) << 1)) & 3u8) as usize
            }),
            4 => internal(dims, ratio, bits, lut, |i| {
                ((code[i >> 1] >> ((i & 1) << 2)) & 15u8) as usize
            }),
            8 => internal(dims, ratio, bits, lut, |i| code[i] as usize),
            _ => unreachable!(),
        }
    }

    fn fscan_preprocess(
        dims: u32,
        ratio: u32,
        bits: u32,
        centroids: &[Self::Scalar],
        vector: Borrowed<'_, Self>,
    ) -> (u32, f32, f32, Vec<u8>) {
        let (k, b, t) = quantize::<255>(&Self::preprocess(dims, ratio, bits, centroids, vector));
        (dims.div_ceil(ratio), k, b, t)
    }
    fn fscan_process(flut: &(u32, f32, f32, Vec<u8>), codes: &[u8]) -> [Distance; 32] {
        let &(width, k, b, ref t) = flut;
        let r = fast_scan_b4(width, codes, t);
        std::array::from_fn(|i| Distance::from(-((width as f32) * b + (r[i] as f32) * k)))
    }
}

impl<S: ScalarLike> OperatorProductQuantization for VectL2<S> {
    type Scalar = S;
    fn subslice(vector: Borrowed<'_, Self>, start: u32, len: u32) -> &[Self::Scalar] {
        &vector.slice()[start as usize..][..len as usize]
    }

    fn preprocess(
        dims: u32,
        ratio: u32,
        bits: u32,
        centroids: &[Self::Scalar],
        vector: Borrowed<'_, Self>,
    ) -> Vec<f32> {
        let mut d2 = Vec::with_capacity((dims.div_ceil(ratio) as usize) * (1 << bits));
        for p in 0..dims.div_ceil(ratio) {
            let subdims = std::cmp::min(ratio, dims - ratio * p);
            d2.extend((0_usize..1 << bits).map(|k| {
                let mut d2 = 0.0f32;
                for i in ratio * p..ratio * p + subdims {
                    let x = vector.slice()[i as usize].to_f32();
                    let y = centroids[(k as u32 * dims + i) as usize].to_f32();
                    let d = x - y;
                    d2 += d * d;
                }
                d2
            }));
        }
        d2
    }
    fn process(dims: u32, ratio: u32, bits: u32, lut: &[f32], code: &[u8]) -> Distance {
        fn internal(
            dims: u32,
            ratio: u32,
            bits: u32,
            t: &[f32],
            f: impl Fn(usize) -> usize,
        ) -> Distance {
            let width = dims.div_ceil(ratio);
            let mut d2 = 0.0f32;
            for i in 0..width as usize {
                d2 += t[i * (1 << bits) + f(i)];
            }
            Distance::from(d2)
        }
        match bits {
            1 => internal(dims, ratio, bits, lut, |i| {
                ((code[i >> 3] >> ((i & 7) << 0)) & 1u8) as usize
            }),
            2 => internal(dims, ratio, bits, lut, |i| {
                ((code[i >> 2] >> ((i & 3) << 1)) & 3u8) as usize
            }),
            4 => internal(dims, ratio, bits, lut, |i| {
                ((code[i >> 1] >> ((i & 1) << 2)) & 15u8) as usize
            }),
            8 => internal(dims, ratio, bits, lut, |i| code[i] as usize),
            _ => unreachable!(),
        }
    }

    fn fscan_preprocess(
        dims: u32,
        ratio: u32,
        bits: u32,
        centroids: &[Self::Scalar],
        vector: Borrowed<'_, Self>,
    ) -> (u32, f32, f32, Vec<u8>) {
        let (k, b, t) = quantize::<255>(&Self::preprocess(dims, ratio, bits, centroids, vector));
        (dims.div_ceil(ratio), k, b, t)
    }
    fn fscan_process(flut: &(u32, f32, f32, Vec<u8>), codes: &[u8]) -> [Distance; 32] {
        let &(width, k, b, ref t) = flut;
        let r = fast_scan_b4(width, codes, t);
        std::array::from_fn(|i| Distance::from((width as f32) * b + (r[i] as f32) * k))
    }
}

macro_rules! unimpl_operator_product_quantization {
    ($t:ty) => {
        impl OperatorProductQuantization for $t {
            type Scalar = Impossible;
            fn subslice(_: Borrowed<'_, Self>, _: u32, _: u32) -> &[Self::Scalar] {
                unimplemented!()
            }

            fn preprocess(
                _: u32,
                _: u32,
                _: u32,
                _: &[Self::Scalar],
                _: Borrowed<'_, Self>,
            ) -> Vec<f32> {
                unimplemented!()
            }
            fn process(_: u32, _: u32, _: u32, _: &[f32], _: &[u8]) -> Distance {
                unimplemented!()
            }

            fn fscan_preprocess(
                _: u32,
                _: u32,
                _: u32,
                _: &[Self::Scalar],
                _: Borrowed<'_, Self>,
            ) -> (u32, f32, f32, Vec<u8>) {
                unimplemented!()
            }
            fn fscan_process(_: &(u32, f32, f32, Vec<u8>), _: &[u8]) -> [Distance; 32] {
                unimplemented!()
            }
        }
    };
}

unimpl_operator_product_quantization!(BVectorDot);
unimpl_operator_product_quantization!(BVectorHamming);
unimpl_operator_product_quantization!(BVectorJaccard);

unimpl_operator_product_quantization!(SVectDot<f32>);
unimpl_operator_product_quantization!(SVectL2<f32>);
