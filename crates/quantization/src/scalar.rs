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
use base::search::RerankerPop;
use base::search::RerankerPush;
use base::search::Vectors;
use base::vector::*;
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use serde::Deserialize;
use serde::Serialize;
use std::cmp::Reverse;
use std::marker::PhantomData;
use std::ops::Range;
use stoppable_rayon as rayon;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct ScalarQuantizer<O: OperatorScalarQuantization> {
    dims: u32,
    bits: u32,
    min: Vec<f32>,
    max: Vec<f32>,
    _phantom: PhantomData<fn(O) -> O>,
}

impl<O: OperatorScalarQuantization> Quantizer<O> for ScalarQuantizer<O> {
    fn train(
        vector_options: VectorOptions,
        options: Option<QuantizationOptions>,
        vectors: &(impl Vectors<O::Vector> + Sync),
        transform: impl Fn(Borrowed<'_, O>) -> O::Vector + Copy + Sync,
    ) -> Self {
        let options = if let Some(QuantizationOptions::Scalar(x)) = options {
            x
        } else {
            panic!("inconsistent parameters: options and generics")
        };
        let dims = vector_options.dims;
        let bits = options.bits;
        let n = vectors.len();
        let (min, max) = (0..n)
            .into_par_iter()
            .fold(
                || {
                    (
                        vec![f32::INFINITY; dims as usize],
                        vec![f32::NEG_INFINITY; dims as usize],
                    )
                },
                |(mut min, mut max), i| {
                    let vector = transform(vectors.vector(i));
                    let vector = vector.as_borrowed();
                    for j in 0..dims {
                        min[j as usize] = min[j as usize].min(O::get(vector, j).to_f32());
                        max[j as usize] = max[j as usize].max(O::get(vector, j).to_f32());
                    }
                    (min, max)
                },
            )
            .reduce(
                || {
                    (
                        vec![f32::INFINITY; dims as usize],
                        vec![f32::NEG_INFINITY; dims as usize],
                    )
                },
                |(mut min, mut max), (rmin, rmax)| {
                    for j in 0..dims {
                        min[j as usize] = min[j as usize].min(rmin[j as usize]);
                        max[j as usize] = max[j as usize].max(rmax[j as usize]);
                    }
                    (min, max)
                },
            );
        Self {
            dims,
            bits,
            min,
            max,
            _phantom: PhantomData,
        }
    }

    fn encode(&self, vector: Borrowed<'_, O>) -> Vec<u8> {
        let dims = self.dims;
        let bits = self.bits;
        let min = self.min.as_slice();
        let max = self.max.as_slice();
        let code_size = (dims * bits).div_ceil(8);
        let mut code = Vec::with_capacity(dims as usize);
        for i in 0..dims {
            let val = O::get(vector, i).to_f32();
            let bas = min[i as usize];
            let del = (max[i as usize] - min[i as usize]) / ((1 << bits) - 1) as f32;
            let j = ((val - bas) / del).round_ties_even() as u32;
            code.push(j.clamp(0, (1 << bits) - 1) as u8);
        }
        match bits {
            1 => InfiniteByteChunks::new(code.into_iter())
                .map(merge_8)
                .take(code_size as usize)
                .collect(),
            2 => InfiniteByteChunks::new(code.into_iter())
                .map(merge_4)
                .take(code_size as usize)
                .collect(),
            4 => InfiniteByteChunks::new(code.into_iter())
                .map(merge_2)
                .take(code_size as usize)
                .collect(),
            8 => code,
            _ => unreachable!(),
        }
    }

    fn fscan_encode(&self, vectors: [O::Vector; 32]) -> Vec<u8> {
        let dims = self.dims;
        let bits = self.bits;
        let min = self.min.as_slice();
        let max = self.max.as_slice();
        if bits == 4 {
            let codes = vectors.map(|vector| {
                let mut code = Vec::with_capacity(dims as usize);
                for i in 0..dims {
                    let val = O::get(vector.as_borrowed(), i).to_f32();
                    let bas = min[i as usize];
                    let del = (max[i as usize] - min[i as usize]) / ((1 << bits) - 1) as f32;
                    let j = ((val - bas) / del).round_ties_even() as u32;
                    code.push(j.clamp(0, (1 << bits) - 1) as u8);
                }
                code
            });
            pack(dims, codes).collect()
        } else {
            Vec::new()
        }
    }

    fn code_size(&self) -> u32 {
        (self.dims * self.bits).div_ceil(8)
    }

    fn fcode_size(&self) -> u32 {
        if self.bits == 4 {
            self.dims * 16
        } else {
            0
        }
    }

    fn project(&self, vector: Borrowed<'_, O>) -> O::Vector {
        vector.own()
    }

    type Lut = Vec<f32>;

    fn preprocess(&self, vector: Borrowed<'_, O>) -> Self::Lut {
        O::preprocess(self.dims, self.bits, &self.min, &self.max, vector)
    }

    fn process(&self, lut: &Self::Lut, code: &[u8], _: Borrowed<'_, O>) -> Distance {
        O::process(self.dims, self.bits, lut, code)
    }

    type FLut = (
        /* dims */ u32,
        /* k */ f32,
        /* b */ f32,
        Vec<u8>,
    );

    fn fscan_preprocess(&self, vector: Borrowed<'_, O>) -> Self::FLut {
        O::fscan_preprocess(self.dims, self.bits, &self.min, &self.max, vector)
    }

    fn fscan_process(&self, flut: &Self::FLut, code: &[u8]) -> [Distance; 32] {
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
        if opts.sq_fast_scan && self.bits == 4 {
            Ok(self.fscan_preprocess(vector))
        } else {
            Err(self.preprocess(vector))
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
        WindowFlatReranker::new(heap, rerank, opts.sq_rerank_size)
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
                    let r = self.fscan_process(flut, locate_1(i).as_ref());
                    for j in 0..32 {
                        if range.contains(&(i * 32 + j)) {
                            heap.push((Reverse(r[j as usize]), AlwaysEqual(i * 32 + j)));
                        }
                    }
                }
                for i in main {
                    let r = self.fscan_process(flut, locate_1(i).as_ref());
                    for j in 0..32 {
                        heap.push((Reverse(r[j as usize]), AlwaysEqual(i * 32 + j)));
                    }
                }
                if let Some(i) = right {
                    let r = self.fscan_process(flut, locate_1(i).as_ref());
                    for j in 0..32 {
                        if range.contains(&(i * 32 + j)) {
                            heap.push((Reverse(r[j as usize]), AlwaysEqual(i * 32 + j)));
                        }
                    }
                }
            }
            Err(lut) => {
                for j in range {
                    let r = O::process(self.dims, self.bits, lut, locate_0(j).as_ref());
                    heap.push((Reverse(r), AlwaysEqual(j)));
                }
            }
        }
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
            move |u| O::process(self.dims, self.bits, &lut, locate(u).as_ref()),
            rerank,
        )
    }
}

pub trait OperatorScalarQuantization: Operator {
    type Scalar: ScalarLike;
    fn get(vector: Borrowed<'_, Self>, i: u32) -> Self::Scalar;

    fn preprocess(
        dims: u32,
        bits: u32,
        min: &[f32],
        max: &[f32],
        vector: Borrowed<'_, Self>,
    ) -> Vec<f32>;
    fn process(dims: u32, bits: u32, lut: &[f32], code: &[u8]) -> Distance;

    fn fscan_preprocess(
        dims: u32,
        bits: u32,
        min: &[f32],
        max: &[f32],
        vector: Borrowed<'_, Self>,
    ) -> (u32, f32, f32, Vec<u8>);
    fn fscan_process(flut: &(u32, f32, f32, Vec<u8>), code: &[u8]) -> [Distance; 32];
}

impl<S: ScalarLike> OperatorScalarQuantization for VectDot<S> {
    type Scalar = S;
    fn get(vector: Borrowed<'_, Self>, i: u32) -> Self::Scalar {
        vector.slice()[i as usize]
    }

    fn preprocess(
        dims: u32,
        bits: u32,
        min: &[f32],
        max: &[f32],
        vector: Borrowed<'_, Self>,
    ) -> Vec<f32> {
        #[inline(never)]
        fn internal<const BITS: usize, S: ScalarLike>(
            dims: usize,
            min: &[f32],
            max: &[f32],
            vector: &[S],
        ) -> Vec<f32> {
            assert!(dims <= 65535);
            assert!(dims == min.len());
            assert!(dims == max.len());
            assert!(dims == vector.len());
            let mut table = Vec::<f32>::with_capacity(dims * (1 << BITS));
            for i in 0..dims {
                let bas = min[i];
                let del = (max[i] - min[i]) / ((1 << BITS) - 1) as f32;
                for j in 0..1 << BITS {
                    let x = vector[i].to_f32();
                    let y = bas + (j as f32) * del;
                    let value = x * y;
                    unsafe {
                        table.as_mut_ptr().add(i * (1 << BITS) + j).write(value);
                    }
                }
            }
            unsafe {
                table.set_len(dims * (1 << BITS));
            }
            table
        }
        match bits {
            1 => internal::<1, _>(dims as _, min, max, vector.slice()),
            2 => internal::<2, _>(dims as _, min, max, vector.slice()),
            4 => internal::<4, _>(dims as _, min, max, vector.slice()),
            8 => internal::<8, _>(dims as _, min, max, vector.slice()),
            _ => unreachable!(),
        }
    }
    fn process(dims: u32, bits: u32, lut: &[f32], rhs: &[u8]) -> Distance {
        fn internal<const BITS: u32>(dims: u32, t: &[f32], f: impl Fn(usize) -> usize) -> Distance {
            let mut xy = 0.0f32;
            for i in 0..dims as usize {
                xy += t[i * (1 << BITS) + f(i)];
            }
            Distance::from(-xy)
        }
        match bits {
            1 => internal::<1>(dims, lut, |i| {
                ((rhs[i >> 3] >> ((i & 7) << 0)) & 1u8) as usize
            }),
            2 => internal::<2>(dims, lut, |i| {
                ((rhs[i >> 2] >> ((i & 3) << 1)) & 3u8) as usize
            }),
            4 => internal::<4>(dims, lut, |i| {
                ((rhs[i >> 1] >> ((i & 1) << 2)) & 15u8) as usize
            }),
            8 => internal::<8>(dims, lut, |i| rhs[i] as usize),
            _ => unreachable!(),
        }
    }

    fn fscan_preprocess(
        dims: u32,
        bits: u32,
        min: &[f32],
        max: &[f32],
        vector: Borrowed<'_, Self>,
    ) -> (u32, f32, f32, Vec<u8>) {
        let (k, b, t) = quantize::<255>(&Self::preprocess(dims, bits, min, max, vector));
        (dims, k, b, t)
    }
    fn fscan_process(flut: &(u32, f32, f32, Vec<u8>), codes: &[u8]) -> [Distance; 32] {
        let &(dims, k, b, ref t) = flut;
        let r = fast_scan_b4(dims, codes, t);
        std::array::from_fn(|i| Distance::from(-((dims as f32) * b + (r[i] as f32) * k)))
    }
}

impl<S: ScalarLike> OperatorScalarQuantization for VectL2<S> {
    type Scalar = S;
    fn get(vector: Borrowed<'_, Self>, i: u32) -> Self::Scalar {
        vector.slice()[i as usize]
    }

    fn preprocess(
        dims: u32,
        bits: u32,
        min: &[f32],
        max: &[f32],
        vector: Borrowed<'_, Self>,
    ) -> Vec<f32> {
        #[inline(never)]
        fn internal<const BITS: usize, S: ScalarLike>(
            dims: usize,
            min: &[f32],
            max: &[f32],
            vector: &[S],
        ) -> Vec<f32> {
            assert!(dims <= 65535);
            assert!(dims == min.len());
            assert!(dims == max.len());
            assert!(dims == vector.len());
            let mut table = Vec::<f32>::with_capacity(dims * (1 << BITS));
            for i in 0..dims {
                let bas = min[i];
                let del = (max[i] - min[i]) / ((1 << BITS) - 1) as f32;
                for j in 0..1 << BITS {
                    let x = vector[i].to_f32();
                    let y = bas + (j as f32) * del;
                    let value = (x - y) * (x - y);
                    unsafe {
                        table.as_mut_ptr().add(i * (1 << BITS) + j).write(value);
                    }
                }
            }
            unsafe {
                table.set_len(dims * (1 << BITS));
            }
            table
        }
        match bits {
            1 => internal::<1, _>(dims as _, min, max, vector.slice()),
            2 => internal::<2, _>(dims as _, min, max, vector.slice()),
            4 => internal::<4, _>(dims as _, min, max, vector.slice()),
            8 => internal::<8, _>(dims as _, min, max, vector.slice()),
            _ => unreachable!(),
        }
    }
    fn process(dims: u32, bits: u32, lut: &[f32], rhs: &[u8]) -> Distance {
        fn internal<const BITS: u32>(dims: u32, t: &[f32], f: impl Fn(usize) -> usize) -> Distance {
            let mut d2 = 0.0f32;
            for i in 0..dims as usize {
                d2 += t[i * (1 << BITS) + f(i)];
            }
            Distance::from(d2)
        }
        match bits {
            1 => internal::<1>(dims, lut, |i| {
                ((rhs[i >> 3] >> ((i & 7) << 0)) & 1u8) as usize
            }),
            2 => internal::<2>(dims, lut, |i| {
                ((rhs[i >> 2] >> ((i & 3) << 1)) & 3u8) as usize
            }),
            4 => internal::<4>(dims, lut, |i| {
                ((rhs[i >> 1] >> ((i & 1) << 2)) & 15u8) as usize
            }),
            8 => internal::<8>(dims, lut, |i| rhs[i] as usize),
            _ => unreachable!(),
        }
    }

    fn fscan_preprocess(
        dims: u32,
        bits: u32,
        min: &[f32],
        max: &[f32],
        vector: Borrowed<'_, Self>,
    ) -> (u32, f32, f32, Vec<u8>) {
        let (k, b, t) = quantize::<255>(&Self::preprocess(dims, bits, min, max, vector));
        (dims, k, b, t)
    }
    fn fscan_process(flut: &(u32, f32, f32, Vec<u8>), codes: &[u8]) -> [Distance; 32] {
        let &(dims, k, b, ref t) = flut;
        let r = fast_scan_b4(dims, codes, t);
        std::array::from_fn(|i| Distance::from((dims as f32) * b + (r[i] as f32) * k))
    }
}

macro_rules! unimpl_operator_scalar_quantization {
    ($t:ty) => {
        impl OperatorScalarQuantization for $t {
            type Scalar = Impossible;
            fn get(_: Borrowed<'_, Self>, _: u32) -> Self::Scalar {
                unimplemented!()
            }

            fn preprocess(_: u32, _: u32, _: &[f32], _: &[f32], _: Borrowed<'_, Self>) -> Vec<f32> {
                unimplemented!()
            }
            fn process(_: u32, _: u32, _: &[f32], _: &[u8]) -> Distance {
                unimplemented!()
            }

            fn fscan_preprocess(
                _: u32,
                _: u32,
                _: &[f32],
                _: &[f32],
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

unimpl_operator_scalar_quantization!(BVectorDot);
unimpl_operator_scalar_quantization!(BVectorHamming);
unimpl_operator_scalar_quantization!(BVectorJaccard);

unimpl_operator_scalar_quantization!(SVectDot<f32>);
unimpl_operator_scalar_quantization!(SVectL2<f32>);
