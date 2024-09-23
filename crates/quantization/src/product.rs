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
use base::vector::VectorBorrowed;
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
    centroids: Vec2<O::Scalar>,
    tcentroids: Vec2<O::Scalar>,
}

impl<O: OperatorProductQuantization> Quantizer<O> for ProductQuantizer<O> {
    fn train(
        vector_options: VectorOptions,
        options: Option<QuantizationOptions>,
        vectors: &(impl Vectors<O::Vector> + Sync),
        transform: impl Fn(Borrowed<'_, O>) -> O::Vector + Copy + Sync,
    ) -> Self {
        let dims = vector_options.dims;
        let options = if let Some(QuantizationOptions::Product(x)) = options {
            x
        } else {
            panic!("inconsistent parameters: options and generics")
        };
        let ratio = options.ratio;
        let bits = options.bits;
        let points = (0..dims.div_ceil(ratio))
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
                k_means(1 << bits, subsamples, false, 25, true)
            })
            .collect::<Vec<_>>();
        let mut centroids = Vec2::zeros((1 << bits, dims as usize));
        for i in 0..dims.div_ceil(ratio) {
            let subdims = std::cmp::min(ratio, dims - ratio * i);
            for j in 0_usize..(1 << bits) {
                centroids[(j,)][(i * ratio) as usize..][..subdims as usize]
                    .copy_from_slice(&points[i as usize][(j,)]);
            }
        }
        let mut tcentroids = Vec2::zeros((dims as usize, 1 << bits));
        for i in 0..dims as usize {
            for j in 0_usize..(1 << bits) {
                tcentroids[(i, j)] = centroids[(j, i)];
            }
        }
        Self {
            dims,
            ratio,
            bits,
            centroids,
            tcentroids,
        }
    }

    fn encode(&self, vector: Borrowed<'_, O>) -> Vec<u8> {
        let dims = self.dims;
        let ratio = self.ratio;
        let bits = self.bits;
        let code_size = (dims.div_ceil(ratio) * bits).div_ceil(8);
        let code = O::code(dims, ratio, bits, &self.centroids, vector);
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
        let ratio = self.ratio;
        let bits = self.bits;
        if bits == 4 {
            let codes = vectors
                .map(|vector| O::code(dims, ratio, bits, &self.centroids, vector.as_borrowed()));
            pack(dims.div_ceil(ratio), codes).collect()
        } else {
            Vec::new()
        }
    }

    fn code_size(&self) -> u32 {
        (self.dims.div_ceil(self.ratio) * self.bits).div_ceil(8)
    }

    fn fcode_size(&self) -> u32 {
        if self.bits == 4 {
            self.dims.div_ceil(self.ratio) * 16
        } else {
            0
        }
    }

    fn project(&self, vector: Borrowed<'_, O>) -> O::Vector {
        vector.own()
    }

    type Lut = Vec<f32>;

    fn preprocess(&self, vector: Borrowed<'_, O>) -> Self::Lut {
        O::preprocess(self.dims, self.ratio, self.bits, &self.tcentroids, vector)
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
        O::fscan_preprocess(self.dims, self.ratio, self.bits, &self.tcentroids, vector)
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
            move |u| O::process(self.dims, self.ratio, self.bits, &lut, locate(u).as_ref()),
            rerank,
        )
    }
}

pub trait OperatorProductQuantization: Operator {
    type Scalar: ScalarLike;
    fn subslice(vector: Borrowed<'_, Self>, start: u32, len: u32) -> &[Self::Scalar];
    fn code(
        dims: u32,
        ratio: u32,
        bits: u32,
        centroids: &Vec2<Self::Scalar>,
        vector: Borrowed<'_, Self>,
    ) -> Vec<u8>;

    fn preprocess(
        dims: u32,
        ratio: u32,
        bits: u32,
        tcentroids: &Vec2<Self::Scalar>,
        vector: Borrowed<'_, Self>,
    ) -> Vec<f32>;
    fn process(dims: u32, ratio: u32, bits: u32, lut: &[f32], code: &[u8]) -> Distance;
    fn fscan_preprocess(
        dims: u32,
        ratio: u32,
        bits: u32,
        tcentroids: &Vec2<Self::Scalar>,
        vector: Borrowed<'_, Self>,
    ) -> (u32, f32, f32, Vec<u8>);
    fn fscan_process(flut: &(u32, f32, f32, Vec<u8>), code: &[u8]) -> [Distance; 32];
}

impl<S: ScalarLike> OperatorProductQuantization for VectDot<S> {
    type Scalar = S;
    fn subslice(vector: Borrowed<'_, Self>, start: u32, len: u32) -> &[Self::Scalar] {
        &vector.slice()[start as usize..][..len as usize]
    }
    fn code(
        dims: u32,
        ratio: u32,
        bits: u32,
        centroids: &Vec2<S>,
        vector: Borrowed<'_, Self>,
    ) -> Vec<u8> {
        let mut code = Vec::with_capacity(dims.div_ceil(ratio) as _);
        for i in 0..dims.div_ceil(ratio) {
            let subdims = std::cmp::min(ratio, dims - ratio * i);
            let left = Self::subslice(vector, i * ratio, subdims);
            let mut minimal = f32::INFINITY;
            let mut target = 0;
            for j in 0_usize..(1 << bits) {
                let right = &centroids[(j,)][(i * ratio) as usize..][..subdims as usize];
                let dis = S::reduce_sum_of_d2(left, right);
                if dis < minimal {
                    minimal = dis;
                    target = j;
                }
            }
            code.push(target as u8);
        }
        code
    }

    fn preprocess(
        dims: u32,
        ratio: u32,
        bits: u32,
        tcentroids: &Vec2<S>,
        vector: Borrowed<'_, Self>,
    ) -> Vec<f32> {
        #[inline(never)]
        fn internal<const RATIO: usize, const BITS: usize, S: ScalarLike>(
            dims: usize,
            tcentroids: &[S],
            vector: &[S],
        ) -> Vec<f32> {
            // code below needs special care, any minor changes would result in huge performance degradation
            // For example:
            // * calling `Vec::with_capacity` with parameter `dims.div_ceil(RATIO) * (1 << BITS)`
            // * move `assert!(dims <= 65535)` after allocation
            // * change pointer arithmetic to `get_unchecked` or `std::hint::assert_unchecked`
            // * change parameters from slices to pointers
            assert!(dims <= 65535);
            assert!(tcentroids.len() == dims * (1 << BITS));
            assert!(vector.len() == dims);
            let mut table = Vec::<f32>::with_capacity((dims / RATIO) * (1 << BITS) + (1 << BITS));
            if dims >= 32 {
                // fast path
                for i in 0..dims / RATIO {
                    for j in 0..1 << BITS {
                        let mut value = 0.0f32;
                        for k in 0..RATIO {
                            let idx_x = RATIO * i + k;
                            let idx_y = (RATIO * i + k) * (1 << BITS) + j;
                            let x = unsafe { vector.as_ptr().add(idx_x).read() };
                            let y = unsafe { tcentroids.as_ptr().add(idx_y).read() };
                            let xy = x.to_f32() * y.to_f32();
                            value += xy;
                        }
                        unsafe {
                            table.as_mut_ptr().add(i * (1 << BITS) + j).write(value);
                        }
                    }
                }
                if dims % RATIO != 0 {
                    let i = dims / RATIO;
                    for j in 0..1 << BITS {
                        let mut value = 0.0f32;
                        for k in 0..dims % RATIO {
                            let idx_x = RATIO * i + k;
                            let idx_y = (RATIO * i + k) * (1 << BITS) + j;
                            let x = unsafe { vector.as_ptr().add(idx_x).read() };
                            let y = unsafe { tcentroids.as_ptr().add(idx_y).read() };
                            let xy = x.to_f32() * y.to_f32();
                            value += xy;
                        }
                        unsafe {
                            table.as_mut_ptr().add(i * (1 << BITS) + j).write(value);
                        }
                    }
                }
            } else {
                // slow path
                for i in 0..dims.div_ceil(RATIO) {
                    for j in 0..1 << BITS {
                        let mut value = 0.0f32;
                        for k in 0..std::cmp::min(RATIO, dims - j * RATIO) {
                            let idx_x = RATIO * i + k;
                            let idx_y = (RATIO * i + k) * (1 << BITS) + j;
                            let x = unsafe { vector.as_ptr().add(idx_x).read() };
                            let y = unsafe { tcentroids.as_ptr().add(idx_y).read() };
                            let xy = x.to_f32() * y.to_f32();
                            value += xy;
                        }
                        unsafe {
                            table.as_mut_ptr().add(i * (1 << BITS) + j).write(value);
                        }
                    }
                }
            }
            unsafe {
                table.set_len(dims.div_ceil(RATIO) * (1 << BITS));
            }
            table
        }
        assert!((1..=8).contains(&ratio) && (bits == 1 || bits == 2 || bits == 4 || bits == 8));
        let no = (ratio - 1) * 4 + bits.ilog2();
        match no {
            0 => internal::<1, 1, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            1 => internal::<1, 2, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            2 => internal::<1, 4, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            3 => internal::<1, 8, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            4 => internal::<2, 1, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            5 => internal::<2, 2, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            6 => internal::<2, 4, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            7 => internal::<2, 8, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            8 => internal::<3, 1, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            9 => internal::<3, 2, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            10 => internal::<3, 4, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            11 => internal::<3, 8, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            12 => internal::<4, 1, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            13 => internal::<4, 2, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            14 => internal::<4, 4, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            15 => internal::<4, 8, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            16 => internal::<5, 1, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            17 => internal::<5, 2, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            18 => internal::<5, 4, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            19 => internal::<5, 8, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            20 => internal::<6, 1, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            21 => internal::<6, 2, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            22 => internal::<6, 4, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            23 => internal::<6, 8, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            24 => internal::<7, 1, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            25 => internal::<7, 2, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            26 => internal::<7, 4, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            27 => internal::<7, 8, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            28 => internal::<8, 1, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            29 => internal::<8, 2, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            30 => internal::<8, 4, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            31 => internal::<8, 8, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            32.. => unreachable!(),
        }
    }
    fn process(dims: u32, ratio: u32, bits: u32, lut: &[f32], code: &[u8]) -> Distance {
        #[inline(never)]
        fn internal<const BITS: usize>(n: usize, lut: &[f32], code: &[u8]) -> Distance {
            assert!(n >= 1);
            assert!(n <= 65535);
            assert!(code.len() == n / (8 / BITS));
            assert!(lut.len() == n * (1 << BITS));
            let mut sum = 0.0f32;
            for i in 0..n {
                unsafe {
                    // Safety: `i < n`
                    std::hint::assert_unchecked(i / (8 / BITS) < n / (8 / BITS));
                }
                let (alpha, beta) = (i / (8 / BITS), i % (8 / BITS));
                let j = (code[alpha] >> (beta * BITS)) as usize % (1 << BITS);
                unsafe {
                    // Safety: `i < n`, `j < (1 << BITS)`
                    std::hint::assert_unchecked(i * (1 << BITS) + j < n * (1 << BITS));
                }
                sum += lut[i * (1 << BITS) + j];
            }
            Distance::from(-sum)
        }
        match bits {
            1 => internal::<1>(dims.div_ceil(ratio) as _, lut, code),
            2 => internal::<2>(dims.div_ceil(ratio) as _, lut, code),
            4 => internal::<4>(dims.div_ceil(ratio) as _, lut, code),
            8 => internal::<8>(dims.div_ceil(ratio) as _, lut, code),
            _ => unreachable!(),
        }
    }

    fn fscan_preprocess(
        dims: u32,
        ratio: u32,
        bits: u32,
        centroids: &Vec2<S>,
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
    fn code(
        dims: u32,
        ratio: u32,
        bits: u32,
        centroids: &Vec2<S>,
        vector: Borrowed<'_, Self>,
    ) -> Vec<u8> {
        let mut code = Vec::with_capacity(dims.div_ceil(ratio) as _);
        for i in 0..dims.div_ceil(ratio) {
            let subdims = std::cmp::min(ratio, dims - ratio * i);
            let left = Self::subslice(vector, i * ratio, subdims);
            let mut minimal = f32::INFINITY;
            let mut target = 0;
            for j in 0_usize..(1 << bits) {
                let right = &centroids[(j,)][(i * ratio) as usize..][..subdims as usize];
                let dis = S::reduce_sum_of_d2(left, right);
                if dis < minimal {
                    minimal = dis;
                    target = j;
                }
            }
            code.push(target as u8);
        }
        code
    }

    fn preprocess(
        dims: u32,
        ratio: u32,
        bits: u32,
        tcentroids: &Vec2<S>,
        vector: Borrowed<'_, Self>,
    ) -> Vec<f32> {
        #[inline(never)]
        fn internal<const RATIO: usize, const BITS: usize, S: ScalarLike>(
            dims: usize,
            tcentroids: &[S],
            vector: &[S],
        ) -> Vec<f32> {
            // code below needs special care, any minor changes would result in huge performance degradation
            // For example:
            // * calling `Vec::with_capacity` with parameter `dims.div_ceil(RATIO) * (1 << BITS)`
            // * move `assert!(dims <= 65535)` after allocation
            // * change pointer arithmetic to `get_unchecked` or `std::hint::assert_unchecked`
            // * change parameters from slices to pointers
            assert!(dims <= 65535);
            assert!(tcentroids.len() == dims * (1 << BITS));
            assert!(vector.len() == dims);
            let mut table = Vec::<f32>::with_capacity((dims / RATIO) * (1 << BITS) + (1 << BITS));
            if dims >= 32 {
                // fast path
                for i in 0..dims / RATIO {
                    for j in 0..1 << BITS {
                        let mut value = 0.0f32;
                        for k in 0..RATIO {
                            let idx_x = RATIO * i + k;
                            let idx_y = (RATIO * i + k) * (1 << BITS) + j;
                            let x = unsafe { vector.as_ptr().add(idx_x).read() };
                            let y = unsafe { tcentroids.as_ptr().add(idx_y).read() };
                            let d = x.to_f32() - y.to_f32();
                            value += d * d;
                        }
                        unsafe {
                            table.as_mut_ptr().add(i * (1 << BITS) + j).write(value);
                        }
                    }
                }
                if dims % RATIO != 0 {
                    let i = dims / RATIO;
                    for j in 0..1 << BITS {
                        let mut value = 0.0f32;
                        for k in 0..dims % RATIO {
                            let idx_x = RATIO * i + k;
                            let idx_y = (RATIO * i + k) * (1 << BITS) + j;
                            let x = unsafe { vector.as_ptr().add(idx_x).read() };
                            let y = unsafe { tcentroids.as_ptr().add(idx_y).read() };
                            let d = x.to_f32() - y.to_f32();
                            value += d * d;
                        }
                        unsafe {
                            table.as_mut_ptr().add(i * (1 << BITS) + j).write(value);
                        }
                    }
                }
            } else {
                // slow path
                for i in 0..dims.div_ceil(RATIO) {
                    for j in 0..1 << BITS {
                        let mut value = 0.0f32;
                        for k in 0..std::cmp::min(RATIO, dims - j * RATIO) {
                            let idx_x = RATIO * i + k;
                            let idx_y = (RATIO * i + k) * (1 << BITS) + j;
                            let x = unsafe { vector.as_ptr().add(idx_x).read() };
                            let y = unsafe { tcentroids.as_ptr().add(idx_y).read() };
                            let d = x.to_f32() - y.to_f32();
                            value += d * d;
                        }
                        unsafe {
                            table.as_mut_ptr().add(i * (1 << BITS) + j).write(value);
                        }
                    }
                }
            }
            unsafe {
                table.set_len(dims.div_ceil(RATIO) * (1 << BITS));
            }
            table
        }
        assert!((1..=8).contains(&ratio) && (bits == 1 || bits == 2 || bits == 4 || bits == 8));
        let no = (ratio - 1) * 4 + bits.ilog2();
        match no {
            0 => internal::<1, 1, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            1 => internal::<1, 2, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            2 => internal::<1, 4, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            3 => internal::<1, 8, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            4 => internal::<2, 1, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            5 => internal::<2, 2, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            6 => internal::<2, 4, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            7 => internal::<2, 8, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            8 => internal::<3, 1, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            9 => internal::<3, 2, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            10 => internal::<3, 4, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            11 => internal::<3, 8, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            12 => internal::<4, 1, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            13 => internal::<4, 2, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            14 => internal::<4, 4, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            15 => internal::<4, 8, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            16 => internal::<5, 1, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            17 => internal::<5, 2, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            18 => internal::<5, 4, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            19 => internal::<5, 8, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            20 => internal::<6, 1, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            21 => internal::<6, 2, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            22 => internal::<6, 4, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            23 => internal::<6, 8, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            24 => internal::<7, 1, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            25 => internal::<7, 2, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            26 => internal::<7, 4, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            27 => internal::<7, 8, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            28 => internal::<8, 1, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            29 => internal::<8, 2, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            30 => internal::<8, 4, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            31 => internal::<8, 8, S>(dims as _, tcentroids.as_slice(), vector.slice()),
            32.. => unreachable!(),
        }
    }
    fn process(dims: u32, ratio: u32, bits: u32, lut: &[f32], code: &[u8]) -> Distance {
        #[inline(never)]
        fn internal<const BITS: usize>(n: usize, lut: &[f32], code: &[u8]) -> Distance {
            assert!(n >= 1);
            assert!(n <= 65535);
            assert!(code.len() == n / (8 / BITS));
            assert!(lut.len() == n * (1 << BITS));
            let mut sum = 0.0f32;
            for i in 0..n {
                unsafe {
                    // Safety: `i < n`
                    std::hint::assert_unchecked(i / (8 / BITS) < n / (8 / BITS));
                }
                let (alpha, beta) = (i / (8 / BITS), i % (8 / BITS));
                let j = (code[alpha] >> (beta * BITS)) as usize % (1 << BITS);
                unsafe {
                    // Safety: `i < n`, `j < (1 << BITS)`
                    std::hint::assert_unchecked(i * (1 << BITS) + j < n * (1 << BITS));
                }
                sum += lut[i * (1 << BITS) + j];
            }
            Distance::from(sum)
        }
        match bits {
            1 => internal::<1>(dims.div_ceil(ratio) as _, lut, code),
            2 => internal::<2>(dims.div_ceil(ratio) as _, lut, code),
            4 => internal::<4>(dims.div_ceil(ratio) as _, lut, code),
            8 => internal::<8>(dims.div_ceil(ratio) as _, lut, code),
            _ => unreachable!(),
        }
    }

    fn fscan_preprocess(
        dims: u32,
        ratio: u32,
        bits: u32,
        centroids: &Vec2<S>,
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
            fn code(
                _: u32,
                _: u32,
                _: u32,
                _: &Vec2<Self::Scalar>,
                _: Borrowed<'_, Self>,
            ) -> Vec<u8> {
                unimplemented!()
            }

            fn preprocess(
                _: u32,
                _: u32,
                _: u32,
                _: &Vec2<Self::Scalar>,
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
                _: &Vec2<Self::Scalar>,
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
