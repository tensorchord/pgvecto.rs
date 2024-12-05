use crate::quantizer::Quantizer;
use crate::reranker::errorless::ErrorlessFlatReranker;
use crate::reranker::graph_2::Graph2Reranker;
use base::always_equal::AlwaysEqual;
use base::distance::Distance;
use base::index::*;
use base::operator::*;
use base::search::*;
use base::simd::impossible::Impossible;
use base::simd::quantize;
use base::simd::ScalarLike;
use base::vector::VectBorrowed;
use base::vector::VectorBorrowed;
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::marker::PhantomData;
use std::ops::Range;

const B: usize = 8;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct Scalar8Quantizer<O: OperatorScalar8Quantization> {
    dims: u32,
    _maker: PhantomData<O>,
}

impl<O: OperatorScalar8Quantization> Quantizer<O> for Scalar8Quantizer<O> {
    fn train(
        vector_options: VectorOptions,
        _: Option<QuantizationOptions>,
        _: &(impl Vectors<O::Vector> + Sync),
        _: impl Fn(Borrowed<'_, O>) -> O::Vector + Copy + Sync,
    ) -> Self {
        let dims = vector_options.dims;
        Self {
            dims,
            _maker: PhantomData,
        }
    }

    fn encode(&self, vector: Borrowed<'_, O>) -> Vec<u8> {
        let (a, b, c, d, e) = O::code(vector);
        let mut result = Vec::with_capacity(size_of::<f32>() * 4);
        result.extend(a.to_ne_bytes());
        result.extend(b.to_ne_bytes());
        result.extend(c.to_ne_bytes());
        result.extend(d.to_ne_bytes());
        result.extend(pack(e));
        while result.len() % 4 != 0 {
            result.push(0);
        }
        result
    }

    fn fscan_encode(&self, _vectors: [O::Vector; 32]) -> Vec<u8> {
        Vec::new()
    }

    fn code_size(&self) -> u32 {
        (size_of::<f32>() * 4 + (self.dims as usize).div_ceil(8 / B)).next_multiple_of(4) as _
    }

    fn fcode_size(&self) -> u32 {
        0
    }

    type Lut = O::Lut;

    fn preprocess(&self, vector: Borrowed<'_, O>) -> Self::Lut {
        O::preprocess(vector)
    }

    fn process(&self, lut: &Self::Lut, code: &[u8], _: Borrowed<'_, O>) -> Distance {
        let c = parse_code(code, self.dims);
        O::process(self.dims, lut, c)
    }

    fn project(&self, vector: Borrowed<'_, O>) -> O::Vector {
        vector.own()
    }

    type FLut = std::convert::Infallible;

    fn fscan_preprocess(&self, _: Borrowed<'_, O>) -> Self::FLut {
        unimplemented!()
    }

    fn fscan_process(&self, _: &Self::FLut, _: &[u8]) -> [Distance; 32] {
        unimplemented!()
    }

    type FlatRerankVec = Vec<(Reverse<Distance>, AlwaysEqual<u32>)>;

    fn flat_rerank_start() -> Self::FlatRerankVec {
        Vec::new()
    }

    fn flat_rerank_preprocess(
        &self,
        vector: Borrowed<'_, O>,
        _: &SearchOptions,
    ) -> Result<Self::FLut, Self::Lut> {
        Err(self.preprocess(vector))
    }

    fn flat_rerank_continue<C>(
        &self,
        locate_0: impl Fn(u32) -> C,
        _: impl Fn(u32) -> C,
        frlut: &Result<Self::FLut, Self::Lut>,
        range: Range<u32>,
        heap: &mut Self::FlatRerankVec,
    ) where
        C: AsRef<[u8]>,
    {
        match frlut {
            Ok(flut) => match *flut {},
            Err(lut) => {
                for j in range {
                    let c = locate_0(j);
                    let c = parse_code(c.as_ref(), self.dims);
                    let r = O::process(self.dims, lut, c);
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
        ErrorlessFlatReranker::new(heap, rerank)
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
            move |u| O::process(self.dims, &lut, parse_code(locate(u).as_ref(), self.dims)),
            rerank,
        )
    }
}

pub trait OperatorScalar8Quantization: Operator {
    type Scalar: ScalarLike;

    fn code(vector: Borrowed<'_, Self>) -> (f32, f32, f32, f32, Vec<u8>);

    type Lut;
    fn preprocess(vector: Borrowed<'_, Self>) -> Self::Lut;
    fn process(dims: u32, lut: &Self::Lut, code: (f32, f32, f32, f32, &[u8])) -> Distance;
}

impl<S: ScalarLike> OperatorScalar8Quantization for VectL2<S> {
    type Scalar = S;

    fn code(vector: Borrowed<'_, Self>) -> (f32, f32, f32, f32, Vec<u8>) {
        make_code(vector)
    }

    type Lut = (f32, f32, f32, f32, Vec<u8>);

    fn preprocess(vector: Borrowed<'_, Self>) -> Self::Lut {
        let (sum_of_x2, sum_of_code, k, b, code) = make_code(vector);
        (sum_of_x2, sum_of_code, k, b, pack(code))
    }

    fn process(
        dims: u32,
        lut: &Self::Lut,
        (sum_of_x2_u, sum_of_code_u, k_u, b_u, t): (f32, f32, f32, f32, &[u8]),
    ) -> Distance {
        let &(sum_of_x2_v, sum_of_code_v, k_v, b_v, ref s) = lut;
        let value = base::simd::u8::reduce_sum_of_xy(s, t);
        let ip = k_u * k_v * value as f32
            + k_u * b_v * sum_of_code_u
            + k_v * b_u * sum_of_code_v
            + dims as f32 * b_u * b_v;
        let rough = sum_of_x2_u + sum_of_x2_v - 2.0 * ip;
        Distance::from_f32(rough)
    }
}

impl<S: ScalarLike> OperatorScalar8Quantization for VectDot<S> {
    type Scalar = S;

    fn code(vector: Borrowed<'_, Self>) -> (f32, f32, f32, f32, Vec<u8>) {
        make_code(vector)
    }

    type Lut = (f32, f32, f32, f32, Vec<u8>);

    fn preprocess(vector: Borrowed<'_, Self>) -> Self::Lut {
        let (sum_of_x2, sum_of_code, k, b, code) = make_code(vector);
        (sum_of_x2, sum_of_code, k, b, pack(code))
    }

    fn process(
        dims: u32,
        lut: &Self::Lut,
        (_, sum_of_code_u, k_u, b_u, t): (f32, f32, f32, f32, &[u8]),
    ) -> Distance {
        let &(_, sum_of_code_v, k_v, b_v, ref s) = lut;
        let value = base::simd::u8::reduce_sum_of_xy(s, t);
        let ip = k_u * k_v * value as f32
            + k_u * b_v * sum_of_code_u
            + k_v * b_u * sum_of_code_v
            + dims as f32 * b_u * b_v;
        let rough = -ip;
        Distance::from_f32(rough)
    }
}

macro_rules! unimpl_operator_rabitq_quantization {
    ($t:ty) => {
        impl OperatorScalar8Quantization for $t {
            type Scalar = Impossible;

            fn code(_: Borrowed<'_, Self>) -> (f32, f32, f32, f32, Vec<u8>) {
                unimplemented!()
            }

            type Lut = std::convert::Infallible;
            fn preprocess(_: Borrowed<'_, Self>) -> Self::Lut {
                unimplemented!()
            }
            fn process(_: u32, _: &Self::Lut, _: (f32, f32, f32, f32, &[u8])) -> Distance {
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

pub fn make_code<S: ScalarLike>(vector: VectBorrowed<'_, S>) -> (f32, f32, f32, f32, Vec<u8>) {
    let dims = vector.dims();
    let vector = vector.slice();
    let sum_of_x2 = S::reduce_sum_of_x2(vector);
    let (k, b, code) = quantize::quantize(
        S::vector_to_f32_borrowed(vector).as_ref(),
        ((1 << B) - 1) as _,
    );
    let sum_of_code = {
        let mut y = 0;
        for i in 0..dims {
            let x = code[i as usize] as u32;
            y += x;
        }
        y as f32
    };
    (sum_of_x2, sum_of_code, k, b, code)
}

fn parse_code(code: &[u8], dims: u32) -> (f32, f32, f32, f32, &[u8]) {
    let a = f32::from_ne_bytes([code[0], code[1], code[2], code[3]]);
    let b = f32::from_ne_bytes([code[4], code[5], code[6], code[7]]);
    let c = f32::from_ne_bytes([code[8], code[9], code[10], code[11]]);
    let d = f32::from_ne_bytes([code[12], code[13], code[14], code[15]]);
    (a, b, c, d, &code[16..][..(dims as usize).div_ceil(8 / B)])
}

fn pack(x: Vec<u8>) -> Vec<u8> {
    x
}
