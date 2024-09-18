#![feature(avx512_target_feature)]
#![cfg_attr(target_arch = "x86_64", feature(stdarch_x86_avx512))]
#![allow(clippy::identity_op)]
#![allow(clippy::needless_range_loop)]
#![allow(clippy::type_complexity)]
#![allow(clippy::too_many_arguments)]

pub mod fast_scan;
pub mod product;
pub mod quantize;
pub mod quantizer;
pub mod reranker;
pub mod scalar;
pub mod trivial;
pub mod utils;

use base::distance::Distance;
use base::index::*;
use base::operator::*;
use base::search::*;
use base::vector::VectorOwned;
use common::json::Json;
use common::mmap_array::MmapArray;
use quantizer::Quantizer;
use std::marker::PhantomData;
use std::ops::Range;
use std::path::Path;

pub struct Quantization<O, Q> {
    train: Json<Q>,
    codes: MmapArray<u8>,
    packed_codes: MmapArray<u8>,
    _maker: PhantomData<fn(O) -> O>,
}

impl<O: Operator, Q: Quantizer<O>> Quantization<O, Q> {
    pub fn create(
        path: impl AsRef<Path>,
        vector_options: VectorOptions,
        quantization_options: Option<QuantizationOptions>,
        vectors: &(impl Vectors<Owned<O>> + Sync),
        transform: impl Fn(Borrowed<'_, O>) -> Owned<O> + Copy + Send + Sync,
    ) -> Self {
        std::fs::create_dir(path.as_ref()).unwrap();
        let train = Q::train(vector_options, quantization_options, vectors, transform);
        let train = Json::create(path.as_ref().join("train"), train);
        let codes = MmapArray::create(path.as_ref().join("codes"), {
            (0..vectors.len()).flat_map(|i| {
                let vector = transform(vectors.vector(i));
                train.encode(vector.as_borrowed())
            })
        });
        let packed_codes = MmapArray::create(path.as_ref().join("packed_codes"), {
            let d = vectors.dims();
            let n = vectors.len();
            let m = n.div_ceil(32);
            let train = &train;
            (0..m).flat_map(move |alpha| {
                let vectors = std::array::from_fn(|beta| {
                    let i = 32 * alpha + beta as u32;
                    if i < n {
                        transform(vectors.vector(i))
                    } else {
                        O::Vector::zero(d)
                    }
                });
                train.fscan_encode(vectors)
            })
        });
        Self {
            train,
            codes,
            packed_codes,
            _maker: PhantomData,
        }
    }

    pub fn open(path: impl AsRef<Path>) -> Self {
        let train = Json::open(path.as_ref().join("train"));
        let codes = MmapArray::open(path.as_ref().join("codes"));
        let packed_codes = MmapArray::open(path.as_ref().join("packed_codes"));
        Self {
            train,
            codes,
            packed_codes,
            _maker: PhantomData,
        }
    }

    pub fn preprocess(&self, vector: Borrowed<'_, O>) -> Q::Lut {
        Q::preprocess(&self.train, vector)
    }

    pub fn flat_rerank_preprocess(
        &self,
        vector: Borrowed<'_, O>,
        opts: &SearchOptions,
    ) -> Result<Q::FLut, Q::Lut> {
        Q::flat_rerank_preprocess(&self.train, vector, opts)
    }

    pub fn process(&self, vectors: &impl Vectors<Owned<O>>, lut: &Q::Lut, u: u32) -> Distance {
        let locate = |i| {
            let code_size = self.train.code_size() as usize;
            let start = i as usize * code_size;
            let end = start + code_size;
            &self.codes[start..end]
        };
        let vector = vectors.vector(u);
        Q::process(&self.train, lut, locate(u), vector)
    }

    pub fn flat_rerank_continue(
        &self,
        frlut: &Result<Q::FLut, Q::Lut>,
        range: Range<u32>,
        heap: &mut Q::FlatRerankVec,
    ) {
        Q::flat_rerank_continue(
            &self.train,
            |i| {
                let code_size = self.train.code_size() as usize;
                let start = i as usize * code_size;
                let end = start + code_size;
                &self.codes[start..end]
            },
            |i| {
                let fcode_size = self.train.fcode_size() as usize;
                let start = i as usize * fcode_size;
                let end = start + fcode_size;
                &self.packed_codes[start..end]
            },
            frlut,
            range,
            heap,
        )
    }

    pub fn flat_rerank_break<'a, 'b, T: 'a, R>(
        &'a self,
        heap: Q::FlatRerankVec,
        rerank: R,
        opts: &'b SearchOptions,
    ) -> impl RerankerPop<T> + 'a + use<'a, 'b, T, O, Q, R>
    where
        R: Fn(u32) -> (Distance, T) + 'a,
    {
        Q::flat_rerank_break(&self.train, heap, rerank, opts)
    }

    pub fn graph_rerank<'a, T: 'a, R: Fn(u32) -> (Distance, T) + 'a>(
        &'a self,
        vector: Borrowed<'a, O>,
        rerank: R,
    ) -> impl RerankerPush + RerankerPop<T> + 'a {
        Q::graph_rerank(
            &self.train,
            |i| {
                let code_size = self.train.code_size() as usize;
                let start = i as usize * code_size;
                let end = start + code_size;
                &self.codes[start..end]
            },
            vector,
            rerank,
        )
    }
}
