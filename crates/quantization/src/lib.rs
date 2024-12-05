#![allow(clippy::identity_op)]
#![allow(clippy::needless_range_loop)]
#![allow(clippy::type_complexity)]
#![allow(clippy::too_many_arguments)]

pub mod product;
pub mod quantizer;
pub mod rabitq;
pub mod rabitq4;
pub mod rabitq8;
pub mod reranker;
pub mod scalar;
pub mod scalar4;
pub mod scalar8;
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
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use std::marker::PhantomData;
use std::ops::Range;
use std::path::Path;
use stoppable_rayon as rayon;

pub struct Quantization<O, Q> {
    quantizer: Json<Q>,
    codes: MmapArray<u8>,
    fcodes: MmapArray<u8>,
    _maker: PhantomData<fn(O) -> O>,
}

impl<O: Operator, Q: Quantizer<O>> Quantization<O, Q> {
    pub fn create(
        path: impl AsRef<Path>,
        vector_options: VectorOptions,
        quantization_options: Option<QuantizationOptions>,
        vectors: &(impl Vectors<O::Vector> + Sync),
        transform: impl Fn(Borrowed<'_, O>) -> O::Vector + Copy + Send + Sync,
    ) -> Self {
        std::fs::create_dir(path.as_ref()).unwrap();
        let quantizer = Json::create(
            path.as_ref().join("quantizer"),
            Q::train(vector_options, quantization_options, vectors, transform),
        );
        let codes = MmapArray::create(path.as_ref().join("codes"), {
            (0..vectors.len())
                .into_par_iter()
                .map(|i| {
                    let vector = quantizer.project(transform(vectors.vector(i)).as_borrowed());
                    quantizer.encode(vector.as_borrowed())
                })
                .collect::<Vec<_>>()
                .into_iter()
                .flatten()
        });
        let fcodes = MmapArray::create(path.as_ref().join("fcodes"), {
            let d = vectors.dims();
            let n = vectors.len();
            let m = n.div_ceil(32);
            let train = &quantizer;
            (0..m)
                .into_par_iter()
                .map(move |alpha| {
                    let vectors = std::array::from_fn(|beta| {
                        let i = 32 * alpha + beta as u32;
                        if i < n {
                            train.project(transform(vectors.vector(i)).as_borrowed())
                        } else {
                            O::Vector::zero(d)
                        }
                    });
                    train.fscan_encode(vectors)
                })
                .collect::<Vec<_>>()
                .into_iter()
                .flatten()
        });
        Self {
            quantizer,
            codes,
            fcodes,
            _maker: PhantomData,
        }
    }

    pub fn open(path: impl AsRef<Path>) -> Self {
        let quantizer = Json::open(path.as_ref().join("quantizer"));
        let codes = MmapArray::open(path.as_ref().join("codes"));
        let fcodes = MmapArray::open(path.as_ref().join("fcodes"));
        Self {
            quantizer,
            codes,
            fcodes,
            _maker: PhantomData,
        }
    }

    pub fn quantizer(&self) -> &Q {
        &self.quantizer
    }

    pub fn project(&self, vector: Borrowed<'_, O>) -> O::Vector {
        Q::project(&self.quantizer, vector)
    }

    pub fn preprocess(&self, vector: Borrowed<'_, O>) -> Q::Lut {
        Q::preprocess(&self.quantizer, vector)
    }

    pub fn flat_rerank_preprocess(
        &self,
        vector: Borrowed<'_, O>,
        opts: &SearchOptions,
    ) -> Result<Q::FLut, Q::Lut> {
        Q::flat_rerank_preprocess(&self.quantizer, vector, opts)
    }

    pub fn process(&self, vectors: &impl Vectors<O::Vector>, lut: &Q::Lut, u: u32) -> Distance {
        let locate = |i| {
            let code_size = self.quantizer.code_size() as usize;
            let start = i as usize * code_size;
            let end = start + code_size;
            &self.codes[start..end]
        };
        let vector = vectors.vector(u);
        Q::process(&self.quantizer, lut, locate(u), vector)
    }

    pub fn flat_rerank_continue(
        &self,
        frlut: &Result<Q::FLut, Q::Lut>,
        range: Range<u32>,
        heap: &mut Q::FlatRerankVec,
    ) {
        Q::flat_rerank_continue(
            &self.quantizer,
            |i| {
                let code_size = self.quantizer.code_size() as usize;
                let start = i as usize * code_size;
                let end = start + code_size;
                &self.codes[start..end]
            },
            |i| {
                let fcode_size = self.quantizer.fcode_size() as usize;
                let start = i as usize * fcode_size;
                let end = start + fcode_size;
                &self.fcodes[start..end]
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
        Q::flat_rerank_break(&self.quantizer, heap, rerank, opts)
    }

    pub fn graph_rerank<'a, T: 'a, R: Fn(u32) -> (Distance, T) + 'a>(
        &'a self,
        lut: Q::Lut,
        rerank: R,
    ) -> impl RerankerPush + RerankerPop<T> + 'a {
        Q::graph_rerank(
            &self.quantizer,
            lut,
            |i| {
                let code_size = self.quantizer.code_size() as usize;
                let start = i as usize * code_size;
                let end = start + code_size;
                &self.codes[start..end]
            },
            rerank,
        )
    }
}
