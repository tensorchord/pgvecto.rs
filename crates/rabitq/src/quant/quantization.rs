use super::quantizer::RabitqQuantizer;
use crate::operator::OperatorRabitq;
use base::always_equal::AlwaysEqual;
use base::index::VectorOptions;
use base::scalar::F32;
use base::search::RerankerPop;
use common::json::Json;
use common::mmap_array::MmapArray;
use quantization::utils::InfiniteByteChunks;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::ops::Range;
use std::path::Path;
use stoppable_rayon as rayon;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound = "")]
pub enum Quantizer<O: OperatorRabitq> {
    Rabitq(RabitqQuantizer<O>),
}

impl<O: OperatorRabitq> Quantizer<O> {
    pub fn train(vector_options: VectorOptions) -> Self {
        Self::Rabitq(RabitqQuantizer::train(vector_options))
    }
}

pub enum QuantizationPreprocessed<O: OperatorRabitq> {
    Rabitq(
        (
            <O as OperatorRabitq>::QuantizationPreprocessed0,
            <O as OperatorRabitq>::QuantizationPreprocessed1,
        ),
    ),
}

pub struct Quantization<O: OperatorRabitq> {
    train: Json<Quantizer<O>>,
    packed_codes: MmapArray<u8>,
    meta_a: MmapArray<F32>,
    meta_b: MmapArray<F32>,
    meta_c: MmapArray<F32>,
    meta_d: MmapArray<F32>,
}

impl<O: OperatorRabitq> Quantization<O> {
    pub fn create(
        path: impl AsRef<Path>,
        vector_options: VectorOptions,
        n: u32,
        vectors: impl Fn(u32) -> Vec<F32> + Sync,
    ) -> Self {
        std::fs::create_dir(path.as_ref()).unwrap();
        let train = Quantizer::train(vector_options);
        let everything = match &train {
            Quantizer::Rabitq(x) => (0..n)
                .into_par_iter()
                .map(|i| x.encode(&vectors(i)))
                .collect::<Vec<_>>(),
        };
        let path = path.as_ref().to_path_buf();
        let packed_codes = MmapArray::create(
            path.join("packed_codes"),
            match &train {
                Quantizer::Rabitq(x) => {
                    use quantization::fast_scan::b4::{pack, BLOCK_SIZE};
                    let blocks = n.div_ceil(BLOCK_SIZE);
                    (0..blocks).flat_map(|block| {
                        let t = x.dims().div_ceil(4);
                        let raw = std::array::from_fn::<_, { BLOCK_SIZE as _ }, _>(|offset| {
                            let i = BLOCK_SIZE * block + offset as u32;
                            let (_, _, _, _, codes) =
                                everything[std::cmp::min(i, n - 1) as usize].clone();
                            InfiniteByteChunks::new(codes.into_iter())
                                .map(|[b0, b1, b2, b3]| b0 | b1 << 1 | b2 << 2 | b3 << 3)
                                .take(t as usize)
                                .collect()
                        });
                        pack(t, raw)
                    })
                }
            },
        );
        let meta_a = MmapArray::create(
            path.join("meta_a"),
            (0..n).map(|i| everything[i as usize].0),
        );
        let meta_b = MmapArray::create(
            path.join("meta_b"),
            (0..n).map(|i| everything[i as usize].1),
        );
        let meta_c = MmapArray::create(
            path.join("meta_c"),
            (0..n).map(|i| everything[i as usize].2),
        );
        let meta_d = MmapArray::create(
            path.join("meta_d"),
            (0..n).map(|i| everything[i as usize].3),
        );
        let train = Json::create(path.join("train"), train);
        Self {
            train,
            packed_codes,
            meta_a,
            meta_b,
            meta_c,
            meta_d,
        }
    }

    pub fn open(path: impl AsRef<Path>) -> Self {
        let train = Json::open(path.as_ref().join("train"));
        let packed_codes = MmapArray::open(path.as_ref().join("packed_codes"));
        let meta_a = MmapArray::open(path.as_ref().join("meta_a"));
        let meta_b = MmapArray::open(path.as_ref().join("meta_b"));
        let meta_c = MmapArray::open(path.as_ref().join("meta_c"));
        let meta_d = MmapArray::open(path.as_ref().join("meta_d"));
        Self {
            train,
            packed_codes,
            meta_a,
            meta_b,
            meta_c,
            meta_d,
        }
    }

    pub fn preprocess(&self, dist: F32, lhs: &[F32],centroid: &[F32],) -> QuantizationPreprocessed<O> {
        match &*self.train {
            Quantizer::Rabitq(x) => QuantizationPreprocessed::Rabitq(x.preprocess(dist, lhs, centroid)),
        }
    }

    pub fn push_batch(
        &self,
        preprocessed: &QuantizationPreprocessed<O>,
        rhs: Range<u32>,
        result: &mut BinaryHeap<(i32, AlwaysEqual<u32>, ())>,
        rerank: impl Fn(u32) -> (F32, ()),
        rq_fast_scan: bool,
        hint: impl Fn(u32) -> F32,
    ) {
        match (&*self.train, preprocessed) {
            (Quantizer::Rabitq(x), QuantizationPreprocessed::Rabitq(lhs)) => x.push_batch(
                lhs,
                rhs,
                result,
                rerank,
                &self.packed_codes,
                &self.meta_a,
                &self.meta_b,
                &self.meta_c,
                &self.meta_d,
                rq_fast_scan,
                hint,
            ),
        }
    }

    pub fn rerank<'a, T: 'a>(
        &'a self,
        heap: Vec<(Reverse<F32>, AlwaysEqual<u32>)>,
        r: impl Fn(u32) -> (F32, T) + 'a,
    ) -> impl RerankerPop<T> + 'a {
        use Quantizer::*;
        match &*self.train {
            Rabitq(x) => x.rerank(heap, r),
        }
    }
}
