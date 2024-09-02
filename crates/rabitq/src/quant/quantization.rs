use super::quantizer::RabitqQuantizer;
use crate::operator::OperatorRabitq;
use base::always_equal::AlwaysEqual;
use base::distance::Distance;
use base::index::VectorOptions;
use base::search::RerankerPop;
use common::json::Json;
use common::mmap_array::MmapArray;
use quantization::utils::InfiniteByteChunks;
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::ops::Range;
use std::path::Path;

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
            <O as OperatorRabitq>::Params,
            <O as OperatorRabitq>::Preprocessed,
        ),
    ),
}

impl<O: OperatorRabitq> From<QuantizationPreprocessed<O>> for QuantizationAnyPreprocessed<O> {
    fn from(value: QuantizationPreprocessed<O>) -> Self {
        match value {
            QuantizationPreprocessed::Rabitq((param, blut)) => Self::Rabitq((param, Ok(blut))),
        }
    }
}

pub enum QuantizationFscanPreprocessed<O: OperatorRabitq> {
    Rabitq((<O as OperatorRabitq>::Params, Vec<u8>)),
}

impl<O: OperatorRabitq> From<QuantizationFscanPreprocessed<O>> for QuantizationAnyPreprocessed<O> {
    fn from(value: QuantizationFscanPreprocessed<O>) -> Self {
        match value {
            QuantizationFscanPreprocessed::Rabitq((param, lut)) => Self::Rabitq((param, Err(lut))),
        }
    }
}

pub enum QuantizationAnyPreprocessed<O: OperatorRabitq> {
    Rabitq(
        (
            <O as OperatorRabitq>::Params,
            Result<<O as OperatorRabitq>::Preprocessed, Vec<u8>>,
        ),
    ),
}

pub struct Quantization<O: OperatorRabitq> {
    train: Json<Quantizer<O>>,
    codes: MmapArray<u8>,
    packed_codes: MmapArray<u8>,
    meta: MmapArray<f32>,
}

impl<O: OperatorRabitq> Quantization<O> {
    pub fn create(
        path: impl AsRef<Path>,
        vector_options: VectorOptions,
        n: u32,
        vectors: impl Fn(u32) -> Vec<f32>,
    ) -> Self {
        std::fs::create_dir(path.as_ref()).unwrap();
        fn merge_8([b0, b1, b2, b3, b4, b5, b6, b7]: [u8; 8]) -> u8 {
            b0 | (b1 << 1) | (b2 << 2) | (b3 << 3) | (b4 << 4) | (b5 << 5) | (b6 << 6) | (b7 << 7)
        }
        fn merge_4([b0, b1, b2, b3]: [u8; 4]) -> u8 {
            b0 | (b1 << 2) | (b2 << 4) | (b3 << 6)
        }
        fn merge_2([b0, b1]: [u8; 2]) -> u8 {
            b0 | (b1 << 4)
        }
        let train = Quantizer::train(vector_options);
        let train = Json::create(path.as_ref().join("train"), train);
        let codes = MmapArray::create(path.as_ref().join("codes"), {
            match &*train {
                Quantizer::Rabitq(x) => Box::new((0..n).flat_map(|i| {
                    let vector = vectors(i);
                    let codes = x.encode(&vector);
                    let bytes = x.bytes();
                    match x.bits() {
                        1 => InfiniteByteChunks::new(codes.into_iter())
                            .map(merge_8)
                            .take(bytes as usize)
                            .collect(),
                        2 => InfiniteByteChunks::new(codes.into_iter())
                            .map(merge_4)
                            .take(bytes as usize)
                            .collect(),
                        4 => InfiniteByteChunks::new(codes.into_iter())
                            .map(merge_2)
                            .take(bytes as usize)
                            .collect(),
                        8 => codes,
                        _ => unreachable!(),
                    }
                })),
            }
        });
        let packed_codes = MmapArray::create(
            path.as_ref().join("packed_codes"),
            match &*train {
                Quantizer::Rabitq(x) => {
                    use quantization::fast_scan::b4::{pack, BLOCK_SIZE};
                    let blocks = n.div_ceil(BLOCK_SIZE);
                    Box::new((0..blocks).flat_map(|block| {
                        let t = x.dims().div_ceil(4);
                        let raw = std::array::from_fn::<_, { BLOCK_SIZE as _ }, _>(|i| {
                            let id = BLOCK_SIZE * block + i as u32;
                            let e = x.encode(&vectors(std::cmp::min(id, n - 1)));
                            InfiniteByteChunks::new(e.into_iter())
                                .map(|[b0, b1, b2, b3]| b0 | b1 << 1 | b2 << 2 | b3 << 3)
                                .take(t as usize)
                                .collect()
                        });
                        pack(t, raw)
                    })) as Box<dyn Iterator<Item = u8>>
                }
            },
        );
        let meta = MmapArray::create(
            path.as_ref().join("meta"),
            match &*train {
                Quantizer::Rabitq(x) => Box::new((0..n).flat_map(|i| {
                    let (a, b, c, d) = x.encode_meta(&vectors(i));
                    [a, b, c, d].into_iter()
                })),
            },
        );
        Self {
            train,
            codes,
            packed_codes,
            meta,
        }
    }

    pub fn open(path: impl AsRef<Path>) -> Self {
        let train = Json::open(path.as_ref().join("train"));
        let codes = MmapArray::open(path.as_ref().join("codes"));
        let packed_codes = MmapArray::open(path.as_ref().join("packed_codes"));
        let meta = MmapArray::open(path.as_ref().join("meta"));
        Self {
            train,
            codes,
            packed_codes,
            meta,
        }
    }

    pub fn preprocess(&self, lhs: &[f32]) -> QuantizationPreprocessed<O> {
        match &*self.train {
            Quantizer::Rabitq(x) => QuantizationPreprocessed::Rabitq(x.preprocess(lhs)),
        }
    }

    pub fn fscan_preprocess(&self, lhs: &[f32]) -> QuantizationFscanPreprocessed<O> {
        match &*self.train {
            Quantizer::Rabitq(x) => QuantizationFscanPreprocessed::Rabitq(x.fscan_preprocess(lhs)),
        }
    }

    pub fn process(&self, preprocessed: &QuantizationPreprocessed<O>, u: u32) -> Distance {
        match (&*self.train, preprocessed) {
            (Quantizer::Rabitq(x), QuantizationPreprocessed::Rabitq(lhs)) => {
                let bytes = x.bytes() as usize;
                let start = u as usize * bytes;
                let end = start + bytes;
                let a = self.meta[4 * u as usize + 0];
                let b = self.meta[4 * u as usize + 1];
                let c = self.meta[4 * u as usize + 2];
                let d = self.meta[4 * u as usize + 3];
                let codes = &self.codes[start..end];
                x.process(&lhs.0, &lhs.1, (a, b, c, d, codes))
            }
        }
    }

    pub fn push_batch(
        &self,
        preprocessed: &QuantizationAnyPreprocessed<O>,
        rhs: Range<u32>,
        heap: &mut Vec<(Reverse<Distance>, AlwaysEqual<u32>)>,
        rq_epsilon: f32,
    ) {
        match (&*self.train, preprocessed) {
            (Quantizer::Rabitq(x), QuantizationAnyPreprocessed::Rabitq((a, b))) => x.push_batch(
                a,
                b,
                rhs,
                heap,
                &self.codes,
                &self.packed_codes,
                &self.meta,
                rq_epsilon,
            ),
        }
    }

    pub fn rerank<'a, T: 'a>(
        &'a self,
        heap: Vec<(Reverse<Distance>, AlwaysEqual<u32>)>,
        r: impl Fn(u32) -> (Distance, T) + 'a,
    ) -> impl RerankerPop<T> + 'a {
        use Quantizer::*;
        match &*self.train {
            Rabitq(x) => x.rerank(heap, r),
        }
    }
}
