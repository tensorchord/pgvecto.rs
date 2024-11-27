use crate::OperatorIndexing;
use base::index::*;
use base::operator::*;
use base::search::*;
use flat::Flat;
use hnsw::Hnsw;
use ivf::Ivf;
use quantization::product::ProductQuantizer;
use quantization::rabitq::RabitqQuantizer;
use quantization::rabitq4::Rabitq4Quantizer;
use quantization::rabitq8::Rabitq8Quantizer;
use quantization::scalar::ScalarQuantizer;
use quantization::scalar4::Scalar4Quantizer;
use quantization::scalar8::Scalar8Quantizer;
use quantization::trivial::TrivialQuantizer;
use sparse_inverted_index::SparseInvertedIndex;
use std::any::Any;
use std::path::Path;

pub enum SealedIndexing<O: OperatorIndexing> {
    Flat(Flat<O, TrivialQuantizer<O>>),
    FlatSq(Flat<O, ScalarQuantizer<O>>),
    FlatPq(Flat<O, ProductQuantizer<O>>),
    FlatRq(Flat<O, RabitqQuantizer<O>>),
    FlatR4q(Flat<O, Rabitq4Quantizer<O>>),
    FlatR8q(Flat<O, Rabitq8Quantizer<O>>),
    FlatS4q(Flat<O, Scalar4Quantizer<O>>),
    FlatS8q(Flat<O, Scalar8Quantizer<O>>),
    Ivf(Ivf<O, TrivialQuantizer<O>>),
    IvfSq(Ivf<O, ScalarQuantizer<O>>),
    IvfPq(Ivf<O, ProductQuantizer<O>>),
    IvfRq(Ivf<O, RabitqQuantizer<O>>),
    IvfR4q(Ivf<O, Rabitq4Quantizer<O>>),
    IvfR8q(Ivf<O, Rabitq8Quantizer<O>>),
    IvfS4q(Ivf<O, Scalar4Quantizer<O>>),
    IvfS8q(Ivf<O, Scalar8Quantizer<O>>),
    Hnsw(Hnsw<O, TrivialQuantizer<O>>),
    HnswSq(Hnsw<O, ScalarQuantizer<O>>),
    HnswPq(Hnsw<O, ProductQuantizer<O>>),
    HnswRq(Hnsw<O, RabitqQuantizer<O>>),
    HnswR4q(Hnsw<O, Rabitq4Quantizer<O>>),
    HnswR8q(Hnsw<O, Rabitq8Quantizer<O>>),
    HnswS4q(Hnsw<O, Scalar4Quantizer<O>>),
    HnswS8q(Hnsw<O, Scalar8Quantizer<O>>),
    SparseInvertedIndex(SparseInvertedIndex<O>),
}

impl<O: OperatorIndexing> SealedIndexing<O> {
    pub fn create(
        path: impl AsRef<Path>,
        options: IndexOptions,
        source: &(impl Vectors<O::Vector> + Collection + Source + Sync),
    ) -> Self {
        match options.indexing {
            IndexingOptions::Flat(FlatIndexingOptions {
                ref quantization, ..
            }) => match quantization {
                None => Self::Flat(Flat::create(path, options, source)),
                Some(QuantizationOptions::Scalar(_)) => {
                    Self::FlatSq(Flat::create(path, options, source))
                }
                Some(QuantizationOptions::Product(_)) => {
                    Self::FlatPq(Flat::create(path, options, source))
                }
                Some(QuantizationOptions::Rabitq(_)) => {
                    Self::FlatRq(Flat::create(path, options, source))
                }
                Some(QuantizationOptions::Rabitq4(_)) => {
                    Self::FlatR4q(Flat::create(path, options, source))
                }
                Some(QuantizationOptions::Rabitq8(_)) => {
                    Self::FlatR8q(Flat::create(path, options, source))
                }
                Some(QuantizationOptions::Scalar4(_)) => {
                    Self::FlatS4q(Flat::create(path, options, source))
                }
                Some(QuantizationOptions::Scalar8(_)) => {
                    Self::FlatS8q(Flat::create(path, options, source))
                }
            },
            IndexingOptions::Ivf(IvfIndexingOptions {
                ref quantization, ..
            }) => match quantization {
                None => Self::Ivf(Ivf::create(path, options, source)),
                Some(QuantizationOptions::Scalar(_)) => {
                    Self::IvfSq(Ivf::create(path, options, source))
                }
                Some(QuantizationOptions::Product(_)) => {
                    Self::IvfPq(Ivf::create(path, options, source))
                }
                Some(QuantizationOptions::Rabitq(_)) => {
                    Self::IvfRq(Ivf::create(path, options, source))
                }
                Some(QuantizationOptions::Rabitq4(_)) => {
                    Self::IvfR4q(Ivf::create(path, options, source))
                }
                Some(QuantizationOptions::Rabitq8(_)) => {
                    Self::IvfR8q(Ivf::create(path, options, source))
                }
                Some(QuantizationOptions::Scalar4(_)) => {
                    Self::IvfS4q(Ivf::create(path, options, source))
                }
                Some(QuantizationOptions::Scalar8(_)) => {
                    Self::IvfS8q(Ivf::create(path, options, source))
                }
            },
            IndexingOptions::Hnsw(HnswIndexingOptions {
                ref quantization, ..
            }) => match quantization {
                None => Self::Hnsw(Hnsw::create(path, options, source)),
                Some(QuantizationOptions::Scalar(_)) => {
                    Self::HnswSq(Hnsw::create(path, options, source))
                }
                Some(QuantizationOptions::Product(_)) => {
                    Self::HnswPq(Hnsw::create(path, options, source))
                }
                Some(QuantizationOptions::Rabitq(_)) => {
                    Self::HnswRq(Hnsw::create(path, options, source))
                }
                Some(QuantizationOptions::Rabitq4(_)) => {
                    Self::HnswR4q(Hnsw::create(path, options, source))
                }
                Some(QuantizationOptions::Rabitq8(_)) => {
                    Self::HnswR8q(Hnsw::create(path, options, source))
                }
                Some(QuantizationOptions::Scalar4(_)) => {
                    Self::HnswS4q(Hnsw::create(path, options, source))
                }
                Some(QuantizationOptions::Scalar8(_)) => {
                    Self::HnswS8q(Hnsw::create(path, options, source))
                }
            },
            IndexingOptions::SparseInvertedIndex(_) => {
                Self::SparseInvertedIndex(SparseInvertedIndex::create(path, options, source))
            }
        }
    }

    pub fn open(path: impl AsRef<Path>, options: IndexOptions) -> Self {
        match options.indexing {
            IndexingOptions::Flat(FlatIndexingOptions {
                ref quantization, ..
            }) => match quantization {
                None => Self::Flat(Flat::open(path)),
                Some(QuantizationOptions::Scalar(_)) => Self::FlatSq(Flat::open(path)),
                Some(QuantizationOptions::Product(_)) => Self::FlatPq(Flat::open(path)),
                Some(QuantizationOptions::Rabitq(_)) => Self::FlatRq(Flat::open(path)),
                Some(QuantizationOptions::Rabitq4(_)) => Self::FlatR4q(Flat::open(path)),
                Some(QuantizationOptions::Rabitq8(_)) => Self::FlatR8q(Flat::open(path)),
                Some(QuantizationOptions::Scalar4(_)) => Self::FlatS4q(Flat::open(path)),
                Some(QuantizationOptions::Scalar8(_)) => Self::FlatS8q(Flat::open(path)),
            },
            IndexingOptions::Ivf(IvfIndexingOptions {
                ref quantization, ..
            }) => match quantization {
                None => Self::Ivf(Ivf::open(path)),
                Some(QuantizationOptions::Scalar(_)) => Self::IvfSq(Ivf::open(path)),
                Some(QuantizationOptions::Product(_)) => Self::IvfPq(Ivf::open(path)),
                Some(QuantizationOptions::Rabitq(_)) => Self::IvfRq(Ivf::open(path)),
                Some(QuantizationOptions::Rabitq4(_)) => Self::IvfR4q(Ivf::open(path)),
                Some(QuantizationOptions::Rabitq8(_)) => Self::IvfR8q(Ivf::open(path)),
                Some(QuantizationOptions::Scalar4(_)) => Self::IvfS4q(Ivf::open(path)),
                Some(QuantizationOptions::Scalar8(_)) => Self::IvfS8q(Ivf::open(path)),
            },
            IndexingOptions::Hnsw(HnswIndexingOptions {
                ref quantization, ..
            }) => match quantization {
                None => Self::Hnsw(Hnsw::open(path)),
                Some(QuantizationOptions::Scalar(_)) => Self::HnswSq(Hnsw::open(path)),
                Some(QuantizationOptions::Product(_)) => Self::HnswPq(Hnsw::open(path)),
                Some(QuantizationOptions::Rabitq(_)) => Self::HnswRq(Hnsw::open(path)),
                Some(QuantizationOptions::Rabitq4(_)) => Self::HnswR4q(Hnsw::open(path)),
                Some(QuantizationOptions::Rabitq8(_)) => Self::HnswR8q(Hnsw::open(path)),
                Some(QuantizationOptions::Scalar4(_)) => Self::HnswS4q(Hnsw::open(path)),
                Some(QuantizationOptions::Scalar8(_)) => Self::HnswS8q(Hnsw::open(path)),
            },
            IndexingOptions::SparseInvertedIndex(_) => {
                Self::SparseInvertedIndex(SparseInvertedIndex::open(path))
            }
        }
    }

    pub fn vbase<'a>(
        &'a self,
        vector: Borrowed<'a, O>,
        opts: &'a SearchOptions,
    ) -> Box<dyn Iterator<Item = Element> + 'a> {
        match self {
            SealedIndexing::Flat(x) => x.vbase(vector, opts),
            SealedIndexing::FlatPq(x) => x.vbase(vector, opts),
            SealedIndexing::FlatSq(x) => x.vbase(vector, opts),
            SealedIndexing::FlatRq(x) => x.vbase(vector, opts),
            SealedIndexing::FlatR4q(x) => x.vbase(vector, opts),
            SealedIndexing::FlatR8q(x) => x.vbase(vector, opts),
            SealedIndexing::FlatS4q(x) => x.vbase(vector, opts),
            SealedIndexing::FlatS8q(x) => x.vbase(vector, opts),

            SealedIndexing::Ivf(x) => x.vbase(vector, opts),
            SealedIndexing::IvfPq(x) => x.vbase(vector, opts),
            SealedIndexing::IvfSq(x) => x.vbase(vector, opts),
            SealedIndexing::IvfRq(x) => x.vbase(vector, opts),
            SealedIndexing::IvfR4q(x) => x.vbase(vector, opts),
            SealedIndexing::IvfR8q(x) => x.vbase(vector, opts),
            SealedIndexing::IvfS4q(x) => x.vbase(vector, opts),
            SealedIndexing::IvfS8q(x) => x.vbase(vector, opts),

            SealedIndexing::Hnsw(x) => x.vbase(vector, opts),
            SealedIndexing::HnswPq(x) => x.vbase(vector, opts),
            SealedIndexing::HnswSq(x) => x.vbase(vector, opts),
            SealedIndexing::HnswRq(x) => x.vbase(vector, opts),
            SealedIndexing::HnswR4q(x) => x.vbase(vector, opts),
            SealedIndexing::HnswR8q(x) => x.vbase(vector, opts),
            SealedIndexing::HnswS4q(x) => x.vbase(vector, opts),
            SealedIndexing::HnswS8q(x) => x.vbase(vector, opts),

            SealedIndexing::SparseInvertedIndex(x) => x.vbase(vector, opts),
        }
    }

    pub fn as_any(&self) -> &dyn Any {
        match &self {
            SealedIndexing::Flat(x) => x,
            SealedIndexing::FlatPq(x) => x,
            SealedIndexing::FlatSq(x) => x,
            SealedIndexing::FlatRq(x) => x,
            SealedIndexing::FlatR4q(x) => x,
            SealedIndexing::FlatR8q(x) => x,
            SealedIndexing::FlatS4q(x) => x,
            SealedIndexing::FlatS8q(x) => x,

            SealedIndexing::Ivf(x) => x,
            SealedIndexing::IvfPq(x) => x,
            SealedIndexing::IvfSq(x) => x,
            SealedIndexing::IvfRq(x) => x,
            SealedIndexing::IvfR4q(x) => x,
            SealedIndexing::IvfR8q(x) => x,
            SealedIndexing::IvfS4q(x) => x,
            SealedIndexing::IvfS8q(x) => x,

            SealedIndexing::Hnsw(x) => x,
            SealedIndexing::HnswPq(x) => x,
            SealedIndexing::HnswSq(x) => x,
            SealedIndexing::HnswRq(x) => x,
            SealedIndexing::HnswR4q(x) => x,
            SealedIndexing::HnswR8q(x) => x,
            SealedIndexing::HnswS4q(x) => x,
            SealedIndexing::HnswS8q(x) => x,

            SealedIndexing::SparseInvertedIndex(x) => x,
        }
    }
}

impl<O: OperatorIndexing> Vectors<O::Vector> for SealedIndexing<O> {
    fn dims(&self) -> u32 {
        match self {
            SealedIndexing::Flat(x) => x.dims(),
            SealedIndexing::FlatSq(x) => x.dims(),
            SealedIndexing::FlatPq(x) => x.dims(),
            SealedIndexing::FlatRq(x) => x.dims(),
            SealedIndexing::FlatR4q(x) => x.dims(),
            SealedIndexing::FlatR8q(x) => x.dims(),
            SealedIndexing::FlatS4q(x) => x.dims(),
            SealedIndexing::FlatS8q(x) => x.dims(),

            SealedIndexing::Ivf(x) => x.dims(),
            SealedIndexing::IvfSq(x) => x.dims(),
            SealedIndexing::IvfPq(x) => x.dims(),
            SealedIndexing::IvfRq(x) => x.dims(),
            SealedIndexing::IvfR4q(x) => x.dims(),
            SealedIndexing::IvfR8q(x) => x.dims(),
            SealedIndexing::IvfS4q(x) => x.dims(),
            SealedIndexing::IvfS8q(x) => x.dims(),

            SealedIndexing::Hnsw(x) => x.dims(),
            SealedIndexing::HnswPq(x) => x.dims(),
            SealedIndexing::HnswSq(x) => x.dims(),
            SealedIndexing::HnswRq(x) => x.dims(),
            SealedIndexing::HnswR4q(x) => x.dims(),
            SealedIndexing::HnswR8q(x) => x.dims(),
            SealedIndexing::HnswS4q(x) => x.dims(),
            SealedIndexing::HnswS8q(x) => x.dims(),

            SealedIndexing::SparseInvertedIndex(x) => x.dims(),
        }
    }

    fn len(&self) -> u32 {
        match self {
            SealedIndexing::Flat(x) => x.len(),
            SealedIndexing::FlatPq(x) => x.len(),
            SealedIndexing::FlatSq(x) => x.len(),
            SealedIndexing::FlatRq(x) => x.len(),
            SealedIndexing::FlatR4q(x) => x.len(),
            SealedIndexing::FlatR8q(x) => x.len(),
            SealedIndexing::FlatS4q(x) => x.len(),
            SealedIndexing::FlatS8q(x) => x.len(),

            SealedIndexing::Ivf(x) => x.len(),
            SealedIndexing::IvfPq(x) => x.len(),
            SealedIndexing::IvfSq(x) => x.len(),
            SealedIndexing::IvfRq(x) => x.len(),
            SealedIndexing::IvfR4q(x) => x.len(),
            SealedIndexing::IvfR8q(x) => x.len(),
            SealedIndexing::IvfS4q(x) => x.len(),
            SealedIndexing::IvfS8q(x) => x.len(),

            SealedIndexing::Hnsw(x) => x.len(),
            SealedIndexing::HnswPq(x) => x.len(),
            SealedIndexing::HnswSq(x) => x.len(),
            SealedIndexing::HnswRq(x) => x.len(),
            SealedIndexing::HnswR4q(x) => x.len(),
            SealedIndexing::HnswR8q(x) => x.len(),
            SealedIndexing::HnswS4q(x) => x.len(),
            SealedIndexing::HnswS8q(x) => x.len(),

            SealedIndexing::SparseInvertedIndex(x) => x.len(),
        }
    }

    fn vector(&self, i: u32) -> Borrowed<'_, O> {
        match self {
            SealedIndexing::Flat(x) => x.vector(i),
            SealedIndexing::FlatPq(x) => x.vector(i),
            SealedIndexing::FlatSq(x) => x.vector(i),
            SealedIndexing::FlatRq(x) => x.vector(i),
            SealedIndexing::FlatR4q(x) => x.vector(i),
            SealedIndexing::FlatR8q(x) => x.vector(i),
            SealedIndexing::FlatS4q(x) => x.vector(i),
            SealedIndexing::FlatS8q(x) => x.vector(i),

            SealedIndexing::Ivf(x) => x.vector(i),
            SealedIndexing::IvfSq(x) => x.vector(i),
            SealedIndexing::IvfPq(x) => x.vector(i),
            SealedIndexing::IvfRq(x) => x.vector(i),
            SealedIndexing::IvfR4q(x) => x.vector(i),
            SealedIndexing::IvfR8q(x) => x.vector(i),
            SealedIndexing::IvfS4q(x) => x.vector(i),
            SealedIndexing::IvfS8q(x) => x.vector(i),

            SealedIndexing::Hnsw(x) => x.vector(i),
            SealedIndexing::HnswSq(x) => x.vector(i),
            SealedIndexing::HnswPq(x) => x.vector(i),
            SealedIndexing::HnswRq(x) => x.vector(i),
            SealedIndexing::HnswR4q(x) => x.vector(i),
            SealedIndexing::HnswR8q(x) => x.vector(i),
            SealedIndexing::HnswS4q(x) => x.vector(i),
            SealedIndexing::HnswS8q(x) => x.vector(i),

            SealedIndexing::SparseInvertedIndex(x) => x.vector(i),
        }
    }
}

impl<O: OperatorIndexing> Collection for SealedIndexing<O> {
    fn payload(&self, i: u32) -> Payload {
        match self {
            SealedIndexing::Flat(x) => x.payload(i),
            SealedIndexing::FlatPq(x) => x.payload(i),
            SealedIndexing::FlatSq(x) => x.payload(i),
            SealedIndexing::FlatRq(x) => x.payload(i),
            SealedIndexing::FlatR4q(x) => x.payload(i),
            SealedIndexing::FlatR8q(x) => x.payload(i),
            SealedIndexing::FlatS4q(x) => x.payload(i),
            SealedIndexing::FlatS8q(x) => x.payload(i),

            SealedIndexing::Ivf(x) => x.payload(i),
            SealedIndexing::IvfPq(x) => x.payload(i),
            SealedIndexing::IvfSq(x) => x.payload(i),
            SealedIndexing::IvfRq(x) => x.payload(i),
            SealedIndexing::IvfR4q(x) => x.payload(i),
            SealedIndexing::IvfR8q(x) => x.payload(i),
            SealedIndexing::IvfS4q(x) => x.payload(i),
            SealedIndexing::IvfS8q(x) => x.payload(i),

            SealedIndexing::Hnsw(x) => x.payload(i),
            SealedIndexing::HnswPq(x) => x.payload(i),
            SealedIndexing::HnswSq(x) => x.payload(i),
            SealedIndexing::HnswRq(x) => x.payload(i),
            SealedIndexing::HnswR4q(x) => x.payload(i),
            SealedIndexing::HnswR8q(x) => x.payload(i),
            SealedIndexing::HnswS4q(x) => x.payload(i),
            SealedIndexing::HnswS8q(x) => x.payload(i),

            SealedIndexing::SparseInvertedIndex(x) => x.payload(i),
        }
    }
}
