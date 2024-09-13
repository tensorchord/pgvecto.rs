use crate::OperatorIndexing;
use base::index::*;
use base::operator::*;
use base::search::*;
use flat::Flat;
use hnsw::Hnsw;
use inverted::InvertedIndex;
use ivf::Ivf;
use quantization::product::ProductQuantizer;
use quantization::scalar::ScalarQuantizer;
use quantization::trivial::TrivialQuantizer;
use rabitq::Rabitq;
use std::path::Path;

pub enum SealedIndexing<O: OperatorIndexing> {
    Flat(Flat<O, TrivialQuantizer<O>>),
    FlatSq(Flat<O, ScalarQuantizer<O>>),
    FlatPq(Flat<O, ProductQuantizer<O>>),
    Ivf(Ivf<O, TrivialQuantizer<O>>),
    IvfSq(Ivf<O, ScalarQuantizer<O>>),
    IvfPq(Ivf<O, ProductQuantizer<O>>),
    Hnsw(Hnsw<O, TrivialQuantizer<O>>),
    HnswSq(Hnsw<O, ScalarQuantizer<O>>),
    HnswPq(Hnsw<O, ProductQuantizer<O>>),
    InvertedIndex(InvertedIndex<O>),
    Rabitq(Rabitq<O>),
}

impl<O: OperatorIndexing> SealedIndexing<O> {
    pub fn create(
        path: impl AsRef<Path>,
        options: IndexOptions,
        source: &(impl Vectors<Owned<O>> + Collection + Source + Sync),
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
            },
            IndexingOptions::InvertedIndex(_) => {
                Self::InvertedIndex(InvertedIndex::create(path, options, source))
            }
            IndexingOptions::Rabitq(_) => Self::Rabitq(Rabitq::create(path, options, source)),
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
            },
            IndexingOptions::Ivf(IvfIndexingOptions {
                ref quantization, ..
            }) => match quantization {
                None => Self::Ivf(Ivf::open(path)),
                Some(QuantizationOptions::Scalar(_)) => Self::IvfSq(Ivf::open(path)),
                Some(QuantizationOptions::Product(_)) => Self::IvfPq(Ivf::open(path)),
            },
            IndexingOptions::Hnsw(HnswIndexingOptions {
                ref quantization, ..
            }) => match quantization {
                None => Self::Hnsw(Hnsw::open(path)),
                Some(QuantizationOptions::Scalar(_)) => Self::HnswSq(Hnsw::open(path)),
                Some(QuantizationOptions::Product(_)) => Self::HnswPq(Hnsw::open(path)),
            },
            IndexingOptions::InvertedIndex(_) => Self::InvertedIndex(InvertedIndex::open(path)),
            IndexingOptions::Rabitq(_) => Self::Rabitq(Rabitq::open(path)),
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
            SealedIndexing::Ivf(x) => x.vbase(vector, opts),
            SealedIndexing::IvfPq(x) => x.vbase(vector, opts),
            SealedIndexing::IvfSq(x) => x.vbase(vector, opts),
            SealedIndexing::Hnsw(x) => x.vbase(vector, opts),
            SealedIndexing::HnswPq(x) => x.vbase(vector, opts),
            SealedIndexing::HnswSq(x) => x.vbase(vector, opts),
            SealedIndexing::InvertedIndex(x) => x.vbase(vector, opts),
            SealedIndexing::Rabitq(x) => x.vbase(vector, opts),
        }
    }
}

impl<O: OperatorIndexing> Vectors<Owned<O>> for SealedIndexing<O> {
    fn dims(&self) -> u32 {
        match self {
            SealedIndexing::Flat(x) => x.dims(),
            SealedIndexing::FlatSq(x) => x.dims(),
            SealedIndexing::FlatPq(x) => x.dims(),
            SealedIndexing::Ivf(x) => x.dims(),
            SealedIndexing::IvfSq(x) => x.dims(),
            SealedIndexing::IvfPq(x) => x.dims(),
            SealedIndexing::Hnsw(x) => x.dims(),
            SealedIndexing::HnswPq(x) => x.dims(),
            SealedIndexing::HnswSq(x) => x.dims(),
            SealedIndexing::InvertedIndex(x) => x.dims(),
            SealedIndexing::Rabitq(x) => x.dims(),
        }
    }

    fn len(&self) -> u32 {
        match self {
            SealedIndexing::Flat(x) => x.len(),
            SealedIndexing::FlatPq(x) => x.len(),
            SealedIndexing::FlatSq(x) => x.len(),
            SealedIndexing::Ivf(x) => x.len(),
            SealedIndexing::IvfPq(x) => x.len(),
            SealedIndexing::IvfSq(x) => x.len(),
            SealedIndexing::Hnsw(x) => x.len(),
            SealedIndexing::HnswPq(x) => x.len(),
            SealedIndexing::HnswSq(x) => x.len(),
            SealedIndexing::InvertedIndex(x) => x.len(),
            SealedIndexing::Rabitq(x) => x.len(),
        }
    }

    fn vector(&self, i: u32) -> Borrowed<'_, O> {
        match self {
            SealedIndexing::Flat(x) => x.vector(i),
            SealedIndexing::FlatPq(x) => x.vector(i),
            SealedIndexing::FlatSq(x) => x.vector(i),
            SealedIndexing::Ivf(x) => x.vector(i),
            SealedIndexing::IvfSq(x) => x.vector(i),
            SealedIndexing::IvfPq(x) => x.vector(i),
            SealedIndexing::Hnsw(x) => x.vector(i),
            SealedIndexing::HnswSq(x) => x.vector(i),
            SealedIndexing::HnswPq(x) => x.vector(i),
            SealedIndexing::InvertedIndex(x) => x.vector(i),
            SealedIndexing::Rabitq(x) => x.vector(i),
        }
    }
}

impl<O: OperatorIndexing> Collection for SealedIndexing<O> {
    fn payload(&self, i: u32) -> Payload {
        match self {
            SealedIndexing::Flat(x) => x.payload(i),
            SealedIndexing::FlatPq(x) => x.payload(i),
            SealedIndexing::FlatSq(x) => x.payload(i),
            SealedIndexing::Ivf(x) => x.payload(i),
            SealedIndexing::IvfPq(x) => x.payload(i),
            SealedIndexing::IvfSq(x) => x.payload(i),
            SealedIndexing::Hnsw(x) => x.payload(i),
            SealedIndexing::HnswPq(x) => x.payload(i),
            SealedIndexing::HnswSq(x) => x.payload(i),
            SealedIndexing::InvertedIndex(x) => x.payload(i),
            SealedIndexing::Rabitq(x) => x.payload(i),
        }
    }
}
