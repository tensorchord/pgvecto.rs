mod flat;
mod hnsw;
mod impls;
mod ivf;
mod vectors;

pub use flat::Flat;
pub use hnsw::Hnsw;
pub use ivf::Ivf;
pub use vectors::Vectors;

use crate::memory::Address;
use crate::prelude::*;
use std::sync::Arc;

pub enum DynAlgorithm {
    Hnsw(Hnsw),
    Flat(Flat),
    Ivf(Ivf),
}

impl DynAlgorithm {
    pub fn build(options: Options, vectors: Arc<Vectors>, n: usize) -> anyhow::Result<Self> {
        use AlgorithmOptions as O;
        match options.algorithm {
            O::Hnsw(_) => Hnsw::build(options, vectors, n).map(Self::Hnsw),
            O::Flat(_) => Flat::build(options, vectors, n).map(Self::Flat),
            O::Ivf(_) => Ivf::build(options, vectors, n).map(Self::Ivf),
        }
    }
    pub fn address(&self) -> Address {
        use DynAlgorithm::*;
        match self {
            Hnsw(sel) => sel.address(),
            Flat(sel) => sel.address(),
            Ivf(sel) => sel.address(),
        }
    }
    pub fn load(options: Options, vectors: Arc<Vectors>, address: Address) -> anyhow::Result<Self> {
        use AlgorithmOptions as O;
        match options.algorithm {
            O::Hnsw(_) => Ok(Self::Hnsw(Hnsw::load(options, vectors, address)?)),
            O::Flat(_) => Ok(Self::Flat(Flat::load(options, vectors, address)?)),
            O::Ivf(_) => Ok(Self::Ivf(Ivf::load(options, vectors, address)?)),
        }
    }
    pub fn insert(&self, insert: usize) -> anyhow::Result<()> {
        use DynAlgorithm::*;
        match self {
            Hnsw(sel) => sel.insert(insert),
            Flat(sel) => sel.insert(insert),
            Ivf(sel) => sel.insert(insert),
        }
    }
    pub fn search(&self, search: (Box<[Scalar]>, usize)) -> anyhow::Result<Vec<(Scalar, u64)>> {
        use DynAlgorithm::*;
        match self {
            Hnsw(sel) => sel.search(search),
            Flat(sel) => sel.search(search),
            Ivf(sel) => sel.search(search),
        }
    }
}
