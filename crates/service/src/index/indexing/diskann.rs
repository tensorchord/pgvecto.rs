use super::AbstractIndexing;
use crate::algorithms::diskann::DiskANN;
use crate::algorithms::quantization::QuantizationOptions;
use crate::index::segments::growing::GrowingSegment;
use crate::index::segments::sealed::SealedSearchGucs;
use crate::index::segments::sealed::SealedSegment;
use crate::index::IndexOptions;
use crate::prelude::*;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use validator::Validate;

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct DiskANNIndexingOptions {
    #[serde(default = "DiskANNIndexingOptions::default_index_path_prefix")]
    pub index_path_prefix: String,

    #[serde(default = "DiskANNIndexingOptions::default_data_path")]
    pub data_path: String,

    // DRAM budget in GB for searching the index to set the compressed level 
    // for data while search happens
    
    //bound on the memory footprint of the index at search time in GB. Once built,
    // the index will use up only the specified RAM limit, the rest will reside on disk.
    // This will dictate how aggressively we compress the data vectors to store in memory.
    // Larger will yield better performance at search time. For an n point index, to use 
    // b byte PQ compressed representation in memory, use `B = ((n * b) / 2^30  + (250000*(4*R + sizeof(T)*ndim)) / 2^30)`.
    // The second term in the summation is to allow some buffer for caching about 250,000 nodes from the graph in memory while serving.
    // If you are not sure about this term, add 0.25GB to the first term. 
    #[serde(default = "DiskANNIndexingOptions::default_search_DRAM_budget")]
    pub search_DRAM_budget: u32,

    // DRAM budget in GB for building the index
    // Limit on the memory allowed for building the index in GB.
    // If you specify a value less than what is required to build the index
    // in one pass, the index is  built using a divide and conquer approach so
    // that sub-graphs will fit in the RAM budget. The sub-graphs are overlayed
    // to build the overall index. This approach can be upto 1.5 times slower than
    // building the index in one shot. Allocate as much memory as your RAM allows.
    #[serde(default = "DiskANNIndexingOptions::default_build_DRAM_budget")]
    pub build_DRAM_budget: u32,

    #[serde(default = "DiskANNIndexingOptions::default_num_threads")]
    pub num_threads: u32,

    // R in the paper
    #[serde(default = "DiskANNIndexingOptions::default_max_degree")]
    pub max_degree: u32,

    // L in the paper
    #[serde(default = "DiskANNIndexingOptions::default_Lbuild")]
    pub max_degree: u32,

    // TODO: QD (quantized dimension)
    // TODO: codebook prefix
    // TODO: PQ disk bytes (compressed bytes on SSD; 0 for no compression)
    // TODO: append reorder data (include full precision data in the index; use only in conjunction with compressed data on SSD)
    // TODO: build_PQ_bytes
    // TODO: use opq
    // TODO: label file (for filtered diskANN)
    // TODO: universal label (for filtered diskANN)
    // TODO: filtered Lbuild (for filtered diskANN)
    // TODO: filter threshold (for filtered diskANN)
    // TODO: label type (for filtered diskANN)

    #[serde(default)]
    #[validate]
    pub quantization: QuantizationOptions,
}

impl DiskANNIndexingOptions {
    fn default_index_path_prefix() -> String {
        "DiskANN_index".to_string()
    }
    fn default_data_path() -> u32 {
        "DiskANN_data".to_string()
    }
    fn default_search_DRAM_budget() -> u32 {
        1
    }
    fn default_build_DRAM_budget() -> u32 {
        1
    }
    fn default_num_threads() -> usize {
        match std::thread::available_parallelism() {
            Ok(threads) => (threads.get() as f64).sqrt() as _,
            Err(_) => 1,
        }
    }
    fn default_max_degree() -> u32 {
        64
    }
    fn default_Lbuild() -> u32 {
        100
    }
}

impl Default for DiskANNIndexingOptions {
    fn default() -> Self {
        Self {
            index_path_prefix: Self::default_index_path_prefix(),
            data_path: Self::default_data_path(),
            search_DRAM_budget: Self::default_search_DRAM_budget(),
            build_DRAM_budget: Self::default_build_DRAM_budget(),
            num_threads: Self::default_num_threads(),
            max_degree: Self::default_max_degree(),
            Lbuild: Self::default_Lbuild(),
            quantization: Default::default(),
        }
    }
}

pub struct DiskANNIndexing<S: G> {
    raw: Ivf<S>,
}

impl<S: G> AbstractIndexing<S> for IvfIndexing<S> {
    fn create(
        path: PathBuf,
        options: IndexOptions,
        sealed: Vec<Arc<SealedSegment<S>>>,
        growing: Vec<Arc<GrowingSegment<S>>>,
    ) -> Self {
        let raw = Ivf::create(path, options, sealed, growing);
        Self { raw }
    }

    fn open(path: PathBuf, options: IndexOptions) -> Self {
        let raw = Ivf::open(path, options);
        Self { raw }
    }

    fn len(&self) -> u32 {
        self.raw.len()
    }

    fn vector(&self, i: u32) -> &[S::Scalar] {
        self.raw.vector(i)
    }

    fn payload(&self, i: u32) -> Payload {
        self.raw.payload(i)
    }

    fn search(
        &self,
        k: usize,
        vector: &[S::Scalar],
        gucs: SealedSearchGucs,
        filter: &mut impl Filter,
    ) -> Heap {
        self.raw.search(k, vector, gucs.ivf_nprob, filter)
    }
}
