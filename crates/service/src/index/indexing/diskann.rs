use super::AbstractIndexing;
use crate::algorithms::diskann::DiskANN;
use crate::algorithms::quantization::QuantizationOptions;
use crate::index::segments::growing::GrowingSegment;
use crate::index::segments::sealed::SealedSegment;
use crate::index::IndexOptions;
use crate::index::SearchOptions;
use crate::prelude::*;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use validator::Validate;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct DiskANNIndexingOptions {
    // TODO(avery): Referenced from Microsoft.DiskANN algorithm, One design is to
    // leave the definition of memory usage to users and estimate the required
    // memory and the given memory to decide the quantization options.
    //
    // Current design is to let users define the ratio of PQ for in memory index.
    // 
    // Besides, it is hard to estimate current memory usage as sealed segment and 
    // growing segement are passed to RawMmap and RawRam. Different from the direct
    // calculation of the vector layout.

    // #[serde(default = "DiskANNIndexingOptions::default_index_path_prefix")]
    // pub index_path_prefix: PathBuf,

    // #[serde(default = "DiskANNIndexingOptions::default_data_path")]
    // pub data_path: PathBuf,

    // // DRAM budget in GB for searching the index to set the compressed level 
    // // for data while search happens
    
    // // bound on the memory footprint of the index at search time in GB. Once built,
    // // the index will use up only the specified RAM limit, the rest will reside on disk.
    // // This will dictate how aggressively we compress the data vectors to store in memory.
    // // Larger will yield better performance at search time. For an n point index, to use 
    // // b byte PQ compressed representation in memory, use `B = ((n * b) / 2^30  + (250000*(4*R + sizeof(T)*ndim)) / 2^30)`.
    // // The second term in the summation is to allow some buffer for caching about 250,000 nodes from the graph in memory while serving.
    // // If you are not sure about this term, add 0.25GB to the first term. 
    // #[serde(default = "DiskANNIndexingOptions::default_search_DRAM_budget")]
    // pub search_DRAM_budget: u32,

    // // DRAM budget in GB for building the index
    // // Limit on the memory allowed for building the index in GB.
    // // If you specify a value less than what is required to build the index
    // // in one pass, the index is  built using a divide and conquer approach so
    // // that sub-graphs will fit in the RAM budget. The sub-graphs are overlayed
    // // to build the overall index. This approach can be upto 1.5 times slower than
    // // building the index in one shot. Allocate as much memory as your RAM allows.
    // #[serde(default = "DiskANNIndexingOptions::default_build_DRAM_budget")]
    // pub build_DRAM_budget: u32,

    #[serde(default = "DiskANNIndexingOptions::default_num_threads")]
    pub num_threads: u32,

    // R in the paper
    #[serde(default = "DiskANNIndexingOptions::default_max_degree")]
    pub max_degree: u32,

    // L in the paper
    #[serde(default = "DiskANNIndexingOptions::default_l_build")]
    pub l_build: u32,

    // alpha in the paper, slack factor
    #[serde(default = "DiskANNIndexingOptions::default_alpha")]
    pub alpha: f32,

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
    // fn default_index_path_prefix() -> PathBuf {
    //     "DiskANN_index".to_string().into()
    // }
    // fn default_data_path() -> PathBuf {
    //     "DiskANN_data".to_string().into()
    // }
    // fn default_search_DRAM_budget() -> u32 {
    //     1
    // }
    // fn default_build_DRAM_budget() -> u32 {
    //     1
    // }

    fn default_num_threads() -> u32 {
        match std::thread::available_parallelism() {
            Ok(threads) => (threads.get() as f64).sqrt() as _,
            Err(_) => 1,
        }
    }
    fn default_max_degree() -> u32 {
        64
    }
    fn default_l_build() -> u32 {
        100
    }
    fn default_alpha() -> f32 {
        1.2
    }
}

impl Default for DiskANNIndexingOptions {
    fn default() -> Self {
        Self {
            // index_path_prefix: Self::default_index_path_prefix(),
            // data_path: Self::default_data_path(),
            // search_DRAM_budget: Self::default_search_DRAM_budget(),
            // build_DRAM_budget: Self::default_build_DRAM_budget(),
            num_threads: Self::default_num_threads(),
            max_degree: Self::default_max_degree(),
            l_build: Self::default_l_build(),
            alpha: Self::default_alpha(),
            quantization: Default::default(),
        }
    }
}

pub struct DiskANNIndexing<S: G> {
    raw: DiskANN<S>,
}

impl<S: G> AbstractIndexing<S> for DiskANNIndexing<S> {
    fn create(
        path: PathBuf,
        options: IndexOptions,
        sealed: Vec<Arc<SealedSegment<S>>>,
        growing: Vec<Arc<GrowingSegment<S>>>,
    ) -> Self {
        let raw = DiskANN::create(path, options, sealed, growing);
        Self { raw }
    }

    fn open(path: PathBuf, options: IndexOptions) -> Self {
        let raw = DiskANN::open(path, options);
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

    fn basic(
        &self,
        vector: &[S::Scalar],
        opts: &SearchOptions,
        filter: impl Filter,
    ) -> BinaryHeap<Reverse<Element>> {
        self.raw.search()
    }

    fn vbase<'a>(
        &'a self,
        vector: &'a [S::Scalar],
        opts: &'a SearchOptions,
        filter: impl Filter + 'a,
    ) -> (Vec<Element>, Box<(dyn Iterator<Item = Element> + 'a)>) {
        unimplemented!("DiskANN does not support vbase mode")
    }
}
