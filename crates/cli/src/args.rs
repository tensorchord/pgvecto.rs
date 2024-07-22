use argh::FromArgs;
use log::warn;

use base::distance::DistanceKind;
use base::index::{IndexAlterableOptions, IndexOptions};
use base::index::{IndexingOptions, OptimizingOptions, SegmentOptions, VectorOptions};
use base::vector::VectorKind;

#[derive(Debug, Eq, PartialEq)]
pub struct ArgumentParseError;

fn vec_type_from_str(s: &str) -> Result<VectorKind, ArgumentParseError> {
    match s.trim() {
        "Vecf32" => Ok(VectorKind::Vecf32),
        "Vecf16" => Ok(VectorKind::Vecf16),
        "SVecf32" => Ok(VectorKind::SVecf32),
        "BVecf32" => Ok(VectorKind::BVecf32),
        "Veci8" => Ok(VectorKind::Veci8),
        _ => Err(ArgumentParseError),
    }
}

fn distance_from_str(s: &str) -> Result<DistanceKind, ArgumentParseError> {
    match s.trim() {
        "L2" => Ok(DistanceKind::L2),
        "Cos" => Ok(DistanceKind::Cos),
        "Dot" => Ok(DistanceKind::Dot),
        "Jaccard" => Ok(DistanceKind::Jaccard),
        _ => Err(ArgumentParseError),
    }
}

fn default_build_timeout_seconds() -> u64 {
    3600
}

fn default_query_top_k() -> usize {
    10
}

#[derive(FromArgs, Debug, PartialEq)]
#[argh(subcommand)]
pub enum SubCommandEnum {
    Add(AddArguments),
    Build(BuildArguments),
    Create(CreateArguments),
    Query(QueryArguments),
}

#[derive(FromArgs, Debug, PartialEq)]
/// create the index
#[argh(subcommand, name = "create")]
pub struct CreateArguments {
    /// vector dim
    #[argh(option)]
    dim: u32,

    /// vector type: [`Vecf32`, `Vecf16`, `SVecf32`, `BVecf32`, `Veci8`]
    #[argh(option, default = "String::from(\"Vecf32\")")]
    vim_type: String,

    /// vector distance: [`L2`, `Cos`, `Dot`, `Jaccard`]
    #[argh(option, default = "String::from(\"L2\")")]
    distance: String,

    /// indexing configurations in TOML string
    #[argh(positional)]
    indexing: Option<String>,

    /// optimizing threads number
    #[argh(option)]
    threads: Option<u16>,
}

impl CreateArguments {
    pub fn get_index_options(
        &self,
    ) -> Result<(IndexOptions, IndexAlterableOptions), ArgumentParseError> {
        let indexing: IndexingOptions = match &self.indexing {
            Some(toml_str) => match toml::from_str(toml_str) {
                Ok(config) => config,
                Err(err) => {
                    warn!("failed to parse the TOML index options: {err}");
                    return Err(ArgumentParseError);
                }
            },
            None => IndexingOptions::default(),
        };
        let index_options = IndexOptions {
            vector: VectorOptions {
                dims: self.dim,
                v: vec_type_from_str(&self.vim_type)?,
                d: distance_from_str(&self.distance)?,
            },
            indexing,
        };
        let mut optimizing = OptimizingOptions::default();
        if let Some(num) = self.threads {
            optimizing.optimizing_threads = num;
        }
        let alterable_options = IndexAlterableOptions {
            optimizing,
            segment: SegmentOptions::default(),
        };
        Ok((index_options, alterable_options))
    }
}

#[derive(FromArgs, Debug, PartialEq)]
/// query the index
#[argh(subcommand, name = "query")]
pub struct QueryArguments {
    /// query file path
    #[argh(option)]
    pub query: String,

    /// groundtruth file path
    #[argh(option)]
    pub truth: String,

    /// top-k
    #[argh(option, default = "default_query_top_k()")]
    pub top_k: usize,
}

#[derive(FromArgs, Debug, PartialEq)]
/// add vectors from file (HDF5, fvecs)
#[argh(subcommand, name = "add")]
pub struct AddArguments {
    /// vector file path
    #[argh(option)]
    pub file: String,
}

#[derive(FromArgs, Debug, PartialEq)]
/// build or optimize the index
#[argh(subcommand, name = "build")]
pub struct BuildArguments {
    /// override the optimizing threads number
    #[argh(option)]
    pub threads: Option<u16>,

    /// timeout for the building process
    #[argh(option, default = "default_build_timeout_seconds()")]
    pub timeout_seconds: u64,
}

#[derive(FromArgs, Debug)]
/// `pgvecto.rs` CLI
pub struct Arguments {
    #[argh(subcommand)]
    pub cmd: SubCommandEnum,

    /// indexing file dir path
    #[argh(option, short = 'p')]
    pub path: String,

    /// verbose
    #[argh(switch, short = 'v')]
    pub verbose: bool,
}
