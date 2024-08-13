use crate::distance::*;
use crate::vector::*;
use base_macros::Alter;
use serde::{Deserialize, Serialize};
use std::num::NonZeroU128;
use thiserror::Error;
use validator::{Validate, ValidationError};

#[must_use]
#[derive(Debug, Clone, Error, Serialize, Deserialize)]
pub enum CreateError {
    #[error("Invalid index options: {reason}.")]
    InvalidIndexOptions { reason: String },
}

#[must_use]
#[derive(Debug, Clone, Error, Serialize, Deserialize)]
pub enum DropError {
    #[error("Index not found.")]
    NotExist,
}

#[must_use]
#[derive(Debug, Clone, Error, Serialize, Deserialize)]
pub enum FlushError {
    #[error("Index not found.")]
    NotExist,
}

#[must_use]
#[derive(Debug, Clone, Error, Serialize, Deserialize)]
pub enum InsertError {
    #[error("Index not found.")]
    NotExist,
    #[error("Invalid vector.")]
    InvalidVector,
}

#[must_use]
#[derive(Debug, Clone, Error, Serialize, Deserialize)]
pub enum DeleteError {
    #[error("Index not found.")]
    NotExist,
}

#[must_use]
#[derive(Debug, Clone, Error, Serialize, Deserialize)]
pub enum VbaseError {
    #[error("Index not found.")]
    NotExist,
    #[error("Invalid vector.")]
    InvalidVector,
    #[error("Invalid search options.")]
    InvalidSearchOptions { reason: String },
}

#[must_use]
#[derive(Debug, Clone, Error, Serialize, Deserialize)]
pub enum ListError {
    #[error("Index not found.")]
    NotExist,
}

#[must_use]
#[derive(Debug, Clone, Error, Serialize, Deserialize)]
pub enum StatError {
    #[error("Index not found.")]
    NotExist,
}

#[must_use]
#[derive(Debug, Clone, Error, Serialize, Deserialize)]
pub enum AlterError {
    #[error("Index not found.")]
    NotExist,
    #[error("Key {key} not found.")]
    KeyNotExists { key: String },
    #[error("Invalid index options: {reason}.")]
    InvalidIndexOptions { reason: String },
}

#[must_use]
#[derive(Debug, Clone, Error, Serialize, Deserialize)]
pub enum StopError {
    #[error("Index not found.")]
    NotExist,
}

#[must_use]
#[derive(Debug, Clone, Error, Serialize, Deserialize)]
pub enum StartError {
    #[error("Index not found.")]
    NotExist,
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
#[validate(schema(function = "IndexOptions::validate_self"))]
pub struct IndexOptions {
    #[validate(nested)]
    pub vector: VectorOptions,
    #[validate(nested)]
    pub indexing: IndexingOptions,
}

impl IndexOptions {
    fn validate_self_quantization(
        &self,
        quantization: &QuantizationOptions,
    ) -> Result<(), ValidationError> {
        match quantization {
            QuantizationOptions::Trivial(_) => Ok(()),
            QuantizationOptions::Scalar(_) | QuantizationOptions::Product(_) => {
                if !matches!(self.vector.v, VectorKind::Vecf32 | VectorKind::Vecf16) {
                    return Err(ValidationError::new(
                        "scalar quantization or product quantization is not support for vectors that are not dense vectors",
                    ));
                }
                Ok(())
            }
        }
    }
    fn validate_self(&self) -> Result<(), ValidationError> {
        match &self.indexing {
            IndexingOptions::Flat(FlatIndexingOptions { quantization }) => {
                self.validate_self_quantization(quantization)?;
            }
            IndexingOptions::Ivf(IvfIndexingOptions { quantization, .. }) => {
                if !matches!(self.vector.v, VectorKind::Vecf32 | VectorKind::Vecf16) {
                    return Err(ValidationError::new(
                        "ivf is not support for vectors that are not dense vectors",
                    ));
                }
                self.validate_self_quantization(quantization)?;
            }
            IndexingOptions::Hnsw(HnswIndexingOptions { quantization, .. }) => {
                self.validate_self_quantization(quantization)?;
            }
            IndexingOptions::InvertedIndex(_) => {
                if !matches!(self.vector.d, DistanceKind::Dot) {
                    return Err(ValidationError::new(
                        "inverted_index is not support for distance that is not negative dot product",
                    ));
                }
                if !matches!(self.vector.v, VectorKind::SVecf32) {
                    return Err(ValidationError::new(
                        "inverted_index is not support for vectors that are not sparse vectors",
                    ));
                }
            }
            IndexingOptions::Rabitq(_) => {
                if !matches!(self.vector.d, DistanceKind::L2) {
                    return Err(ValidationError::new(
                        "inverted_index is not support for distance that is not l2",
                    ));
                }
                if !matches!(self.vector.v, VectorKind::Vecf32) {
                    return Err(ValidationError::new(
                        "inverted_index is not support for vectors that are not vector",
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, Validate, Alter)]
#[serde(deny_unknown_fields)]
pub struct IndexAlterableOptions {
    #[serde(default)]
    #[validate(nested)]
    pub segment: SegmentOptions,
    #[serde(default)]
    #[validate(nested)]
    pub optimizing: OptimizingOptions,
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
#[validate(schema(function = "Self::validate_self"))]
pub struct VectorOptions {
    #[validate(range(min = 1, max = 1_048_575))]
    #[serde(rename = "dimensions")]
    pub dims: u32,
    #[serde(rename = "vector")]
    pub v: VectorKind,
    #[serde(rename = "distance")]
    pub d: DistanceKind,
}

impl VectorOptions {
    pub fn validate_self(&self) -> Result<(), ValidationError> {
        match (self.v, self.d, self.dims) {
            (VectorKind::Vecf32, DistanceKind::L2, 1..65536) => Ok(()),
            (VectorKind::Vecf32, DistanceKind::Dot, 1..65536) => Ok(()),
            (VectorKind::Vecf16, DistanceKind::L2, 1..65536) => Ok(()),
            (VectorKind::Vecf16, DistanceKind::Dot, 1..65536) => Ok(()),
            (VectorKind::SVecf32, DistanceKind::L2, 1..1048576) => Ok(()),
            (VectorKind::SVecf32, DistanceKind::Dot, 1..1048576) => Ok(()),
            (VectorKind::BVector, DistanceKind::Dot, 1..65536) => Ok(()),
            (VectorKind::BVector, DistanceKind::Hamming, 1..65536) => Ok(()),
            (VectorKind::BVector, DistanceKind::Jaccard, 1..65536) => Ok(()),
            _ => Err(ValidationError::new("not valid vector options")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate, Alter)]
#[serde(deny_unknown_fields)]
pub struct SegmentOptions {
    #[serde(default = "SegmentOptions::default_max_growing_segment_size")]
    #[validate(range(min = 1, max = 4_000_000_000u32))]
    pub max_growing_segment_size: u32,
    #[serde(default = "SegmentOptions::default_max_sealed_segment_size")]
    #[validate(range(min = 1, max = 4_000_000_000u32))]
    pub max_sealed_segment_size: u32,
}

impl SegmentOptions {
    fn default_max_growing_segment_size() -> u32 {
        20_000
    }
    fn default_max_sealed_segment_size() -> u32 {
        4_000_000_000u32
    }
}

impl Default for SegmentOptions {
    fn default() -> Self {
        Self {
            max_growing_segment_size: Self::default_max_growing_segment_size(),
            max_sealed_segment_size: Self::default_max_sealed_segment_size(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate, Alter)]
#[serde(deny_unknown_fields)]
pub struct OptimizingOptions {
    #[serde(default = "OptimizingOptions::default_sealing_secs")]
    #[validate(range(min = 1, max = 86400))]
    pub sealing_secs: u64,
    #[serde(default = "OptimizingOptions::default_sealing_size")]
    #[validate(range(min = 1, max = 4_000_000_000u32))]
    pub sealing_size: u32,
    #[serde(default = "OptimizingOptions::default_optimizing_secs")]
    #[validate(range(min = 1, max = 86400))]
    pub optimizing_secs: u64,
    #[serde(default = "OptimizingOptions::default_optimizing_threads")]
    #[validate(range(min = 1, max = 65535))]
    pub optimizing_threads: u16,
    #[serde(default = "OptimizingOptions::default_delete_threshold")]
    #[validate(range(min = 0.0001, max = 1.0000))]
    pub delete_threshold: f64,
}

impl OptimizingOptions {
    fn default_sealing_secs() -> u64 {
        10
    }
    fn default_sealing_size() -> u32 {
        1
    }
    fn default_optimizing_secs() -> u64 {
        60
    }
    fn default_optimizing_threads() -> u16 {
        1
    }
    fn default_delete_threshold() -> f64 {
        0.2
    }
}

impl Default for OptimizingOptions {
    fn default() -> Self {
        Self {
            sealing_secs: Self::default_sealing_secs(),
            sealing_size: Self::default_sealing_size(),
            optimizing_secs: Self::default_optimizing_secs(),
            optimizing_threads: Self::default_optimizing_threads(),
            delete_threshold: Self::default_delete_threshold(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub enum IndexingOptions {
    Flat(FlatIndexingOptions),
    Ivf(IvfIndexingOptions),
    Hnsw(HnswIndexingOptions),
    InvertedIndex(InvertedIndexingOptions),
    Rabitq(RabitqIndexingOptions),
}

impl IndexingOptions {
    pub fn unwrap_flat(self) -> FlatIndexingOptions {
        let IndexingOptions::Flat(x) = self else {
            unreachable!()
        };
        x
    }
    pub fn unwrap_ivf(self) -> IvfIndexingOptions {
        let IndexingOptions::Ivf(x) = self else {
            unreachable!()
        };
        x
    }
    pub fn unwrap_hnsw(self) -> HnswIndexingOptions {
        let IndexingOptions::Hnsw(x) = self else {
            unreachable!()
        };
        x
    }
    pub fn unwrap_rabitq(self) -> RabitqIndexingOptions {
        let IndexingOptions::Rabitq(x) = self else {
            unreachable!()
        };
        x
    }
}

impl Default for IndexingOptions {
    fn default() -> Self {
        Self::Hnsw(Default::default())
    }
}

impl Validate for IndexingOptions {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            Self::Flat(x) => x.validate(),
            Self::Ivf(x) => x.validate(),
            Self::Hnsw(x) => x.validate(),
            Self::InvertedIndex(x) => x.validate(),
            Self::Rabitq(x) => x.validate(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct InvertedIndexingOptions {}

impl Default for InvertedIndexingOptions {
    fn default() -> Self {
        Self {}
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct FlatIndexingOptions {
    #[serde(default)]
    #[validate(nested)]
    pub quantization: QuantizationOptions,
}

impl Default for FlatIndexingOptions {
    fn default() -> Self {
        Self {
            quantization: QuantizationOptions::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct IvfIndexingOptions {
    #[serde(default = "IvfIndexingOptions::default_nlist")]
    #[validate(range(min = 1, max = 1_000_000))]
    pub nlist: u32,
    #[serde(default = "IvfIndexingOptions::default_spherical_centroids")]
    pub spherical_centroids: bool,
    #[serde(default = "IvfIndexingOptions::default_residual_quantization")]
    pub residual_quantization: bool,
    #[serde(default)]
    #[validate(nested)]
    pub quantization: QuantizationOptions,
}

impl IvfIndexingOptions {
    fn default_nlist() -> u32 {
        1000
    }
    fn default_spherical_centroids() -> bool {
        false
    }
    fn default_residual_quantization() -> bool {
        false
    }
}

impl Default for IvfIndexingOptions {
    fn default() -> Self {
        Self {
            nlist: Self::default_nlist(),
            spherical_centroids: false,
            residual_quantization: false,
            quantization: Default::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct HnswIndexingOptions {
    #[serde(default = "HnswIndexingOptions::default_m")]
    #[validate(range(min = 4, max = 128))]
    // minimal value of `m` is 4 and maximum value of `max_sealed_segment_size` is 4_000_000_000
    // so there are at most 15 hierarchical graphs expect the level-0 graph
    pub m: u32,
    #[serde(default = "HnswIndexingOptions::default_ef_construction")]
    #[validate(range(min = 10, max = 2000))]
    pub ef_construction: u32,
    #[serde(default)]
    #[validate(nested)]
    pub quantization: QuantizationOptions,
}

impl HnswIndexingOptions {
    fn default_m() -> u32 {
        12
    }
    fn default_ef_construction() -> u32 {
        300
    }
}

impl Default for HnswIndexingOptions {
    fn default() -> Self {
        Self {
            m: Self::default_m(),
            ef_construction: Self::default_ef_construction(),
            quantization: Default::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct RabitqIndexingOptions {
    #[serde(default = "RabitqIndexingOptions::default_nlist")]
    #[validate(range(min = 1, max = 1_000_000))]
    pub nlist: u32,
}

impl RabitqIndexingOptions {
    fn default_nlist() -> u32 {
        1000
    }
}

impl Default for RabitqIndexingOptions {
    fn default() -> Self {
        Self {
            nlist: Self::default_nlist(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub enum QuantizationOptions {
    Trivial(TrivialQuantizationOptions),
    Scalar(ScalarQuantizationOptions),
    Product(ProductQuantizationOptions),
}

impl Validate for QuantizationOptions {
    fn validate(&self) -> Result<(), validator::ValidationErrors> {
        match self {
            Self::Trivial(x) => x.validate(),
            Self::Scalar(x) => x.validate(),
            Self::Product(x) => x.validate(),
        }
    }
}

impl Default for QuantizationOptions {
    fn default() -> Self {
        Self::Trivial(Default::default())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct TrivialQuantizationOptions {}

impl Default for TrivialQuantizationOptions {
    fn default() -> Self {
        Self {}
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
#[validate(schema(function = "Self::validate_self"))]
pub struct ScalarQuantizationOptions {
    #[serde(default = "ScalarQuantizationOptions::default_bits")]
    pub bits: u32,
}

impl ScalarQuantizationOptions {
    fn default_bits() -> u32 {
        8
    }
    fn validate_self(&self) -> Result<(), ValidationError> {
        match self.bits {
            1 | 2 | 4 | 8 => Ok(()),
            _ => Err(ValidationError::new("invalid quantization bits")),
        }
    }
}

impl Default for ScalarQuantizationOptions {
    fn default() -> Self {
        Self {
            bits: Self::default_bits(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
#[validate(schema(function = "Self::validate_self"))]
pub struct ProductQuantizationOptions {
    #[serde(default = "ProductQuantizationOptions::default_ratio")]
    #[validate(range(min = 1, max = 1024))]
    pub ratio: u32,
    #[serde(default = "ProductQuantizationOptions::default_bits")]
    pub bits: u32,
}

impl ProductQuantizationOptions {
    fn default_ratio() -> u32 {
        1
    }
    fn default_bits() -> u32 {
        8
    }
    fn validate_self(&self) -> Result<(), ValidationError> {
        match self.bits {
            1 | 2 | 4 | 8 => Ok(()),
            _ => Err(ValidationError::new("invalid quantization bits")),
        }
    }
}

impl Default for ProductQuantizationOptions {
    fn default() -> Self {
        Self {
            ratio: Self::default_ratio(),
            bits: Self::default_bits(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct SearchOptions {
    #[validate(range(min = 0, max = 65535))]
    pub flat_sq_rerank_size: u32,
    pub flat_sq_fast_scan: bool,
    #[validate(range(min = 0, max = 65535))]
    pub flat_pq_rerank_size: u32,
    pub flat_pq_fast_scan: bool,
    #[validate(range(min = 0, max = 65535))]
    pub ivf_sq_rerank_size: u32,
    pub ivf_sq_fast_scan: bool,
    #[validate(range(min = 0, max = 65535))]
    pub ivf_pq_rerank_size: u32,
    pub ivf_pq_fast_scan: bool,
    #[validate(range(min = 1, max = 65535))]
    pub ivf_nprobe: u32,
    #[validate(range(min = 1, max = 65535))]
    pub hnsw_ef_search: u32,
    #[validate(range(min = 1, max = 65535))]
    pub rabitq_nprobe: u32,
    pub rabitq_fast_scan: bool,
    #[validate(range(min = 1, max = 65535))]
    pub diskann_ef_search: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IndexStat {
    pub indexing: bool,
    pub segments: Vec<SegmentStat>,
    pub options: IndexOptions,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SegmentStat {
    pub id: NonZeroU128,
    pub r#type: String,
    pub length: usize,
    pub size: u64,
}

pub trait Alter {
    fn alter(&mut self, key: &[&str], value: &str) -> Result<(), AlterError>;
}

macro_rules! impl_alter_for {
    {$($t:ty)*} => {
        $(impl Alter for $t {
            fn alter(&mut self, key: &[&str], value: &str) -> Result<(), AlterError> {
                use std::str::FromStr;
                if key.is_empty() {
                    *self = FromStr::from_str(value).map_err(|_| AlterError::InvalidIndexOptions { reason: "failed to parse".to_string() })?;
                    return Ok(());
                }
                Err(AlterError::KeyNotExists { key: key.join(".") })
            }
        })*
    };
}

impl_alter_for! {
    String u8 u16 u32 u64 i8 i16 i32 i64 f32 f64 bool
}
