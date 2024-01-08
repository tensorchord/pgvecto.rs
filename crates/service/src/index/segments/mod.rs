pub mod growing;
pub mod sealed;

use super::IndexTracker;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use validator::Validate;
use validator::ValidationError;

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
#[validate(schema(function = "Self::validate_0"))]
pub struct SegmentsOptions {
    #[serde(default = "SegmentsOptions::default_max_growing_segment_size")]
    #[validate(range(min = 1, max = 4_000_000_000))]
    pub max_growing_segment_size: u32,
    #[serde(default = "SegmentsOptions::default_max_sealed_segment_size")]
    #[validate(range(min = 1, max = 4_000_000_000))]
    pub max_sealed_segment_size: u32,
}

impl SegmentsOptions {
    fn default_max_growing_segment_size() -> u32 {
        20_000
    }
    fn default_max_sealed_segment_size() -> u32 {
        1_000_000
    }
    // max_growing_segment_size <= max_sealed_segment_size
    fn validate_0(&self) -> Result<(), ValidationError> {
        if self.max_growing_segment_size > self.max_sealed_segment_size {
            return Err(ValidationError::new(
                "`max_growing_segment_size` must be less than or equal to `max_sealed_segment_size`",
            ));
        }
        Ok(())
    }
}

impl Default for SegmentsOptions {
    fn default() -> Self {
        Self {
            max_growing_segment_size: Self::default_max_growing_segment_size(),
            max_sealed_segment_size: Self::default_max_sealed_segment_size(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SegmentTracker {
    path: PathBuf,
    _tracker: Arc<IndexTracker>,
}

impl Drop for SegmentTracker {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.path).unwrap();
    }
}
