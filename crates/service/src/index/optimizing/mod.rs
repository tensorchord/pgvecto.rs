pub mod indexing;
pub mod sealing;
pub mod vacuum;

use serde::{Deserialize, Serialize};
use validator::Validate;

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct OptimizingOptions {
    #[serde(default = "OptimizingOptions::default_sealing_secs")]
    #[validate(range(min = 0, max = 60))]
    pub sealing_secs: u64,
    #[serde(default = "OptimizingOptions::default_sealing_size")]
    #[validate(range(min = 1, max = 4_000_000_000))]
    pub sealing_size: u32,
    #[serde(default = "OptimizingOptions::default_deleted_threshold", skip)]
    #[validate(range(min = 0.01, max = 1.00))]
    pub deleted_threshold: f64,
    #[serde(default = "OptimizingOptions::default_optimizing_threads")]
    #[validate(range(min = 0, max = 65535))]
    pub optimizing_threads: usize,
}

impl OptimizingOptions {
    fn default_sealing_secs() -> u64 {
        60
    }
    fn default_sealing_size() -> u32 {
        1
    }
    fn default_deleted_threshold() -> f64 {
        0.2
    }
    fn default_optimizing_threads() -> usize {
        match std::thread::available_parallelism() {
            Ok(threads) => (threads.get() as f64).sqrt() as _,
            Err(_) => 1,
        }
    }
}

impl Default for OptimizingOptions {
    fn default() -> Self {
        Self {
            sealing_secs: Self::default_sealing_secs(),
            sealing_size: Self::default_sealing_size(),
            deleted_threshold: Self::default_deleted_threshold(),
            optimizing_threads: Self::default_optimizing_threads(),
        }
    }
}
