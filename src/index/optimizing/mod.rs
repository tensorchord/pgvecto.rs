pub mod indexing;
pub mod vacuum;

use serde::{Deserialize, Serialize};
use validator::Validate;

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct OptimizingOptions {
    #[serde(default = "OptimizingOptions::default_waiting_secs", skip)]
    #[validate(range(min = 0, max = 600))]
    pub waiting_secs: u64,
    #[serde(default = "OptimizingOptions::default_deleted_threshold", skip)]
    #[validate(range(min = 0.01, max = 1.00))]
    pub deleted_threshold: f64,
    #[serde(default = "OptimizingOptions::default_optimizing_threads")]
    #[validate(range(min = 0, max = 65535))]
    pub optimizing_threads: usize,
}

impl OptimizingOptions {
    fn default_waiting_secs() -> u64 {
        60
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
            waiting_secs: Self::default_waiting_secs(),
            deleted_threshold: Self::default_deleted_threshold(),
            optimizing_threads: Self::default_optimizing_threads(),
        }
    }
}
