use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct VectorIndexInfo {
    pub indexing: bool,
    pub idx_tuples: i32,
    pub idx_sealed_len: i32,
    pub idx_growing_len: i32,
    pub idx_write: i32,
    pub idx_sealed: Vec<i32>,
    pub idx_growing: Vec<i32>,
    pub idx_config: String,
}
