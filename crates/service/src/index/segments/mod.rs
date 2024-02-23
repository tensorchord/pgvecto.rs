pub mod growing;
pub mod sealed;

use super::IndexTracker;
use std::path::PathBuf;
use std::sync::Arc;

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
