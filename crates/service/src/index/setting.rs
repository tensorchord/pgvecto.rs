use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use validator::Validate;

// A flattened NotifiedSetting, Serializable for RPC
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Validate, PartialEq, Eq)]
pub struct RuntimeOptions {
    pub optimizing_threads: usize,
}

// These settings are watched and notified at runtime by index
pub struct NotifiedSetting {
    pub optimizing_threads_limit: AtomicUsize,
}

impl NotifiedSetting {
    pub fn update(&self, opts: RuntimeOptions) {
        self.optimizing_threads_limit
            .store(opts.optimizing_threads, Ordering::Relaxed);
    }

    pub fn load(&self) -> RuntimeOptions {
        RuntimeOptions {
            optimizing_threads: self.optimizing_threads_limit.load(Ordering::Relaxed),
        }
    }
}

impl Default for NotifiedSetting {
    fn default() -> Self {
        let val = match std::thread::available_parallelism() {
            Ok(threads) => (threads.get() as f64).sqrt() as _,
            Err(_) => 1,
        };
        NotifiedSetting {
            optimizing_threads_limit: AtomicUsize::new(val),
        }
    }
}
