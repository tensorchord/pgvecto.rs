#![feature(thread_local)]

use rayoff as rayon;
use std::cell::OnceCell;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub use rayon::array;
pub use rayon::collections;
pub use rayon::iter;
pub use rayon::option;
pub use rayon::prelude;
pub use rayon::range;
pub use rayon::range_inclusive;
pub use rayon::result;
pub use rayon::slice;
pub use rayon::str;
pub use rayon::string;
pub use rayon::vec;

pub use rayon::{current_num_threads, current_thread_index, max_num_threads};
pub use rayon::{in_place_scope, in_place_scope_fifo};
pub use rayon::{join, join_context};
pub use rayon::{scope, scope_fifo};
pub use rayon::{spawn, spawn_fifo};
pub use rayon::{yield_local, yield_now};
pub use rayon::{FnContext, Scope, ScopeFifo, Yield};

#[derive(Debug, Default)]
pub struct ThreadPoolBuilder {
    builder: rayon::ThreadPoolBuilder,
}

impl ThreadPoolBuilder {
    pub fn new() -> Self {
        Self {
            builder: rayon::ThreadPoolBuilder::new(),
        }
    }
    pub fn num_threads(self, num_threads: usize) -> Self {
        Self {
            builder: self.builder.num_threads(num_threads),
        }
    }
    pub fn build_scoped(
        self,
        f: impl FnOnce(&ThreadPool),
    ) -> Result<(), rayon::ThreadPoolBuildError> {
        let stop = Arc::new(AtomicBool::new(false));
        match std::panic::catch_unwind(AssertUnwindSafe(|| {
            self.builder
                .panic_handler(|e| {
                    if e.downcast_ref::<CheckPanic>().is_some() {
                        return;
                    }
                    log::error!("Asynchronous task panickied.");
                })
                .build_scoped(
                    |thread| thread.run(),
                    |pool| {
                        pool.broadcast(|_| {
                            STOP.set(stop.clone()).unwrap();
                        });
                        let pool = ThreadPool::new(stop.clone(), pool);
                        f(&pool)
                    },
                )
        })) {
            Ok(Ok(())) => (),
            Ok(Err(e)) => return Err(e),
            Err(e) if e.downcast_ref::<CheckPanic>().is_some() => (),
            Err(e) => std::panic::resume_unwind(e),
        }
        if Arc::strong_count(&stop) > 1 {
            panic!("Thread leak detected.");
        }
        Ok(())
    }
}

pub struct ThreadPool<'a> {
    stop: Arc<AtomicBool>,
    pool: &'a rayon::ThreadPool,
}

impl<'a> ThreadPool<'a> {
    fn new(stop: Arc<AtomicBool>, pool: &'a rayon::ThreadPool) -> Self {
        Self { stop, pool }
    }
    pub fn install<OP, R>(&self, op: OP) -> R
    where
        OP: FnOnce() -> R + Send,
        R: Send,
    {
        self.pool.install(op)
    }
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
        self.pool.broadcast(|_| {
            check();
        });
    }
}

#[thread_local]
static STOP: OnceCell<Arc<AtomicBool>> = OnceCell::new();

struct CheckPanic;

pub fn check() {
    if let Some(stop) = STOP.get() {
        if stop.load(Ordering::Relaxed) {
            std::panic::panic_any(CheckPanic);
        }
    } else {
        panic!("`check` is called outside rayon")
    }
}
