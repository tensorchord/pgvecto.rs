use std::cell::RefCell;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub mod iter {
    pub use rayon::iter::IntoParallelIterator;
    pub use rayon::iter::IntoParallelRefMutIterator;
    pub use rayon::iter::ParallelIterator;
}

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
    pub fn build_scoped<R>(
        self,
        f: impl FnOnce(&ThreadPool) -> R,
    ) -> Result<Option<R>, rayon::ThreadPoolBuildError> {
        let stop = Arc::new(AtomicBool::new(false));
        let x = match std::panic::catch_unwind(AssertUnwindSafe(|| {
            self.builder
                .start_handler({
                    let stop = stop.clone();
                    move |_| {
                        STOP.replace(Some(stop.clone()));
                    }
                })
                .exit_handler(|_| {
                    STOP.take();
                })
                .panic_handler(|e| {
                    if e.downcast_ref::<CheckPanic>().is_some() {
                        return;
                    }
                    log::error!("Asynchronous task panickied.");
                })
                .build_scoped(
                    |thread| thread.run(),
                    |pool| {
                        let pool = ThreadPool::new(stop.clone(), pool);
                        f(&pool)
                    },
                )
        })) {
            Ok(Ok(r)) => Some(r),
            Ok(Err(e)) => return Err(e),
            Err(e) if e.downcast_ref::<CheckPanic>().is_some() => None,
            Err(e) => std::panic::resume_unwind(e),
        };
        if Arc::strong_count(&stop) > 1 {
            panic!("Thread leak detected.");
        }
        Ok(x)
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

std::thread_local! {
    static STOP: RefCell<Option<Arc<AtomicBool>>> = const { RefCell::new(None) };
}

struct CheckPanic;

pub fn check() {
    STOP.with(|stop| {
        if let Some(stop) = stop.borrow().as_ref() {
            if stop.load(Ordering::Relaxed) {
                std::panic::panic_any(CheckPanic);
            }
        } else {
            panic!("`check` is called outside rayon")
        }
    });
}
