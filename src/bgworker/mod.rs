pub mod normal;

use std::sync::atomic::{AtomicBool, Ordering};

static STARTED: AtomicBool = AtomicBool::new(false);

pub unsafe fn init() {
    use service::Worker;
    let path = std::path::Path::new("pg_vectors");
    if !path.try_exists().unwrap() || Worker::check(path.to_owned()) {
        use pgrx::bgworkers::BackgroundWorkerBuilder;
        use pgrx::bgworkers::BgWorkerStartTime;
        use std::time::Duration;
        BackgroundWorkerBuilder::new("vectors")
            .set_library("vectors")
            .set_function("_vectors_main")
            .set_argument(None)
            .enable_shmem_access(None)
            .set_start_time(BgWorkerStartTime::PostmasterStart)
            .set_restart_time(Some(Duration::from_secs(15)))
            .load();
        STARTED.store(true, Ordering::Relaxed);
    }
}

pub fn is_started() -> bool {
    STARTED.load(Ordering::Relaxed)
}

#[pgrx::pg_guard]
#[no_mangle]
extern "C" fn _vectors_main(_arg: pgrx::pg_sys::Datum) {
    pub struct AllocErrorPanicPayload {
        pub layout: std::alloc::Layout,
    }
    {
        let mut builder = env_logger::builder();
        builder.target(env_logger::Target::Stderr);
        #[cfg(not(debug_assertions))]
        {
            builder.filter(None, log::LevelFilter::Info);
        }
        #[cfg(debug_assertions)]
        {
            builder.filter(None, log::LevelFilter::Trace);
        }
        builder.init();
    }
    std::panic::set_hook(Box::new(|info| {
        if let Some(oom) = info.payload().downcast_ref::<AllocErrorPanicPayload>() {
            log::error!("Out of memory. Layout: {:?}.", oom.layout);
            return;
        }
        let backtrace;
        #[cfg(not(debug_assertions))]
        {
            backtrace = std::backtrace::Backtrace::capture();
        }
        #[cfg(debug_assertions)]
        {
            backtrace = std::backtrace::Backtrace::force_capture();
        }
        log::error!("Panickied. Info: {:?}. Backtrace: {}.", info, backtrace);
    }));
    std::alloc::set_alloc_error_hook(|layout| {
        std::panic::panic_any(AllocErrorPanicPayload { layout });
    });
    use service::Worker;
    use std::path::Path;
    let path = Path::new("pg_vectors");
    if path.try_exists().unwrap() {
        let worker = Worker::open(path.to_owned());
        self::normal::normal(worker);
    } else {
        let worker = Worker::create(path.to_owned());
        self::normal::normal(worker);
    }
}
