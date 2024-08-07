pub mod normal;

use std::sync::atomic::{AtomicBool, Ordering};

static STARTED: AtomicBool = AtomicBool::new(false);

pub unsafe fn init() {
    use service::Version;
    let path = std::path::Path::new("pg_vectors");
    if !path.try_exists().unwrap() || Version::read(path.join("VERSION")).is_ok() {
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
    // for debugging, set `RUST_LOG=trace`
    crate::logger::Logger::new(
        match std::env::var("RUST_LOG").as_ref().map(|x| x.as_str()) {
            Ok("off" | "Off" | "OFF") => log::LevelFilter::Off,
            Ok("error" | "Error" | "ERROR") => log::LevelFilter::Error,
            Ok("warn" | "Warn" | "WARN") => log::LevelFilter::Warn,
            Ok("info" | "Info" | "INFO") => log::LevelFilter::Info,
            Ok("debug" | "Debug" | "DEBUG") => log::LevelFilter::Debug,
            Ok("trace" | "Trace" | "TRACE") => log::LevelFilter::Trace,
            _ => log::LevelFilter::Info, // default level
        },
    )
    .init()
    .expect("failed to set logger");
    std::panic::set_hook(Box::new(|info| {
        let message = if let Some(s) = info.payload().downcast_ref::<&str>() {
            s.to_string()
        } else if let Some(s) = info.payload().downcast_ref::<String>() {
            s.clone()
        } else {
            "(none)".to_string()
        };
        // for debugging, set `RUST_BACKTRACE=1`
        let backtrace = std::backtrace::Backtrace::capture();
        log::error!("Panickied. Message: {message:?}. Backtrace: {backtrace}.");
    }));
    use service::Version;
    use service::Worker;
    use std::path::Path;
    let path = Path::new("pg_vectors");
    if path.try_exists().unwrap() {
        let worker = Worker::open(path.to_owned());
        normal::normal(worker);
    } else {
        let worker = Worker::create(path.to_owned());
        Version::write(path.join("VERSION"));
        normal::normal(worker);
    }
}
