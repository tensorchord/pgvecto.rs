//! Postgres vector extension.
//!
//! Provides an easy-to-use extension for vector similarity search.
#![feature(core_intrinsics)]
#![feature(allocator_api)]
#![feature(thread_local)]
#![feature(auto_traits)]
#![feature(negative_impls)]
#![feature(ptr_metadata)]
#![feature(new_uninit)]
#![feature(int_roundings)]
#![feature(never_type)]
#![feature(lazy_cell)]
#![feature(fs_try_exists)]
#![feature(sync_unsafe_cell)]
#![allow(clippy::complexity)]
#![allow(clippy::style)]

mod algorithms;
mod bgworker;
mod embedding;
mod index;
mod ipc;
mod postgres;
mod prelude;
mod utils;

pgrx::pg_module_magic!();
pgrx::extension_sql_file!("./sql/bootstrap.sql", bootstrap);
pgrx::extension_sql_file!("./sql/finalize.sql", finalize);

#[allow(non_snake_case)]
#[pgrx::pg_guard]
pub unsafe extern "C" fn _PG_init() {
    use crate::prelude::*;
    if pgrx::pg_sys::IsUnderPostmaster {
        FriendlyError::BadInit.friendly();
    }
    use pgrx::bgworkers::BackgroundWorkerBuilder;
    use pgrx::bgworkers::BgWorkerStartTime;
    BackgroundWorkerBuilder::new("vectors")
        .set_function("vectors_main")
        .set_library("vectors")
        .set_argument(None)
        .enable_shmem_access(None)
        .set_start_time(BgWorkerStartTime::PostmasterStart)
        .load();
    self::postgres::init();
    self::ipc::transport::unix::init();
    self::ipc::transport::mmap::init();
}

#[no_mangle]
extern "C" fn vectors_main(_arg: pgrx::pg_sys::Datum) {
    let _ = std::panic::catch_unwind(crate::bgworker::main);
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
