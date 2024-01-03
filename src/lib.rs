//! Postgres vector extension.
//!
//! Provides an easy-to-use extension for vector similarity search.
#![feature(never_type)]

mod bgworker;
mod datatype;
mod embedding;
mod gucs;
mod index;
mod ipc;
mod prelude;
mod utils;

pgrx::pg_module_magic!();
pgrx::extension_sql_file!("./sql/bootstrap.sql", bootstrap);
pgrx::extension_sql_file!("./sql/finalize.sql", finalize);

#[allow(non_snake_case)]
#[pgrx::pg_guard]
unsafe extern "C" fn _PG_init() {
    use service::prelude::*;
    if unsafe { pgrx::pg_sys::IsUnderPostmaster } {
        FriendlyError::BadInit.friendly();
    }
    unsafe {
        self::gucs::init();
        self::index::init();
        self::ipc::init();
        self::bgworker::init();
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "freebsd")))]
compile_error!("Target is not supported.");

#[cfg(not(target_endian = "little"))]
compile_error!("Target is not supported.");

#[cfg(not(target_pointer_width = "64"))]
compile_error!("Target is not supported.");

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
