//! Postgres vector extension.
//!
//! Provides an easy-to-use extension for vector similarity search.
#![feature(alloc_error_hook)]
#![allow(clippy::needless_range_loop)]
#![allow(clippy::too_many_arguments)]

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
    use crate::prelude::*;
    if unsafe { pgrx::pg_sys::IsUnderPostmaster } {
        bad_init();
    }
    unsafe {
        detect::initialize();
        self::gucs::init();
        self::index::init();
        self::ipc::init();
        self::bgworker::init();
    }
}

#[cfg(not(all(target_endian = "little", target_pointer_width = "64")))]
compile_error!("Target is not supported.");

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![
            "shared_preload_libraries=vectors.so",
            "search_path=\"$user\", public, vectors",
            "logging_collector=on",
        ]
    }
}
