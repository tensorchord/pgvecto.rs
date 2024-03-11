//! Postgres vector extension.
//!
//! Provides an easy-to-use extension for vector similarity search.
#![feature(alloc_error_hook)]
#![allow(clippy::needless_range_loop)]
#![allow(clippy::single_match)]
#![allow(clippy::too_many_arguments)]

mod bgworker;
mod datatype;
mod embedding;
mod error;
mod gucs;
mod index;
mod ipc;
mod utils;

pgrx::pg_module_magic!();
pgrx::extension_sql_file!("./sql/bootstrap.sql", bootstrap);
pgrx::extension_sql_file!("./sql/finalize.sql", finalize);

#[pgrx::pg_guard]
unsafe extern "C" fn _PG_init() {
    use crate::error::*;
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

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(not(all(target_endian = "little", target_pointer_width = "64")))]
compile_error!("Target is not supported.");
