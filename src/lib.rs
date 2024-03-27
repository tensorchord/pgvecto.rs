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
mod upgrade;
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
        detect::init();
        gucs::init();
        index::init();
        ipc::init();
        bgworker::init();
    }
}

#[cfg(not(all(target_endian = "little", target_pointer_width = "64")))]
compile_error!("Target is not supported.");

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[no_mangle]
unsafe extern "Rust" fn _vectors_jemalloc_alloc(layout: std::alloc::Layout) -> *mut u8 {
    use std::alloc::GlobalAlloc;
    unsafe { GLOBAL.alloc(layout) }
}

#[no_mangle]
unsafe extern "Rust" fn _vectors_jemalloc_dealloc(ptr: *mut u8, layout: std::alloc::Layout) {
    use std::alloc::GlobalAlloc;
    unsafe { GLOBAL.dealloc(ptr, layout) }
}
