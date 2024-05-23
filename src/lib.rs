//! Postgres vector extension.
//!
//! Provides an easy-to-use extension for vector similarity search.
#![feature(alloc_error_hook)]
#![feature(slice_split_once)]
#![allow(clippy::needless_range_loop)]
#![allow(clippy::single_match)]
#![allow(clippy::too_many_arguments)]
// for pgrx
#![allow(non_snake_case)]
#![allow(clippy::let_unit_value)]

mod bgworker;
mod datatype;
mod embedding;
mod error;
mod gucs;
mod index;
mod ipc;
mod logger;
mod upgrade;
mod utils;

pgrx::pg_module_magic!();
pgrx::extension_sql_file!("./sql/bootstrap.sql", bootstrap);
pgrx::extension_sql_file!("./sql/finalize.sql", finalize);
#[pgrx::pg_extern]
fn test_logger() -> String {
    log::error!("pjw-测试");
    let des = unsafe { pgrx::pg_sys::Log_destination };
    log::error!("{}", des);
    log::trace!("测试");
    String::from("OK")
}

#[pgrx::pg_guard]
unsafe extern "C" fn _PG_init() {
    use crate::error::*;
    if unsafe { pgrx::pg_sys::IsUnderPostmaster } {
        bad_init();
    }
    unsafe {
        let mut log = logger::VectorLogger::build();
        log.filter_level(log::LevelFilter::Trace);
        log.init();
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

const SCHEMA: &str = include_str!("../.schema");

const SCHEMA_C_BYTES: [u8; SCHEMA.len() + 1] = {
    let mut bytes = [0u8; SCHEMA.len() + 1];
    let mut i = 0_usize;
    while i < SCHEMA.len() {
        bytes[i] = SCHEMA.as_bytes()[i];
        i += 1;
    }
    bytes
};

const SCHEMA_C_STR: &std::ffi::CStr = match std::ffi::CStr::from_bytes_with_nul(&SCHEMA_C_BYTES) {
    Ok(x) => x,
    Err(_) => panic!("there are null characters in schema"),
};
