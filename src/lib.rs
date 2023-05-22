//! Postgres vector extension.
//!
//! Provides an easy-to-use extension for vector similarity search.
#![feature(core_intrinsics)]
#![feature(allocator_api)]
#![feature(arbitrary_self_types)]
#![feature(let_chains)]
#![feature(lazy_cell)]

use pgrx::prelude::*;

mod datatype;
mod embedding;
mod gucs;
mod hnsw;
mod operator;
mod postgres;
mod udf;

pgrx::pg_module_magic!();

#[allow(non_snake_case)]
#[pg_guard]
pub unsafe extern "C" fn _PG_init() {
    gucs::init();
}

pgrx::extension_sql_file!("./sql/bootstrap.sql", bootstrap);
pgrx::extension_sql_file!("./sql/finalize.sql", finalize);

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
