//! Postgres vector extension.
//!
//! Provides an easy-to-use extension for vector similarity search.
#![feature(allocator_api)]

use pgrx::prelude::*;

mod embedding;
mod gucs;
mod index;
mod operator;
mod udf;
mod datatype;

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
