//! Postgres vector extension.
//!
//! Provides an easy-to-use extension for vector similarity search.
#![feature(allocator_api)]
#![feature(try_blocks)]
#![feature(async_fn_in_trait)]

use pgrx::prelude::*;

mod bgworker;
mod embedding;
mod hnsw;
mod postgres;
mod prelude;
mod udf;

pgrx::pg_module_magic!();

pgrx::extension_sql_file!("./sql/bootstrap.sql", bootstrap);
pgrx::extension_sql_file!("./sql/finalize.sql", finalize);

#[allow(non_snake_case)]
#[pg_guard]
pub unsafe extern "C" fn _PG_init() {
    use pgrx::bgworkers::BackgroundWorkerBuilder;
    use pgrx::bgworkers::BgWorkerStartTime;
    BackgroundWorkerBuilder::new("pgvectors")
        .set_function("pgvectors_main")
        .set_library("vectors")
        .set_argument(None)
        .set_start_time(BgWorkerStartTime::ConsistentState)
        .enable_spi_access()
        .enable_shmem_access(None)
        .load();
    postgres::gucs::init();
    postgres::index::init();
    postgres::hooks::init();
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
