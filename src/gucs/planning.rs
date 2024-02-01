use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};

#[derive(Debug, Clone, Copy, pgrx::PostgresGucEnum)]
#[allow(non_camel_case_types)]
pub enum Mode {
    basic,
    vbase,
}

pub static ENABLE_INDEX: GucSetting<bool> = GucSetting::<bool>::new(true);

pub static SEARCH_MODE: GucSetting<Mode> = GucSetting::<Mode>::new(Mode::vbase);

pub static ENABLE_PGVECTOR_COMPATIBILITY: GucSetting<bool> = GucSetting::<bool>::new(false);

pub unsafe fn init() {
    GucRegistry::define_bool_guc(
        "vectors.enable_index",
        "Enables or disables the query planner's use of vector index-scan plan types.",
        "https://docs.pgvecto.rs/usage/search.html",
        &ENABLE_INDEX,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_enum_guc(
        "vectors.search_mode",
        "Search mode.",
        "https://docs.pgvecto.rs/usage/search.html",
        &SEARCH_MODE,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        "vectors.pgvector_compatibility",
        "Enables or disables pgvector compatibility mode.",
        "https://docs.pgvecto.rs/usage/compatibility.html",
        &ENABLE_PGVECTOR_COMPATIBILITY,
        GucContext::Userset,
        GucFlags::default(),
    )
}
