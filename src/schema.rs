use std::ffi::CString;
use std::option_env;

const fn pgvectors_schema() -> &'static str {
    match option_env!("PGVECTORS_SCHEMA") {
        Some(val) => val,
        None => "vectors",
    }
}

pub(crate) const fn pgvectors_index_stat_name() -> &'static str {
    match option_env!("PGVECTORS_STAT_NAME") {
        Some(val) => val,
        None => "vectors.vector_index_stat",
    }
}

pub(crate) fn pgvectors_schema_cstr() -> CString {
    // CString is not a const func
    CString::new(pgvectors_schema()).expect("failed to convert the schema name to a c-string")
}
