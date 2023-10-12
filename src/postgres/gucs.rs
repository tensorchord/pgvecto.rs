use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting, PostgresGucEnum};
use std::ffi::CStr;

pub static OPENAI_API_KEY_GUC: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);

pub static K: GucSetting<i32> = GucSetting::<i32>::new(64);

pub static FILTER_MODE: GucSetting<FilterMode> =
    GucSetting::<FilterMode>::new(FilterMode::Prefilter);

#[derive(PostgresGucEnum, PartialEq, Eq, Clone, Copy)]
pub enum FilterMode {
    Skip,
    Prefilter,
    Postfilter,
}

pub unsafe fn init() {
    GucRegistry::define_string_guc(
        "vectors.openai_api_key",
        "The API key of OpenAI.",
        "The OpenAI API key is required to use OpenAI embedding.",
        &OPENAI_API_KEY_GUC,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        "vectors.k",
        "The number of nearest neighbors to return for searching.",
        "The number of nearest neighbors to return for searching.",
        &K,
        1,
        u16::MAX as _,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_enum_guc(
        "vectors.filter_mode",
        "The filter mode for vector index",
        "The filter mode for vector index",
        &FILTER_MODE,
        GucContext::Userset,
        GucFlags::default(),
    );
}
