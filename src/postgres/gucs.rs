use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};

// GUC setting for OpenAI API key
pub(crate) static OPENAI_API_KEY_GUC: GucSetting<Option<&'static str>> = GucSetting::new(None);

pub(crate) static BGWORKER_PORT: GucSetting<i32> = GucSetting::new(33509);

pub(crate) static SEARCH_K: GucSetting<i32> = GucSetting::new(64);

// register guc
pub(crate) fn init() {
    GucRegistry::define_string_guc(
        "openai_api_key",
        "The API key of OpenAI",
        "The OpenAI API key is required to use OpenAI embedding",
        &OPENAI_API_KEY_GUC,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        "bgworker_port",
        "The port for the bgworker to listen",
        "If the system runs over one Postgres cluster, the port should be set with different values",
        &BGWORKER_PORT,
        1,
        u16::MAX as _,
        GucContext::Postmaster,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        "search_k",
        "The number of nearest neighbors to return for searching",
        "The number of nearest neighbors to return for searching",
        &SEARCH_K,
        1,
        u16::MAX as _,
        GucContext::Userset,
        GucFlags::default(),
    );
}
