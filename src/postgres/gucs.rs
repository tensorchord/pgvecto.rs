use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};
use std::ffi::CStr;

pub static OPENAI_API_KEY_GUC: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);

pub static PORT: GucSetting<i32> = GucSetting::<i32>::new(33509);

pub static K: GucSetting<i32> = GucSetting::<i32>::new(64);

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
        "vectors.port",
        "The port for the background worker to listen.",
        "If the system runs two or more Postgres clusters, ports should be set with different values.",
        &PORT,
        1,
        u16::MAX as _,
        GucContext::Postmaster,
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
}
