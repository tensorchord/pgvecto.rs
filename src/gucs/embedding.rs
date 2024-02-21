use super::guc_string_parse;
use embedding::OpenAIOptions;
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};
use std::ffi::CStr;

pub fn openai_options() -> OpenAIOptions {
    let base_url = guc_string_parse(&OPENAI_BASE_URL, "vectors.openai_base");
    let api_key = guc_string_parse(&OPENAI_API_KEY, "vectors.openai_api_key");
    OpenAIOptions { base_url, api_key }
}

static OPENAI_API_KEY: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);

static OPENAI_BASE_URL: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(Some(c"https://api.openai.com/v1/"));

pub unsafe fn init() {
    GucRegistry::define_string_guc(
        "vectors.openai_api_key",
        "The API key of OpenAI.",
        "",
        &OPENAI_API_KEY,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        "vectors.openai_base_url",
        "The base url of OpenAI or compatible server.",
        "",
        &OPENAI_BASE_URL,
        GucContext::Userset,
        GucFlags::default(),
    );
}
