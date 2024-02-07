use std::ffi::CStr;

use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};

use crate::prelude::guc_parse_failed;

fn guc_string_parse(target: &'static GucSetting<Option<&'static CStr>>, name: String) -> String {
    let value = match target.get() {
        Some(s) => s,
        None => guc_parse_failed(&name, "uninitialized"),
    };
    match value.to_str() {
        Ok(s) => s.to_string(),
        Err(_e) => guc_parse_failed(&name, "utf8 parse failed"),
    }
}

pub struct OpenAIOptions {
    pub base_url: String,
    pub api_key: String,
}

pub fn openai_options() -> OpenAIOptions {
    let base_url = guc_string_parse(&OPENAI_BASE_URL, "vectors.openai_base".to_string());
    let api_key = guc_string_parse(&OPENAI_API_KEY, "vectors.openai_api_key".to_string());
    OpenAIOptions { base_url, api_key }
}

static OPENAI_API_KEY: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);

static OPENAI_BASE_URL: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(Some(unsafe {
        CStr::from_bytes_with_nul_unchecked(b"https://api.openai.com/v1/\0")
    }));

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
        "vectors.openai_base",
        "The base url of OpenAI or compatible server.",
        "",
        &OPENAI_BASE_URL,
        GucContext::Userset,
        GucFlags::default(),
    );
}
