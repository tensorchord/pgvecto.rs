use embedding::OpenAIOptions;
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use std::ffi::CStr;

pub fn openai_options() -> OpenAIOptions {
    use crate::error::*;
    use pgrx::guc::GucSetting;
    use std::ffi::CStr;
    fn parse(target: &'static GucSetting<Option<&'static CStr>>, name: &'static str) -> String {
        let value = match target.get() {
            Some(s) => s,
            None => bad_guc_literal(name, "should not be `NULL`"),
        };
        match value.to_str() {
            Ok(s) => s.to_string(),
            Err(_e) => bad_guc_literal(name, "should be a valid UTF-8 string"),
        }
    }
    let base_url = parse(&OPENAI_BASE_URL, "vectors.openai_base_url");
    let api_key = parse(&OPENAI_API_KEY, "vectors.openai_api_key");
    OpenAIOptions { base_url, api_key }
}

static OPENAI_API_KEY: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);

static OPENAI_BASE_URL: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(Some(c"https://api.openai.com/v1"));

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
