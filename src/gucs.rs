use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};

// GUC setting for OpenAI API key
pub(crate) static OPENAI_API_KEY_GUC: GucSetting<Option<&'static str>> = GucSetting::new(None);

// register guc
pub(crate) fn init() {
    GucRegistry::define_string_guc(
        "openai_api_key",
        "The API key of OpenAI",
        "The OpenAI API key is required to use OpenAI embedding",
        &OPENAI_API_KEY_GUC,
        GucContext::Userset,
        GucFlags::default(),
    )
}
