pub mod executing;
pub mod internal;
pub mod planning;

use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};
use std::ffi::CStr;

pub static OPENAI_API_KEY_GUC: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);

pub unsafe fn init() {
    unsafe {
        self::planning::init();
        self::internal::init();
        self::executing::init();
    }
    // undocumented
    GucRegistry::define_string_guc(
        "vectors.openai_api_key",
        "The API key of OpenAI.",
        "",
        &OPENAI_API_KEY_GUC,
        GucContext::Userset,
        GucFlags::default(),
    );
}
