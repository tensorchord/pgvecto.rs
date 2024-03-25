use crate::error::*;
use pgrx::GucSetting;
use std::ffi::CStr;

pub mod embedding;
pub mod executing;
pub mod internal;
pub mod planning;

pub unsafe fn init() {
    unsafe {
        planning::init();
        internal::init();
        executing::init();
        embedding::init();
    }
}

fn guc_string_parse(
    target: &'static GucSetting<Option<&'static CStr>>,
    name: &'static str,
) -> String {
    let value = match target.get() {
        Some(s) => s,
        None => bad_guc_literal(name, "should not be `NULL`"),
    };
    match value.to_str() {
        Ok(s) => s.to_string(),
        Err(_e) => bad_guc_literal(name, "should be a valid UTF-8 string"),
    }
}
