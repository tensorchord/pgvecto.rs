use crate::prelude::*;
use pgrx::Array;
use serde::{Deserialize, Serialize};
use std::ffi::{CStr, CString};
use std::num::NonZeroU16;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Typmod {
    Any,
    Dims(NonZeroU16),
}

impl Typmod {
    pub fn parse_from_i32(x: i32) -> Option<Self> {
        use Typmod::*;
        if x == -1 {
            Some(Any)
        } else if 1 <= x && x <= u16::MAX as i32 {
            Some(Dims(NonZeroU16::new(x as u16).unwrap()))
        } else {
            None
        }
    }
    pub fn into_option_string(self) -> Option<String> {
        use Typmod::*;
        match self {
            Any => None,
            Dims(x) => Some(i32::from(x.get()).to_string()),
        }
    }
    pub fn into_i32(self) -> i32 {
        use Typmod::*;
        match self {
            Any => -1,
            Dims(x) => i32::from(x.get()),
        }
    }
    pub fn dims(self) -> Option<NonZeroU16> {
        use Typmod::*;
        match self {
            Any => None,
            Dims(dims) => Some(dims),
        }
    }
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_typmod_in(list: Array<&CStr>) -> i32 {
    if list.is_empty() {
        -1
    } else if list.len() == 1 {
        let s = list.get(0).unwrap().unwrap().to_str().unwrap();
        let typmod = Typmod::Dims(check_type_dimensions(s.parse::<NonZeroU16>().ok()));
        typmod.into_i32()
    } else {
        check_type_dimensions(None);
        unreachable!()
    }
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_typmod_out(typmod: i32) -> CString {
    let typmod = Typmod::parse_from_i32(typmod).unwrap();
    match typmod.into_option_string() {
        Some(s) => CString::new(format!("({})", s)).unwrap(),
        None => CString::new("()").unwrap(),
    }
}
