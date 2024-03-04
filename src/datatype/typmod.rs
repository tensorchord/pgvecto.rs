use crate::prelude::*;
use pgrx::Array;
use serde::{Deserialize, Serialize};
use std::ffi::{CStr, CString};
use std::num::{NonZeroU16, NonZeroU32};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Typmod {
    Any,
    Dims(NonZeroU32),
}

impl Typmod {
    pub fn parse_from_i32(x: i32) -> Option<Self> {
        use Typmod::*;
        if x == -1 {
            Some(Any)
        } else if x >= 1 {
            Some(Dims(NonZeroU32::new(x as u32).unwrap()))
        } else {
            None
        }
    }
    pub fn into_option_string(self) -> Option<String> {
        use Typmod::*;
        match self {
            Any => None,
            Dims(x) => Some(x.get().to_string()),
        }
    }
    pub fn into_i32(self) -> i32 {
        use Typmod::*;
        match self {
            Any => -1,
            Dims(x) => x.get() as i32,
        }
    }
    pub fn dims(self) -> Option<NonZeroU32> {
        use Typmod::*;
        match self {
            Any => None,
            Dims(dims) => Some(dims),
        }
    }
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_typmod_in_65535(list: Array<&CStr>) -> i32 {
    if list.is_empty() {
        -1
    } else if list.len() == 1 {
        let s = list.get(0).unwrap().unwrap().to_str().unwrap();
        let typmod = Typmod::Dims(check_type_dims_u16(s.parse::<NonZeroU16>().ok()).into());
        typmod.into_i32()
    } else {
        check_type_dims_u16(None);
        unreachable!()
    }
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_typmod_in_1048575(list: Array<&CStr>) -> i32 {
    if list.is_empty() {
        -1
    } else if list.len() == 1 {
        let s = list.get(0).unwrap().unwrap().to_str().unwrap();
        let typmod = Typmod::Dims(check_type_dims_max(s.parse::<NonZeroU32>().ok()));
        typmod.into_i32()
    } else {
        check_type_dims_max(None);
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
