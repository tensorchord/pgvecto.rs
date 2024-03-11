use super::memory_vecf32::Vecf32Output;
use crate::datatype::memory_vecf32::Vecf32Input;
use crate::datatype::typmod::Typmod;
use crate::prelude::*;
use pgrx::pg_sys::Oid;
use std::ffi::{CStr, CString};

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_vecf32_in(input: &CStr, _oid: Oid, typmod: i32) -> Vecf32Output {
    use crate::utils::parse::parse_vector;
    let reserve = Typmod::parse_from_i32(typmod)
        .unwrap()
        .dims()
        .map(|x| x.get())
        .unwrap_or(0);
    let v = parse_vector(input.to_bytes(), reserve as usize, |s| s.parse().ok());
    match v {
        Err(e) => {
            bad_literal(&e.to_string());
        }
        Ok(vector) => {
            check_value_dims_65535(vector.len());
            Vecf32Output::new(Vecf32Borrowed::new(&vector))
        }
    }
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_vecf32_out(vector: Vecf32Input<'_>) -> CString {
    let mut buffer = String::new();
    buffer.push('[');
    if let Some(&x) = vector.slice().first() {
        buffer.push_str(format!("{}", x).as_str());
    }
    for &x in vector.slice().iter().skip(1) {
        buffer.push_str(format!(", {}", x).as_str());
    }
    buffer.push(']');
    CString::new(buffer).unwrap()
}
