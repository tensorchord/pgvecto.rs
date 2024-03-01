use super::memory_vecf16::Vecf16Output;
use crate::datatype::memory_vecf16::Vecf16Input;
use crate::datatype::typmod::Typmod;
use crate::prelude::*;
use base::vector::Vecf16Borrowed;
use pgrx::pg_sys::Oid;
use std::ffi::{CStr, CString};

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_vecf16_in(input: &CStr, _oid: Oid, typmod: i32) -> Vecf16Output {
    use crate::utils::parse::parse_vector;
    let reserve = Typmod::parse_from_i32(typmod)
        .unwrap()
        .dims()
        .map(|x| x.get())
        .unwrap_or(0);
    let mut vector = Vec::<F16>::with_capacity(reserve as usize);
    if let Err(e) = parse_vector(input.to_bytes(), |s| match s.parse::<F16>() {
        Ok(s) => {
            vector.push(s);
            true
        }
        Err(_) => false,
    }) {
        bad_literal(&e.to_string());
    }
    check_value_dims_u16(vector.len());
    Vecf16Output::new(Vecf16Borrowed::new(&vector))
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_vecf16_out(vector: Vecf16Input<'_>) -> CString {
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
