use super::memory_svecf32::SVecf32Output;
use crate::datatype::memory_svecf32::SVecf32Input;
use crate::prelude::*;
use base::scalar::F32;
use base::vector::{SVecf32Borrowed, VectorBorrowed};
use pgrx::pg_sys::Oid;
use std::ffi::{CStr, CString};

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_svecf32_in(input: &CStr, _oid: Oid, _typmod: i32) -> SVecf32Output {
    use crate::utils::parse::parse_vector;
    let mut dims = 0;
    let mut indexes = Vec::<u32>::new();
    let mut values = Vec::<F32>::new();
    if let Err(e) = parse_vector(input.to_bytes(), |s| match s.parse::<F32>() {
        Ok(val) => {
            if !val.is_zero() {
                indexes.push(dims);
                values.push(val);
            }
            dims += 1;
            true
        }
        Err(_) => false,
    }) {
        bad_literal(&e.to_string());
    }
    check_value_dims_max(dims as usize);
    SVecf32Output::new(SVecf32Borrowed::new(dims, &indexes, &values))
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_svecf32_out(vector: SVecf32Input<'_>) -> CString {
    let mut buffer = String::new();
    buffer.push('[');
    let vec = vector.for_borrow().to_vec();
    let mut iter = vec.iter();
    if let Some(x) = iter.next() {
        buffer.push_str(format!("{}", x).as_str());
    }
    for x in iter {
        buffer.push_str(format!(", {}", x).as_str());
    }
    buffer.push(']');
    CString::new(buffer).unwrap()
}
