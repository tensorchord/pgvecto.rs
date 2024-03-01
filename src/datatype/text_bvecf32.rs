use super::memory_bvecf32::{BVecf32Input, BVecf32Output};
use crate::datatype::typmod::Typmod;
use crate::prelude::*;
use pgrx::pg_sys::Oid;
use std::ffi::{CStr, CString};
use std::fmt::Write;

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_bvecf32_in(input: &CStr, _oid: Oid, typmod: i32) -> BVecf32Output {
    use crate::utils::parse::parse_vector;
    let reserve = Typmod::parse_from_i32(typmod)
        .unwrap()
        .dims()
        .map(|x| x.get())
        .unwrap_or(0);
    let mut bool_vec = Vec::<bool>::with_capacity(reserve as usize);
    if let Err(e) = parse_vector(input.to_bytes(), |_, s| match s.parse::<u8>() {
        Ok(0) => {
            bool_vec.push(false);
            true
        }
        Ok(1) => {
            bool_vec.push(true);
            true
        }
        _ => false,
    }) {
        bad_literal(&e.to_string());
    }
    let mut values = BVecf32Owned::new_zeroed(bool_vec.len() as u16);
    for (i, &x) in bool_vec.iter().enumerate() {
        if x {
            values.set(i, true);
        }
    }
    BVecf32Output::new(values.for_borrow())
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_bvecf32_out(vector: BVecf32Input<'_>) -> CString {
    let mut buffer = String::new();
    buffer.push('[');
    let mut iter = vector.for_borrow().iter();
    if let Some(x) = iter.next() {
        write!(buffer, "{}", x as u32).unwrap();
    }
    for x in iter {
        write!(buffer, ", {}", x as u32).unwrap();
    }
    buffer.push(']');
    CString::new(buffer).unwrap()
}
