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
    let v = parse_vector(input.to_bytes(), reserve as usize, |s| {
        s.parse::<u8>().ok().and_then(|x| match x {
            0 => Some(false),
            1 => Some(true),
            _ => None,
        })
    });
    match v {
        Err(e) => {
            bad_literal(&e.to_string());
        }
        Ok(vector) => {
            check_value_dims_65535(vector.len());
            let mut values = BVecf32Owned::new_zeroed(vector.len() as u16);
            for (i, &x) in vector.iter().enumerate() {
                if x {
                    values.set(i, true);
                }
            }
            BVecf32Output::new(values.for_borrow())
        }
    }
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
