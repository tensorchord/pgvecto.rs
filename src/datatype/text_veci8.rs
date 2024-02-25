use crate::datatype::memory_veci8::{Veci8Input, Veci8Output};
use crate::datatype::typmod::Typmod;
use crate::prelude::*;
use base::vector::Veci8Borrowed;
use pgrx::pg_sys::Oid;
use std::ffi::{CStr, CString};

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_veci8_in(input: &CStr, _oid: Oid, typmod: i32) -> Veci8Output {
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
            check_value_dims(vector.len());
            let (vector, alpha, offset) = i8_quantization(vector);
            Veci8Output::new(
                Veci8Borrowed::new_checked_without_precomputed(
                    vector.len() as u16,
                    &vector,
                    alpha,
                    offset,
                )
                .unwrap(),
            )
        }
    }
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_veci8_out(vector: Veci8Input<'_>) -> CString {
    let vector = i8_dequantization(vector.data(), vector.alpha(), vector.offset());
    let mut buffer = String::new();
    buffer.push('[');
    if let Some(&x) = vector.first() {
        buffer.push_str(format!("{}", x).as_str());
    }
    for &x in vector.iter().skip(1) {
        buffer.push_str(format!(", {}", x).as_str());
    }
    buffer.push(']');
    CString::new(buffer).unwrap()
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_to_veci8(len: i32, alpha: f32, offset: f32, values: pgrx::Array<i32>) -> Veci8Output {
    check_value_dims(len as usize);
    if (len as usize) != values.len() {
        bad_literal("Lengths of values and len are not matched.");
    }
    if values.contains_nulls() {
        bad_literal("Index or value contains nulls.");
    }
    let values = values
        .iter()
        .map(|x| I8(x.unwrap() as i8))
        .collect::<Vec<_>>();
    Veci8Output::new(
        Veci8Borrowed::new_checked_without_precomputed(
            values.len() as u16,
            &values,
            F32(alpha),
            F32(offset),
        )
        .unwrap(),
    )
}
