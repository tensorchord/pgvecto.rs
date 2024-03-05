use crate::datatype::memory_veci8::{Veci8Input, Veci8Output};
use crate::datatype::typmod::Typmod;
use crate::prelude::*;
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
            check_value_dims_65535(vector.len());
            let (vector, alpha, offset) = i8_quantization(&vector);
            let (sum, l2_norm) = i8_precompute(&vector, alpha, offset);
            Veci8Output::new(
                Veci8Borrowed::new_checked(
                    vector.len() as u32,
                    &vector,
                    alpha,
                    offset,
                    sum,
                    l2_norm,
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
