use crate::datatype::memory_veci8::{Veci8Input, Veci8Output};
use crate::datatype::typmod::Typmod;
use crate::error::*;
use base::vector::*;
use pgrx::pg_sys::Oid;
use std::ffi::{CStr, CString};

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
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
            let dims = u32::try_from(vector.len()).expect("input is too large");
            check_value_dims_65535(dims);
            let (data, alpha, offset) = veci8::i8_quantization(&vector);
            Veci8Output::new(Veci8Borrowed::new(&data, alpha, offset))
        }
    }
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_veci8_out(vector: Veci8Input<'_>) -> CString {
    let vector = veci8::i8_dequantization(vector.data(), vector.alpha(), vector.offset());
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
