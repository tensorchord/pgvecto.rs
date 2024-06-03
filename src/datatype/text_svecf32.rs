use super::memory_svecf32::SVecf32Output;
use crate::datatype::memory_svecf32::SVecf32Input;
use crate::error::*;
use base::scalar::*;
use base::vector::*;
use pgrx::pg_sys::Oid;
use std::ffi::{CStr, CString};

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_in(input: &CStr, _oid: Oid, _typmod: i32) -> SVecf32Output {
    use crate::utils::parse::parse_pgvector_svector;
    let v = parse_pgvector_svector(input.to_bytes(), |s| s.parse::<F32>().ok());
    match v {
        Err(e) => {
            bad_literal(&e.to_string());
        }
        Ok((indexes, values, dims)) => {
            check_value_dims_1048575(dims);
            SVecf32Output::new(SVecf32Borrowed::new(dims as u32, &indexes, &values))
        }
    }
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_out(vector: SVecf32Input<'_>) -> CString {
    let dims = vector.for_borrow().dims();
    let mut buffer = String::new();
    buffer.push('{');
    let svec = vector.for_borrow();
    let mut need_splitter = true;
    for (&index, &value) in svec.indexes().iter().zip(svec.values().iter()) {
        match need_splitter {
            true => {
                buffer.push_str(format!("{}:{}", index, value).as_str());
                need_splitter = false;
            }
            false => buffer.push_str(format!(", {}:{}", index, value).as_str()),
        }
    }
    buffer.push_str(format!("}}/{}", dims).as_str());
    CString::new(buffer).unwrap()
}
