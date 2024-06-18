use super::memory_svecf32::SVecf32Output;
use crate::datatype::memory_svecf32::SVecf32Input;
use crate::error::*;
use base::scalar::*;
use base::vector::*;
use pgrx::pg_sys::Oid;
use std::ffi::{CStr, CString};
use std::fmt::Write;

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_in(input: &CStr, _oid: Oid, _typmod: i32) -> SVecf32Output {
    use crate::utils::parse::{parse_pgvector_svector, svector_filter_nonzero};
    let v = parse_pgvector_svector(input.to_bytes(), |s| s.parse::<F32>().ok());
    match v {
        Err(e) => {
            bad_literal(&e.to_string());
        }
        Ok((indexes, values, dims)) => {
            check_value_dims_1048575(dims);
            check_index_in_bound(&indexes, dims);
            let (non_zero_indexes, non_zero_values) = svector_filter_nonzero(&indexes, &values);
            SVecf32Output::new(SVecf32Borrowed::new(
                dims as u32,
                &non_zero_indexes,
                &non_zero_values,
            ))
        }
    }
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_out(vector: SVecf32Input<'_>) -> CString {
    let dims = vector.for_borrow().dims();
    let mut buffer = String::new();
    buffer.push('{');
    let svec = vector.for_borrow();
    let mut need_splitter = false;
    for (&index, &value) in svec.indexes().iter().zip(svec.values().iter()) {
        match need_splitter {
            false => {
                write!(buffer, "{}:{}", index, value).unwrap();
                need_splitter = true;
            }
            true => write!(buffer, ", {}:{}", index, value).unwrap(),
        }
    }
    write!(buffer, "}}/{}", dims).unwrap();
    CString::new(buffer).unwrap()
}
