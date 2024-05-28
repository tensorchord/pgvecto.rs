use super::memory_svecf32::SVecf32Output;
use crate::datatype::memory_svecf32::SVecf32Input;
use crate::datatype::typmod::Typmod;
use crate::error::*;
use base::scalar::*;
use base::vector::*;
use num_traits::Zero;
use pgrx::pg_sys::Oid;
use std::ffi::{CStr, CString};

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_in(input: &CStr, _oid: Oid, typmod: i32) -> SVecf32Output {
    use crate::utils::parse::parse_pgvector_svector;
    let reserve = Typmod::parse_from_i32(typmod)
        .unwrap()
        .dims()
        .map(|x| x.get())
        .unwrap_or(0);
    let v = parse_pgvector_svector(input.to_bytes(), reserve as usize, |s| {
        s.parse::<F32>().ok()
    });
    match v {
        Err(e) => {
            bad_literal(&e.to_string());
        }
        Ok(vector) => {
            check_value_dims_1048575(vector.len());
            let mut indexes = Vec::<u32>::new();
            let mut values = Vec::<F32>::new();
            for (i, &x) in vector.iter().enumerate() {
                if !x.is_zero() {
                    indexes.push(i as u32);
                    values.push(x);
                }
            }
            SVecf32Output::new(SVecf32Borrowed::new(vector.len() as u32, &indexes, &values))
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
