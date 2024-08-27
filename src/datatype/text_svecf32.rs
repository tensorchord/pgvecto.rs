use super::memory_svecf32::SVecf32Output;
use crate::datatype::memory_svecf32::SVecf32Input;
use crate::error::*;
use base::vector::*;
use pgrx::error;
use pgrx::pg_sys::Oid;
use std::ffi::{CStr, CString};
use std::fmt::Write;

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_in(input: &CStr, _oid: Oid, _typmod: i32) -> SVecf32Output {
    use crate::utils::parse::parse_pgvector_svector;
    let v = parse_pgvector_svector(input.to_bytes(), |s| s.parse::<f32>().ok());
    match v {
        Err(e) => {
            bad_literal(&e.to_string());
        }
        Ok((mut indexes, mut values, dims)) => {
            let dims = u32::try_from(dims).expect("input is too large");
            check_value_dims_1048575(dims);
            // is_sorted
            if !indexes.windows(2).all(|i| i[0] <= i[1]) {
                assert_eq!(indexes.len(), values.len());
                let n = indexes.len();
                let mut permutation = (0..n).collect::<Vec<_>>();
                permutation.sort_unstable_by_key(|&i| &indexes[i]);
                for i in 0..n {
                    if i == permutation[i] || usize::MAX == permutation[i] {
                        continue;
                    }
                    let index = indexes[i];
                    let value = values[i];
                    let mut j = i;
                    while i != permutation[j] {
                        let next = permutation[j];
                        indexes[j] = indexes[permutation[j]];
                        values[j] = values[permutation[j]];
                        permutation[j] = usize::MAX;
                        j = next;
                    }
                    indexes[j] = index;
                    values[j] = value;
                    permutation[j] = usize::MAX;
                }
            }
            let mut last: Option<u32> = None;
            for index in indexes.clone() {
                if last == Some(index) {
                    error!(
                        "Indexes need to be unique, but there are more than one same index {index}"
                    )
                }
                if last >= Some(dims) {
                    error!("Index out of bounds: the dim is {dims} but the index is {index}");
                }
                last = Some(index);
                {
                    let mut i = 0;
                    let mut j = 0;
                    while j < values.len() {
                        if values[j] != 0.0 {
                            indexes[i] = indexes[j];
                            values[i] = values[j];
                            i += 1;
                        }
                        j += 1;
                    }
                    indexes.truncate(i);
                    values.truncate(i);
                }
            }
            SVecf32Output::new(SVectBorrowed::new(dims, &indexes, &values))
        }
    }
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_out(vector: SVecf32Input<'_>) -> CString {
    let dims = vector.as_borrowed().dims();
    let mut buffer = String::new();
    buffer.push('{');
    let svec = vector.as_borrowed();
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
