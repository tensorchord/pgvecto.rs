use super::memory_bvector::{BVectorInput, BVectorOutput};
use crate::datatype::typmod::Typmod;
use crate::error::*;
use base::vector::*;
use pgrx::pg_sys::Oid;
use std::ffi::{CStr, CString};
use std::fmt::Write;

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvector_in(input: &CStr, _oid: Oid, typmod: i32) -> BVectorOutput {
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
            let dims = u32::try_from(vector.len()).expect("input is too large");
            check_value_dims_65535(dims);
            let mut data = vec![0_u64; dims.div_ceil(BVECTOR_WIDTH) as _];
            for i in 0..dims {
                if vector[i as usize] {
                    data[(i / BVECTOR_WIDTH) as usize] |= 1 << (i % BVECTOR_WIDTH);
                }
            }
            BVectorOutput::new(BVectBorrowed::new(dims, &data))
        }
    }
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvector_out(vector: BVectorInput<'_>) -> CString {
    let mut buffer = String::new();
    buffer.push('[');
    let mut iter = vector.as_borrowed().iter();
    if let Some(x) = iter.next() {
        write!(buffer, "{}", x as u32).unwrap();
    }
    for x in iter {
        write!(buffer, ", {}", x as u32).unwrap();
    }
    buffer.push(']');
    CString::new(buffer).unwrap()
}
