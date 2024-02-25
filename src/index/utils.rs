#![allow(unsafe_op_in_unsafe_fn)]

use crate::datatype::memory_svecf32::SVecf32Header;
use crate::datatype::memory_vecf16::Vecf16Header;
use crate::datatype::memory_vecf32::Vecf32Header;
use crate::datatype::memory_veci8::Veci8Header;
use crate::prelude::*;

#[repr(C, align(8))]
struct Header {
    varlena: u32,
    dims: u16,
    kind: u16,
}

pub unsafe fn from_datum(datum: pgrx::pg_sys::Datum) -> OwnedVector {
    let p = datum.cast_mut_ptr::<pgrx::pg_sys::varlena>();
    let q = pgrx::pg_sys::pg_detoast_datum(p);
    let vector = match (*q.cast::<Header>()).kind {
        0 => {
            let v = &*q.cast::<Vecf32Header>();
            OwnedVector::Vecf32(v.for_borrow().for_own())
        }
        1 => {
            let v = &*q.cast::<Vecf16Header>();
            OwnedVector::Vecf16(v.for_borrow().for_own())
        }
        2 => {
            let v = &*q.cast::<SVecf32Header>();
            OwnedVector::SVecF32(v.for_borrow().for_own())
        }
        3 => {
            let v = &*q.cast::<Veci8Header>();
            OwnedVector::Veci8(v.for_borrow().for_own())
        }
        _ => unreachable!(),
    };
    if p != q {
        pgrx::pg_sys::pfree(q.cast());
    }
    vector
}
