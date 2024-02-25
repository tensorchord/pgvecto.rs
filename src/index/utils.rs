#![allow(unsafe_op_in_unsafe_fn)]

use crate::datatype::svecf32::SVecf32;
use crate::datatype::vecf16::Vecf16;
use crate::datatype::vecf32::Vecf32;
use crate::datatype::veci8::Veci8;
use service::prelude::*;
use crate::datatype::memory_svecf32::SVecf32Header;
use crate::datatype::memory_vecf16::Vecf16Header;
use crate::datatype::memory_vecf32::Vecf32Header;
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
        0 => DynamicVector::F32((*q.cast::<Vecf32>()).data().to_vec()),
        1 => DynamicVector::F16((*q.cast::<Vecf16>()).data().to_vec()),
        2 => {
            let v = &*q.cast::<SVecf32Header>();
            OwnedVector::SVecF32(v.for_borrow().for_own())
        }
        3 => {
            let veci8 = &*q.cast::<Veci8>();
            DynamicVector::I8(veci8.to_ref().to_owned())
        }
        _ => unreachable!(),
    };
    if p != q {
        pgrx::pg_sys::pfree(q.cast());
    }
    vector
}
