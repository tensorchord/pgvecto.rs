#![allow(unsafe_op_in_unsafe_fn)]

use crate::datatype::bvector::BVector;
use crate::datatype::svecf32::SVecf32;
use crate::datatype::vecf16::Vecf16;
use crate::datatype::vecf32::Vecf32;
use service::prelude::*;

#[repr(C, align(8))]
struct Header {
    varlena: u32,
    len: u16,
    kind: u8,
    reserved: u8,
}

pub unsafe fn from_datum(datum: pgrx::pg_sys::Datum) -> DynamicVector {
    let p = datum.cast_mut_ptr::<pgrx::pg_sys::varlena>();
    let q = pgrx::pg_sys::pg_detoast_datum(p);
    let vector = match (*q.cast::<Header>()).kind {
        0 => DynamicVector::F32((*q.cast::<Vecf32>()).data().to_vec()),
        1 => DynamicVector::F16((*q.cast::<Vecf16>()).data().to_vec()),
        2 => {
            let svec = &*q.cast::<SVecf32>();
            DynamicVector::SparseF32(SparseF32::from(svec.data()))
        }
        3 => DynamicVector::Binary(BinaryVec::from((*q.cast::<BVector>()).data())),
        _ => unreachable!(),
    };
    if p != q {
        pgrx::pg_sys::pfree(q.cast());
    }
    vector
}
