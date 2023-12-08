use crate::datatype::vecf16::Vecf16;
use crate::datatype::vecf32::Vecf32;
use service::prelude::DynamicVector;

#[repr(C, align(8))]
struct Header {
    varlena: u32,
    kind: u8,
    pad: u8,
    len: u16,
}

pub unsafe fn from_datum(datum: pgrx::pg_sys::Datum) -> DynamicVector {
    let p = datum.cast_mut_ptr::<pgrx::pg_sys::varlena>();
    let q = pgrx::pg_sys::pg_detoast_datum(p);
    let vector = match (*q.cast::<Header>()).kind {
        32 => DynamicVector::F32((*q.cast::<Vecf32>()).data().to_vec()),
        16 => DynamicVector::F16((*q.cast::<Vecf16>()).data().to_vec()),
        _ => unreachable!(),
    };
    if p != q {
        pgrx::pg_sys::pfree(q.cast());
    }
    vector
}
