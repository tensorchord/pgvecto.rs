use super::binary::Bytea;
use super::memory_vecf16::{Vecf16Input, Vecf16Output};
use base::vector::VectBorrowed;
use half::f16;
use pgrx::datum::Internal;
use pgrx::datum::IntoDatum;
use pgrx::pg_sys::Oid;
use std::ffi::c_char;

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf16_send(vector: Vecf16Input<'_>) -> Bytea {
    use pgrx::pg_sys::StringInfoData;
    unsafe {
        let mut buf = StringInfoData::default();
        let dims = vector.dims();
        let internal_dims = dims as u16;
        let b_slice = size_of::<f16>() * dims as usize;
        pgrx::pg_sys::pq_begintypsend(&mut buf);
        pgrx::pg_sys::pq_sendbytes(&mut buf, (&internal_dims) as *const u16 as _, 2);
        pgrx::pg_sys::pq_sendbytes(&mut buf, vector.slice().as_ptr() as _, b_slice as _);
        Bytea::new(pgrx::pg_sys::pq_endtypsend(&mut buf))
    }
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf16_recv(internal: Internal, oid: Oid, typmod: i32) -> Vecf16Output {
    let _ = (oid, typmod);
    use pgrx::pg_sys::StringInfo;
    unsafe {
        let buf: StringInfo = internal.into_datum().unwrap().cast_mut_ptr();
        let internal_dims = (pgrx::pg_sys::pq_getmsgbytes(buf, 2) as *const u16).read_unaligned();
        let dims = internal_dims as u32;

        let b_slice = size_of::<f16>() * dims as usize;
        let p_slice = pgrx::pg_sys::pq_getmsgbytes(buf, b_slice as _);
        let mut slice = Vec::<f16>::with_capacity(dims as usize);
        std::ptr::copy(p_slice, slice.as_mut_ptr().cast::<c_char>(), b_slice);
        slice.set_len(dims as usize);

        if let Some(x) = VectBorrowed::new_checked(&slice) {
            Vecf16Output::new(x)
        } else {
            pgrx::error!("detect data corruption");
        }
    }
}
