use super::binary::Bytea;
use super::memory_vecf16::{Vecf16Input, Vecf16Output};
use base::scalar::F16;
use base::vector::Vecf16Borrowed;
use pgrx::datum::Internal;
use pgrx::datum::IntoDatum;
use pgrx::pg_sys::Oid;
use std::ffi::c_char;

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_vecf16_send(vector: Vecf16Input<'_>) -> Bytea {
    use pgrx::pg_sys::StringInfoData;
    unsafe {
        let mut buf = StringInfoData::default();
        let dims = vector.dims() as u16;
        let b_slice = std::mem::size_of::<F16>() * dims as usize;
        pgrx::pg_sys::pq_begintypsend(&mut buf);
        pgrx::pg_sys::pq_sendbytes(&mut buf, (&dims) as *const u16 as _, 2);
        pgrx::pg_sys::pq_sendbytes(&mut buf, vector.slice().as_ptr() as _, b_slice as _);
        Bytea::new(pgrx::pg_sys::pq_endtypsend(&mut buf))
    }
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_vecf16_recv(internal: Internal, _oid: Oid, _typmod: i32) -> Vecf16Output {
    use pgrx::pg_sys::StringInfo;
    unsafe {
        let buf: StringInfo = internal.into_datum().unwrap().cast_mut_ptr();
        let dims = (pgrx::pg_sys::pq_getmsgbytes(buf, 2) as *const u16).read_unaligned();

        let b_slice = std::mem::size_of::<F16>() * dims as usize;
        let p_slice = pgrx::pg_sys::pq_getmsgbytes(buf, b_slice as _);
        let mut slice = Vec::<F16>::with_capacity(dims as usize);
        std::ptr::copy(p_slice, slice.as_mut_ptr().cast::<c_char>(), b_slice);
        slice.set_len(dims as usize);

        if let Some(x) = Vecf16Borrowed::new_checked(&slice) {
            Vecf16Output::new(x)
        } else {
            pgrx::error!("detect data corruption");
        }
    }
}
