use super::binary::Bytea;
use super::memory_veci8::{Veci8Input, Veci8Output};
use base::scalar::{F32, I8};
use base::vector::Veci8Borrowed;
use pgrx::datum::Internal;
use pgrx::datum::IntoDatum;
use pgrx::pg_sys::Oid;
use std::ffi::c_char;

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_veci8_send(vector: Veci8Input<'_>) -> Bytea {
    use pgrx::pg_sys::StringInfoData;
    unsafe {
        let mut buf = StringInfoData::default();
        let dims = vector.dims();
        let alpha = vector.alpha();
        let offset = vector.offset();
        let sum = vector.sum();
        let l2_norm = vector.l2_norm();
        let bytes = std::mem::size_of::<I8>() * dims as usize;
        pgrx::pg_sys::pq_begintypsend(&mut buf);
        pgrx::pg_sys::pq_sendbytes(&mut buf, (&dims) as *const u32 as _, 4);
        pgrx::pg_sys::pq_sendbytes(&mut buf, (&alpha) as *const F32 as _, 4);
        pgrx::pg_sys::pq_sendbytes(&mut buf, (&offset) as *const F32 as _, 4);
        pgrx::pg_sys::pq_sendbytes(&mut buf, (&sum) as *const F32 as _, 4);
        pgrx::pg_sys::pq_sendbytes(&mut buf, (&l2_norm) as *const F32 as _, 4);
        pgrx::pg_sys::pq_sendbytes(&mut buf, vector.data().as_ptr() as _, bytes as _);
        Bytea::new(pgrx::pg_sys::pq_endtypsend(&mut buf))
    }
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_veci8_recv(internal: Internal, oid: Oid, typmod: i32) -> Veci8Output {
    let _ = (oid, typmod);
    use pgrx::pg_sys::StringInfo;
    unsafe {
        let buf: StringInfo = internal.into_datum().unwrap().cast_mut_ptr();
        let dims = (pgrx::pg_sys::pq_getmsgbytes(buf, 4) as *const u32).read_unaligned();
        let alpha = (pgrx::pg_sys::pq_getmsgbytes(buf, 4) as *const F32).read_unaligned();
        let offset = (pgrx::pg_sys::pq_getmsgbytes(buf, 4) as *const F32).read_unaligned();
        let _sum = (pgrx::pg_sys::pq_getmsgbytes(buf, 4) as *const F32).read_unaligned();
        let _l2_norm = (pgrx::pg_sys::pq_getmsgbytes(buf, 4) as *const F32).read_unaligned();
        let bytes = std::mem::size_of::<I8>() * dims as usize;
        let ptr = pgrx::pg_sys::pq_getmsgbytes(buf, bytes as _);
        let mut slice = Vec::<I8>::with_capacity(dims as usize);
        std::ptr::copy(ptr, slice.as_mut_ptr().cast::<c_char>(), bytes);
        slice.set_len(dims as usize);
        if let Some(x) = Veci8Borrowed::new_checked(&slice, alpha, offset) {
            Veci8Output::new(x)
        } else {
            pgrx::error!("detect data corruption");
        }
    }
}
