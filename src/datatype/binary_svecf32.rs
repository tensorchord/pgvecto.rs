use super::binary::Bytea;
use super::memory_svecf32::SVecf32Input;
use super::memory_svecf32::SVecf32Output;
use base::scalar::F32;
use base::vector::SVecf32Borrowed;
use pgrx::datum::Internal;
use pgrx::datum::IntoDatum;
use pgrx::pg_sys::Oid;
use std::ffi::c_char;

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_svecf32_send(vector: SVecf32Input<'_>) -> Bytea {
    use pgrx::pg_sys::StringInfoData;
    unsafe {
        let mut buf = StringInfoData::default();
        let dims = vector.dims() as u32;
        let len = vector.len() as u32;
        let x = vector.for_borrow();
        let b_indexes = std::mem::size_of::<u32>() * len as usize;
        let b_values = std::mem::size_of::<F32>() * len as usize;
        pgrx::pg_sys::pq_begintypsend(&mut buf);
        pgrx::pg_sys::pq_sendbytes(&mut buf, (&dims) as *const u32 as _, 4);
        pgrx::pg_sys::pq_sendbytes(&mut buf, (&len) as *const u32 as _, 4);
        pgrx::pg_sys::pq_sendbytes(&mut buf, x.indexes().as_ptr() as _, b_indexes as _);
        pgrx::pg_sys::pq_sendbytes(&mut buf, x.values().as_ptr() as _, b_values as _);
        Bytea::new(pgrx::pg_sys::pq_endtypsend(&mut buf))
    }
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_svecf32_recv(internal: Internal, _oid: Oid, _typmod: i32) -> SVecf32Output {
    use pgrx::pg_sys::StringInfo;
    unsafe {
        let buf: StringInfo = internal.into_datum().unwrap().cast_mut_ptr();
        let dims = (pgrx::pg_sys::pq_getmsgbytes(buf, 4) as *const u32).read_unaligned();
        let len = (pgrx::pg_sys::pq_getmsgbytes(buf, 4) as *const u32).read_unaligned();

        let b_indexes = std::mem::size_of::<u32>() * len as usize;
        let p_indexes = pgrx::pg_sys::pq_getmsgbytes(buf, b_indexes as _);
        let mut indexes = Vec::<u32>::with_capacity(len as usize);
        std::ptr::copy(p_indexes, indexes.as_mut_ptr().cast::<c_char>(), b_indexes);
        indexes.set_len(len as usize);

        let b_values = std::mem::size_of::<F32>() * len as usize;
        let p_values = pgrx::pg_sys::pq_getmsgbytes(buf, b_values as _);
        let mut values = Vec::<F32>::with_capacity(len as usize);
        std::ptr::copy(p_values, values.as_mut_ptr().cast::<c_char>(), b_values);
        values.set_len(len as usize);

        if let Some(x) = SVecf32Borrowed::new_checked(dims, &indexes, &values) {
            SVecf32Output::new(x)
        } else {
            pgrx::error!("detect data corruption");
        }
    }
}
