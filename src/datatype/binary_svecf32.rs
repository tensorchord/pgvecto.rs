use super::memory_svecf32::SVecf32Input;
use super::memory_svecf32::SVecf32Output;
use base::scalar::F32;
use base::vector::SVecf32Borrowed;
use pgrx::datum::IntoDatum;
use pgrx::pg_sys::Datum;
use pgrx::pg_sys::Oid;
use std::ffi::c_char;

#[pgrx::pg_extern(sql = "\
CREATE FUNCTION _vectors_svecf32_send(svector) RETURNS bytea
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';")]
fn _vectors_svecf32_send(vector: SVecf32Input<'_>) -> Datum {
    use pgrx::pg_sys::StringInfoData;
    unsafe {
        let mut buf = StringInfoData::default();
        let dims = vector.dims() as u16;
        let len = vector.len() as u16;
        let x = vector.for_borrow();
        let b_indexes = std::mem::size_of::<u16>() * len as usize;
        let b_values = std::mem::size_of::<F32>() * len as usize;
        pgrx::pg_sys::pq_begintypsend(&mut buf);
        pgrx::pg_sys::pq_sendbytes(&mut buf, (&dims) as *const u16 as _, 2);
        pgrx::pg_sys::pq_sendbytes(&mut buf, (&len) as *const u16 as _, 2);
        pgrx::pg_sys::pq_sendbytes(&mut buf, x.indexes().as_ptr() as _, b_indexes as _);
        pgrx::pg_sys::pq_sendbytes(&mut buf, x.values().as_ptr() as _, b_values as _);
        Datum::from(pgrx::pg_sys::pq_endtypsend(&mut buf))
    }
}

#[pgrx::pg_extern(sql = "
CREATE FUNCTION _vectors_svecf32_recv(internal, oid, integer) RETURNS svector
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';")]
fn _vectors_svecf32_recv(internal: pgrx::Internal, _oid: Oid, _typmod: i32) -> SVecf32Output {
    use pgrx::pg_sys::StringInfo;
    unsafe {
        let buf: StringInfo = internal.into_datum().unwrap().cast_mut_ptr();
        let dims = (pgrx::pg_sys::pq_getmsgbytes(buf, 2) as *const u16).read_unaligned();
        let len = (pgrx::pg_sys::pq_getmsgbytes(buf, 2) as *const u16).read_unaligned();

        let b_indexes = std::mem::size_of::<u16>() * len as usize;
        let p_indexes = pgrx::pg_sys::pq_getmsgbytes(buf, b_indexes as _);
        let mut indexes = Vec::<u16>::with_capacity(len as usize);
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
