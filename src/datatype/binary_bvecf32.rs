use super::binary::Bytea;
use super::memory_bvecf32::BVecf32Input;
use super::memory_bvecf32::BVecf32Output;
use base::vector::BVecf32Borrowed;
use base::vector::BVEC_WIDTH;
use pgrx::datum::Internal;
use pgrx::datum::IntoDatum;
use pgrx::pg_sys::Oid;

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_bvecf32_send(vector: BVecf32Input<'_>) -> Bytea {
    use pgrx::pg_sys::StringInfoData;
    unsafe {
        let mut buf = StringInfoData::default();
        let len = vector.dims() as u16;
        let bytes = (len as usize).div_ceil(BVEC_WIDTH) * std::mem::size_of::<usize>();
        pgrx::pg_sys::pq_begintypsend(&mut buf);
        pgrx::pg_sys::pq_sendbytes(&mut buf, (&len) as *const u16 as _, 2);
        pgrx::pg_sys::pq_sendbytes(&mut buf, vector.data().as_ptr() as _, bytes as _);
        Bytea::new(pgrx::pg_sys::pq_endtypsend(&mut buf))
    }
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_bvecf32_recv(internal: Internal, _oid: Oid, _typmod: i32) -> BVecf32Output {
    use pgrx::pg_sys::StringInfo;
    unsafe {
        let buf: StringInfo = internal.into_datum().unwrap().cast_mut_ptr();
        let dims = (pgrx::pg_sys::pq_getmsgbytes(buf, 2) as *const u16).read_unaligned();

        let l_slice = (dims as usize).div_ceil(BVEC_WIDTH);
        let b_slice = l_slice * std::mem::size_of::<usize>();
        let p_slice = pgrx::pg_sys::pq_getmsgbytes(buf, b_slice as _);
        let mut slice = Vec::<usize>::with_capacity(l_slice);
        std::ptr::copy(p_slice, slice.as_mut_ptr().cast(), b_slice);
        slice.set_len(l_slice);

        if let Some(x) = BVecf32Borrowed::new_checked(dims, &slice) {
            BVecf32Output::new(x)
        } else {
            pgrx::error!("detect data corruption");
        }
    }
}
