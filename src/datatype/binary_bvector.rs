use super::binary::Bytea;
use super::memory_bvector::BVectorInput;
use super::memory_bvector::BVectorOutput;
use base::vector::BVectBorrowed;
use base::vector::BVECTOR_WIDTH;
use pgrx::datum::Internal;
use pgrx::datum::IntoDatum;
use pgrx::pg_sys::Oid;

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvector_send(vector: BVectorInput<'_>) -> Bytea {
    use pgrx::pg_sys::StringInfoData;
    unsafe {
        let mut buf = StringInfoData::default();
        let dims = vector.dims();
        let internal_dims = dims as u16;
        let bytes = dims.div_ceil(BVECTOR_WIDTH) as usize * size_of::<u64>();
        pgrx::pg_sys::pq_begintypsend(&mut buf);
        pgrx::pg_sys::pq_sendbytes(&mut buf, (&internal_dims) as *const u16 as _, 2);
        pgrx::pg_sys::pq_sendbytes(&mut buf, vector.data().as_ptr() as _, bytes as _);
        Bytea::new(pgrx::pg_sys::pq_endtypsend(&mut buf))
    }
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvector_recv(internal: Internal, oid: Oid, typmod: i32) -> BVectorOutput {
    let _ = (oid, typmod);
    use pgrx::pg_sys::StringInfo;
    unsafe {
        let buf: StringInfo = internal.into_datum().unwrap().cast_mut_ptr();
        let internal_dims = (pgrx::pg_sys::pq_getmsgbytes(buf, 2) as *const u16).read_unaligned();
        let dims = internal_dims as u32;

        let l_slice = dims.div_ceil(BVECTOR_WIDTH) as usize;
        let b_slice = l_slice * size_of::<u64>();
        let p_slice = pgrx::pg_sys::pq_getmsgbytes(buf, b_slice as _);
        let mut slice = Vec::<u64>::with_capacity(l_slice);
        std::ptr::copy(p_slice, slice.as_mut_ptr().cast(), b_slice);
        slice.set_len(l_slice);

        if let Some(x) = BVectBorrowed::new_checked(dims, &slice) {
            BVectorOutput::new(x)
        } else {
            pgrx::error!("detect data corruption");
        }
    }
}
