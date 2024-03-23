use crate::datatype::memory_bvecf32::BVecf32Header;
use crate::datatype::memory_svecf32::SVecf32Header;
use crate::datatype::memory_vecf16::Vecf16Header;
use crate::datatype::memory_vecf32::Vecf32Header;
use crate::datatype::memory_veci8::Veci8Header;
use crate::utils::cells::PgCell;
use base::search::*;
use base::vector::*;

pub unsafe fn from_datum_to_vector(
    value: pgrx::pg_sys::Datum,
    is_null: bool,
) -> Option<OwnedVector> {
    #[repr(C, align(8))]
    struct Header {
        varlena: u32,
        _reserved: u16,
        kind: u16,
    }
    if is_null {
        return None;
    }
    let p = value.cast_mut_ptr::<pgrx::pg_sys::varlena>();
    let q = scopeguard::guard(unsafe { pgrx::pg_sys::pg_detoast_datum(p) }, |q| {
        if p != q {
            unsafe {
                pgrx::pg_sys::pfree(q.cast());
            }
        }
    });
    let vector = match unsafe { (*q.cast::<Header>()).kind } {
        0 => {
            let v = unsafe { &*q.cast::<Vecf32Header>() };
            Some(OwnedVector::Vecf32(v.for_borrow().for_own()))
        }
        1 => {
            let v = unsafe { &*q.cast::<Vecf16Header>() };
            Some(OwnedVector::Vecf16(v.for_borrow().for_own()))
        }
        2 => {
            let v = unsafe { &*q.cast::<SVecf32Header>() };
            Some(OwnedVector::SVecf32(v.for_borrow().for_own()))
        }
        3 => {
            let v = unsafe { &*q.cast::<BVecf32Header>() };
            Some(OwnedVector::BVecf32(v.for_borrow().for_own()))
        }
        4 => {
            let v = unsafe { &*q.cast::<Veci8Header>() };
            Some(OwnedVector::Veci8(v.for_borrow().for_own()))
        }
        _ => unreachable!(),
    };
    vector
}

pub fn from_oid_to_handle(oid: pgrx::pg_sys::Oid) -> Handle {
    static SYSTEM_IDENTIFIER: PgCell<u64> = unsafe { PgCell::new(0) };
    if SYSTEM_IDENTIFIER.get() == 0 {
        SYSTEM_IDENTIFIER.set(unsafe { pgrx::pg_sys::GetSystemIdentifier() });
    }
    let tenant_id = 0_u128;
    let cluster_id = SYSTEM_IDENTIFIER.get();
    let database_id = unsafe { pgrx::pg_sys::MyDatabaseId.as_u32() };
    let index_id = oid.as_u32();
    Handle::new(tenant_id, cluster_id, database_id, index_id)
}

pub fn pointer_to_ctid(pointer: Pointer) -> pgrx::pg_sys::ItemPointerData {
    let value = pointer.as_u64();
    pgrx::pg_sys::ItemPointerData {
        ip_blkid: pgrx::pg_sys::BlockIdData {
            bi_hi: ((value >> 32) & 0xffff) as u16,
            bi_lo: ((value >> 16) & 0xffff) as u16,
        },
        ip_posid: (value & 0xffff) as u16,
    }
}

pub fn ctid_to_pointer(ctid: pgrx::pg_sys::ItemPointerData) -> Pointer {
    let mut value = 0;
    value |= (ctid.ip_blkid.bi_hi as u64) << 32;
    value |= (ctid.ip_blkid.bi_lo as u64) << 16;
    value |= ctid.ip_posid as u64;
    Pointer::new(value)
}
