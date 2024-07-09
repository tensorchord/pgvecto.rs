use crate::utils::cells::PgCell;
use base::search::*;

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
