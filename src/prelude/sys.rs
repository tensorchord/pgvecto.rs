use service::prelude::*;

pub trait FromSys<T> {
    fn from_sys(sys: T) -> Self;
}

impl FromSys<pgrx::pg_sys::Oid> for Handle {
    fn from_sys(sys: pgrx::pg_sys::Oid) -> Self {
        Self {
            newtype: sys.as_u32(),
        }
    }
}

impl FromSys<pgrx::pg_sys::ItemPointerData> for Pointer {
    fn from_sys(sys: pgrx::pg_sys::ItemPointerData) -> Self {
        let mut newtype = 0;
        newtype |= (sys.ip_blkid.bi_hi as u64) << 32;
        newtype |= (sys.ip_blkid.bi_lo as u64) << 16;
        newtype |= sys.ip_posid as u64;
        Self { newtype }
    }
}

pub trait IntoSys<T> {
    fn into_sys(self) -> T;
}

impl IntoSys<pgrx::pg_sys::ItemPointerData> for Pointer {
    fn into_sys(self) -> pgrx::pg_sys::ItemPointerData {
        pgrx::pg_sys::ItemPointerData {
            ip_blkid: pgrx::pg_sys::BlockIdData {
                bi_hi: ((self.newtype >> 32) & 0xffff) as u16,
                bi_lo: ((self.newtype >> 16) & 0xffff) as u16,
            },
            ip_posid: (self.newtype & 0xffff) as u16,
        }
    }
}
