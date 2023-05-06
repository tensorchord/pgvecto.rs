use pgrx::{pg_sys::int64, prelude::*};

#[pg_guard]
pub(crate) extern "C" fn am_begin_scan(
    index_relation: pg_sys::Relation,
    n_keys: std::os::raw::c_int,
    n_order_bys: std::os::raw::c_int,
) -> pg_sys::IndexScanDesc {
    unimplemented!()
}

#[pg_guard]
pub(crate) extern "C" fn am_re_scan(
    scan: pg_sys::IndexScanDesc,
    keys: pg_sys::ScanKey,
    n_keys: std::os::raw::c_int,
    orderbys: pg_sys::ScanKey,
    n_orderbys: std::os::raw::c_int,
) {
    unimplemented!()
}

#[pg_guard]
pub(crate) extern "C" fn am_get_tuple(
    scan: pg_sys::IndexScanDesc,
    direction: pg_sys::ScanDirection,
) -> bool {
    unimplemented!()
}

#[pg_guard]
pub(crate) extern "C" fn am_end_scan(scan: pg_sys::IndexScanDesc) {
    unimplemented!()
}
