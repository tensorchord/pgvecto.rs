#![allow(unsafe_op_in_unsafe_fn)]

use crate::error::*;
use crate::gucs::executing::search_options;
use crate::gucs::planning::Mode;
use crate::gucs::planning::SEARCH_MODE;
use crate::index::utils::{from_datum, get_handle};
use crate::ipc::{ClientBasic, ClientVbase};
use crate::utils::sys::IntoSys;
use base::index::*;
use base::vector::*;
use pgrx::pg_sys::SK_ISNULL;

pub enum Scanner {
    Initial { vector: Option<OwnedVector> },
    Basic { basic: ClientBasic },
    Vbase { vbase: ClientVbase },
    Empty {},
}

pub unsafe fn make_scan(index_relation: pgrx::pg_sys::Relation) -> pgrx::pg_sys::IndexScanDesc {
    use pgrx::PgMemoryContexts;

    let scan = pgrx::pg_sys::RelationGetIndexScan(index_relation, 0, 1);

    (*scan).xs_recheck = false;
    (*scan).xs_recheckorderby = false;

    (*scan).opaque = PgMemoryContexts::CurrentMemoryContext
        .leak_and_drop_on_delete(Scanner::Initial { vector: None }) as _;

    (*scan).xs_orderbyvals = pgrx::pg_sys::palloc0(std::mem::size_of::<pgrx::pg_sys::Datum>()) as _;

    (*scan).xs_orderbynulls = {
        let data = pgrx::pg_sys::palloc(std::mem::size_of::<bool>()) as *mut bool;
        data.write_bytes(1, 1);
        data
    };

    scan
}

pub unsafe fn start_scan(scan: pgrx::pg_sys::IndexScanDesc, orderbys: pgrx::pg_sys::ScanKey) {
    std::ptr::copy(orderbys, (*scan).orderByData, 1);
    let is_null = (SK_ISNULL & (*orderbys.add(0)).sk_flags as u32) != 0;
    let vector = from_datum((*orderbys.add(0)).sk_argument, is_null);

    let scanner = &mut *((*scan).opaque as *mut Scanner);
    let scanner = std::mem::replace(scanner, Scanner::Initial { vector });

    match scanner {
        Scanner::Initial { .. } => {}
        Scanner::Basic { basic, .. } => {
            basic.leave();
        }
        Scanner::Vbase { vbase, .. } => {
            vbase.leave();
        }
        Scanner::Empty {} => {}
    }
}

pub unsafe fn next_scan(scan: pgrx::pg_sys::IndexScanDesc) -> bool {
    let scanner = &mut *((*scan).opaque as *mut Scanner);
    if let Scanner::Initial { vector } = scanner {
        if let Some(vector) = vector.as_ref() {
            // https://www.postgresql.org/docs/current/index-locking.html
            // If heap entries referenced physical pointers are deleted before
            // they are consumed by PostgreSQL, PostgreSQL will received wrong
            // physical pointers: no rows or irreverent rows are referenced.
            if (*(*scan).xs_snapshot).snapshot_type != pgrx::pg_sys::SnapshotType_SNAPSHOT_MVCC {
                pgrx::error!("scanning with a non-MVCC-compliant snapshot is not supported");
            }

            let oid = (*(*scan).indexRelation).rd_id;
            let id = get_handle(oid);

            let rpc = check_client(crate::ipc::client());

            match SEARCH_MODE.get() {
                Mode::basic => {
                    let opts = search_options();
                    let basic = match rpc.basic(id, vector.clone(), opts) {
                        Ok(x) => x,
                        Err((_, BasicError::NotExist)) => bad_service_not_exist(),
                        Err((_, BasicError::InvalidVector)) => bad_service_invalid_vector(),
                        Err((_, BasicError::InvalidSearchOptions { reason: _ })) => unreachable!(),
                    };
                    *scanner = Scanner::Basic { basic };
                }
                Mode::vbase => {
                    let opts = search_options();
                    let vbase = match rpc.vbase(id, vector.clone(), opts) {
                        Ok(x) => x,
                        Err((_, VbaseError::NotExist)) => bad_service_not_exist(),
                        Err((_, VbaseError::InvalidVector)) => bad_service_invalid_vector(),
                        Err((_, VbaseError::InvalidSearchOptions { reason: _ })) => unreachable!(),
                    };
                    *scanner = Scanner::Vbase { vbase };
                }
            }
        } else {
            *scanner = Scanner::Empty {};
        }
    }
    match scanner {
        Scanner::Initial { .. } => unreachable!(),
        Scanner::Basic { basic, .. } => {
            if let Some(p) = basic.next() {
                (*scan).xs_heaptid = p.into_sys();
                true
            } else {
                false
            }
        }
        Scanner::Vbase { vbase, .. } => {
            if let Some(p) = vbase.next() {
                (*scan).xs_heaptid = p.into_sys();
                true
            } else {
                false
            }
        }
        Scanner::Empty {} => false,
    }
}

pub unsafe fn end_scan(scan: pgrx::pg_sys::IndexScanDesc) {
    let scanner = &mut *((*scan).opaque as *mut Scanner);
    let scanner = std::mem::replace(scanner, Scanner::Initial { vector: None });

    match scanner {
        Scanner::Initial { .. } => {}
        Scanner::Basic { basic, .. } => {
            basic.leave();
        }
        Scanner::Vbase { vbase, .. } => {
            vbase.leave();
        }
        Scanner::Empty {} => {}
    }
}
