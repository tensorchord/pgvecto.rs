#![allow(unsafe_op_in_unsafe_fn)]

use crate::gucs::executing::search_options;
use crate::gucs::planning::Mode;
use crate::gucs::planning::SEARCH_MODE;
use crate::index::utils::from_datum;
use crate::ipc::{ClientBasic, ClientVbase};
use crate::prelude::*;
use pgrx::pg_sys::SK_ISNULL;
use pgrx::FromDatum;

pub enum Scanner {
    Initial {
        node: Option<*mut pgrx::pg_sys::IndexScanState>,
        vector: Option<OwnedVector>,
    },
    Basic {
        node: *mut pgrx::pg_sys::IndexScanState,
        basic: ClientBasic,
    },
    Vbase {
        node: *mut pgrx::pg_sys::IndexScanState,
        vbase: ClientVbase,
    },
}

impl Scanner {
    fn node(&self) -> Option<*mut pgrx::pg_sys::IndexScanState> {
        match self {
            Scanner::Initial { node, .. } => *node,
            Scanner::Basic { node, .. } => Some(*node),
            Scanner::Vbase { node, .. } => Some(*node),
        }
    }
}

pub unsafe fn make_scan(index_relation: pgrx::pg_sys::Relation) -> pgrx::pg_sys::IndexScanDesc {
    use pgrx::PgMemoryContexts;

    let scan = pgrx::pg_sys::RelationGetIndexScan(index_relation, 0, 1);

    (*scan).xs_recheck = false;
    (*scan).xs_recheckorderby = false;

    (*scan).opaque =
        PgMemoryContexts::CurrentMemoryContext.leak_and_drop_on_delete(Scanner::Initial {
            vector: None,
            node: None,
        }) as _;

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
    let scanner = std::mem::replace(
        scanner,
        Scanner::Initial {
            node: scanner.node(),
            vector,
        },
    );

    match scanner {
        Scanner::Initial { .. } => {}
        Scanner::Basic { basic, .. } => {
            basic.leave();
        }
        Scanner::Vbase { vbase, .. } => {
            vbase.leave();
        }
    }
}

pub unsafe fn next_scan(scan: pgrx::pg_sys::IndexScanDesc) -> bool {
    let scanner = &mut *((*scan).opaque as *mut Scanner);
    if let Scanner::Initial { node, vector } = scanner {
        let node = node.expect("Hook failed.");
        let vector = vector.as_ref().expect("Scan failed.");

        #[cfg(any(feature = "pg14", feature = "pg15"))]
        let oid = (*(*scan).indexRelation).rd_node.relNode;
        #[cfg(feature = "pg16")]
        let oid = (*(*scan).indexRelation).rd_locator.relNumber;
        let id = Handle::from_sys(oid);

        let rpc = check_client(crate::ipc::client());

        match SEARCH_MODE.get() {
            Mode::basic => {
                let opts = search_options();
                let basic = match rpc.basic(id, vector.clone(), opts) {
                    Ok(x) => x,
                    Err((_, BasicError::NotExist)) => bad_service_not_exist(),
                    Err((_, BasicError::Upgrade)) => bad_service_upgrade(),
                    Err((_, BasicError::InvalidVector)) => bad_service_invalid_vector(),
                    Err((_, BasicError::InvalidSearchOptions { reason: _ })) => unreachable!(),
                };
                *scanner = Scanner::Basic { node, basic };
            }
            Mode::vbase => {
                let opts = search_options();
                let vbase = match rpc.vbase(id, vector.clone(), opts) {
                    Ok(x) => x,
                    Err((_, VbaseError::NotExist)) => bad_service_not_exist(),
                    Err((_, VbaseError::Upgrade)) => bad_service_upgrade(),
                    Err((_, VbaseError::InvalidVector)) => bad_service_invalid_vector(),
                    Err((_, VbaseError::InvalidSearchOptions { reason: _ })) => unreachable!(),
                };
                *scanner = Scanner::Vbase { node, vbase };
            }
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
    }
}

pub unsafe fn end_scan(scan: pgrx::pg_sys::IndexScanDesc) {
    let scanner = &mut *((*scan).opaque as *mut Scanner);
    let scanner = std::mem::replace(
        scanner,
        Scanner::Initial {
            node: scanner.node(),
            vector: None,
        },
    );

    match scanner {
        Scanner::Initial { .. } => {}
        Scanner::Basic { basic, .. } => {
            basic.leave();
        }
        Scanner::Vbase { vbase, .. } => {
            vbase.leave();
        }
    }
}

#[allow(unused)]
unsafe fn execute_boolean_qual(
    state: *mut pgrx::pg_sys::ExprState,
    econtext: *mut pgrx::pg_sys::ExprContext,
) -> bool {
    use pgrx::PgMemoryContexts;
    if state.is_null() {
        return true;
    }
    assert!((*state).flags & pgrx::pg_sys::EEO_FLAG_IS_QUAL as u8 != 0);
    let mut is_null = true;
    pgrx::pg_sys::MemoryContextReset((*econtext).ecxt_per_tuple_memory);
    let ret = PgMemoryContexts::For((*econtext).ecxt_per_tuple_memory)
        .switch_to(|_| (*state).evalfunc.unwrap()(state, econtext, &mut is_null));
    assert!(!is_null);
    bool::from_datum(ret, is_null).unwrap()
}

#[allow(unused)]
unsafe fn check_quals(node: *mut pgrx::pg_sys::IndexScanState) -> bool {
    let slot = (*node).ss.ss_ScanTupleSlot;
    let econtext = (*node).ss.ps.ps_ExprContext;
    (*econtext).ecxt_scantuple = slot;
    if (*node).ss.ps.qual.is_null() {
        return true;
    }
    let state = (*node).ss.ps.qual;
    let econtext = (*node).ss.ps.ps_ExprContext;
    execute_boolean_qual(state, econtext)
}

#[allow(unused)]
unsafe fn check_mvcc(node: *mut pgrx::pg_sys::IndexScanState, p: Pointer) -> bool {
    let scan_desc = (*node).iss_ScanDesc;
    let heap_fetch = (*scan_desc).xs_heapfetch;
    let index_relation = (*heap_fetch).rel;
    let rd_tableam = (*index_relation).rd_tableam;
    let snapshot = (*scan_desc).xs_snapshot;
    let index_fetch_tuple = (*rd_tableam).index_fetch_tuple.unwrap();
    let mut all_dead = false;
    let slot = (*node).ss.ss_ScanTupleSlot;
    let mut heap_continue = false;
    let found = index_fetch_tuple(
        heap_fetch,
        &mut p.into_sys(),
        snapshot,
        slot,
        &mut heap_continue,
        &mut all_dead,
    );
    if found {
        return true;
    }
    while heap_continue {
        let found = index_fetch_tuple(
            heap_fetch,
            &mut p.into_sys(),
            snapshot,
            slot,
            &mut heap_continue,
            &mut all_dead,
        );
        if found {
            return true;
        }
    }
    false
}

#[allow(unused)]
unsafe fn check(node: *mut pgrx::pg_sys::IndexScanState, p: Pointer) -> bool {
    if !check_mvcc(node, p) {
        return false;
    }
    if !check_quals(node) {
        return false;
    }
    true
}
