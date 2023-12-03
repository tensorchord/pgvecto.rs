use super::gucs::ENABLE_PREFILTER;
use super::hook_transaction::{client, ClientGuard};
use crate::ipc::client::SearchVbaseHandler;
use crate::postgres::datatype::VectorInput;
use crate::postgres::gucs::{K, VBASE_RANGE};
use crate::postgres::hook_transaction::client_guard;
use crate::prelude::*;
use pgrx::FromDatum;

pub struct Scanner {
    pub index_scan_state: *mut pgrx::pg_sys::IndexScanState,
    pub state: ScannerState,
}

pub enum ScannerState {
    Initial {
        vector: Option<Vec<Scalar>>,
    },
    Once {
        data: Vec<Pointer>,
    },
    Iter {
        handler: SearchVbaseHandler,
        guard: ClientGuard,
    },
    Stop,
}

pub unsafe fn make_scan(
    index_relation: pgrx::pg_sys::Relation,
    n_keys: std::os::raw::c_int,
    n_orderbys: std::os::raw::c_int,
) -> pgrx::pg_sys::IndexScanDesc {
    use pgrx::PgMemoryContexts;

    assert!(n_keys == 0);
    assert!(n_orderbys == 1);

    let scan = pgrx::pg_sys::RelationGetIndexScan(index_relation, n_keys, n_orderbys);

    (*scan).xs_recheck = false;
    (*scan).xs_recheckorderby = false;

    let scanner = Scanner {
        index_scan_state: std::ptr::null_mut(),
        state: ScannerState::Initial { vector: None },
    };

    (*scan).opaque = PgMemoryContexts::CurrentMemoryContext.leak_and_drop_on_delete(scanner) as _;

    scan
}

pub unsafe fn start_scan(
    scan: pgrx::pg_sys::IndexScanDesc,
    keys: pgrx::pg_sys::ScanKey,
    n_keys: std::os::raw::c_int,
    orderbys: pgrx::pg_sys::ScanKey,
    n_orderbys: std::os::raw::c_int,
) {
    use ScannerState::*;

    assert!((*scan).numberOfKeys == n_keys);
    assert!((*scan).numberOfOrderBys == n_orderbys);
    assert!(n_keys == 0);
    assert!(n_orderbys == 1);

    if n_keys > 0 {
        std::ptr::copy(keys, (*scan).keyData, n_keys as usize);
    }
    if n_orderbys > 0 {
        std::ptr::copy(orderbys, (*scan).orderByData, n_orderbys as usize);
    }
    if n_orderbys > 0 {
        let size = std::mem::size_of::<pgrx::pg_sys::Datum>();
        let size = size * (*scan).numberOfOrderBys as usize;
        let data = pgrx::pg_sys::palloc0(size) as *mut _;
        (*scan).xs_orderbyvals = data;
    }
    if n_orderbys > 0 {
        let size = std::mem::size_of::<bool>();
        let size = size * (*scan).numberOfOrderBys as usize;
        let data = pgrx::pg_sys::palloc(size) as *mut bool;
        data.write_bytes(1, (*scan).numberOfOrderBys as usize);
        (*scan).xs_orderbynulls = data;
    }
    let orderby = orderbys.add(0);
    let argument = (*orderby).sk_argument;
    let vector = VectorInput::from_datum(argument, false).unwrap();
    let vector = vector.to_vec();

    let state = &mut (*((*scan).opaque as *mut Scanner)).state;
    *state = Initial {
        vector: Some(vector),
    };
}

pub unsafe fn next_scan(scan: pgrx::pg_sys::IndexScanDesc) -> bool {
    use ScannerState::*;

    let scanner = &mut *((*scan).opaque as *mut Scanner);
    if matches!(scanner.state, Stop) {
        return false;
    }

    if matches!(scanner.state, Initial { .. }) {
        let Initial { vector } = std::mem::replace(&mut scanner.state, Initial { vector: None })
        else {
            unreachable!()
        };

        #[cfg(any(feature = "pg12", feature = "pg13", feature = "pg14", feature = "pg15"))]
        let oid = (*(*scan).indexRelation).rd_node.relNode;
        #[cfg(feature = "pg16")]
        let oid = (*(*scan).indexRelation).rd_locator.relNumber;
        let id = Id::from_sys(oid);
        let vector = vector.expect("`rescan` is never called.");
        let index_scan_state = scanner.index_scan_state;

        if VBASE_RANGE.get() == 0 {
            let prefilter = !index_scan_state.is_null() && ENABLE_PREFILTER.get();
            client(|rpc| {
                let k = K.get() as _;
                let mut handler = rpc.search(id, (vector, k), prefilter).friendly();
                let mut res;
                let rpc = loop {
                    use crate::ipc::client::SearchHandle::*;
                    match handler.handle().friendly() {
                        Check { p, x } => {
                            let result = check(index_scan_state, p);
                            handler = x.leave(result).friendly();
                        }
                        Leave { result, x } => {
                            res = result.friendly();
                            break x;
                        }
                    }
                };
                res.reverse();
                scanner.state = Once { data: res };
                rpc
            });
        } else {
            let range = VBASE_RANGE.get() as _;
            let (rpc, guard) = client_guard();
            let handler = rpc.search_vbase(id, (vector, range)).friendly();
            scanner.state = Iter { handler, guard };
        }
    }

    if let Once { data } = &mut scanner.state {
        if let Some(p) = data.pop() {
            (*scan).xs_heaptid = p.into_sys();
            return true;
        }
        scanner.state = Stop;
        return false;
    }

    let Iter { handler, guard } = std::mem::replace(&mut scanner.state, Stop) else {
        unreachable!()
    };
    use crate::ipc::client::SearchVbaseHandle::*;
    match handler.handle().friendly() {
        Next { p, x } => {
            (*scan).xs_heaptid = p.into_sys();
            let handler = x.next().friendly();
            scanner.state = ScannerState::Iter { handler, guard };
            true
        }
        Leave { result, x } => {
            result.friendly();
            guard.reset(x);
            false
        }
    }
}

pub unsafe fn end_scan(scan: pgrx::pg_sys::IndexScanDesc) {
    use ScannerState::*;

    let scanner = &mut *((*scan).opaque as *mut Scanner);
    if let Iter { handler, guard } = std::mem::replace(&mut scanner.state, Stop) {
        use crate::ipc::client::SearchVbaseHandle::*;
        match handler.handle().friendly() {
            Next { p, x } => {
                (*scan).xs_heaptid = p.into_sys();
                let client = x.stop().friendly();
                guard.reset(client);
            }
            Leave { result, x } => {
                result.friendly();
                guard.reset(x);
            }
        }
    }
}

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

unsafe fn check(node: *mut pgrx::pg_sys::IndexScanState, p: Pointer) -> bool {
    if !check_mvcc(node, p) {
        return false;
    }
    if !check_quals(node) {
        return false;
    }
    true
}
