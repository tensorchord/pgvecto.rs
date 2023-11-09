use super::gucs::ENABLE_PREFILTER;
use super::hook_transaction::client;
use crate::postgres::datatype::VectorInput;
use crate::postgres::gucs::K;
use crate::prelude::*;
use pgrx::FromDatum;
use std::collections::HashSet;

#[derive(Debug, Clone)]
pub enum Scanner {
    Initial {
        // fields to be filled by amhandler and hook
        vector: Option<Vec<Scalar>>,
        index_scan_state: Option<*mut pgrx::pg_sys::IndexScanState>,
        bitmap: Option<*mut pgrx::pg_sys::TIDBitmap>,
    },
    Type0 {
        data: Vec<Pointer>,
    },
    Type1 {
        index_scan_state: *mut pgrx::pg_sys::IndexScanState,
        data: Vec<Pointer>,
    },
    Type2 {
        index_scan_state: *mut pgrx::pg_sys::IndexScanState,
        bitmap: *mut pgrx::pg_sys::TIDBitmap,
        data: Vec<Pointer>,
    },
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

    let scanner = Scanner::Initial {
        vector: None,
        index_scan_state: None,
        bitmap: None,
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
    use Scanner::*;

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

    let last = (*((*scan).opaque as *mut Scanner)).clone();
    let scanner = (*scan).opaque as *mut Scanner;

    match last {
        Initial {
            index_scan_state,
            bitmap,
            ..
        } => {
            *scanner = Initial {
                vector: Some(vector),
                index_scan_state,
                bitmap,
            };
        }
        Type0 { data: _ } => {
            *scanner = Initial {
                vector: Some(vector),
                index_scan_state: None,
                bitmap: None,
            };
        }
        Type1 {
            index_scan_state,
            data: _,
        } => {
            *scanner = Initial {
                vector: Some(vector),
                index_scan_state: Some(index_scan_state),
                bitmap: None,
            };
        }
        Type2 {
            index_scan_state,
            bitmap,
            data: _,
        } => {
            *scanner = Initial {
                vector: Some(vector),
                index_scan_state: Some(index_scan_state),
                bitmap: Some(bitmap),
            };
        }
    }
}

pub unsafe fn next_scan(scan: pgrx::pg_sys::IndexScanDesc) -> bool {
    let scanner = &mut *((*scan).opaque as *mut Scanner);
    if matches!(scanner, Scanner::Initial { .. }) {
        let Scanner::Initial {
            vector,
            index_scan_state,
            bitmap,
        } = std::mem::replace(
            scanner,
            Scanner::Initial {
                vector: None,
                index_scan_state: None,
                bitmap: None,
            },
        )
        else {
            unreachable!()
        };
        let oid = (*(*scan).indexRelation).rd_id;
        let id = Id::from_sys(oid);
        let vector = vector.expect("`rescan` is never called.");
        if index_scan_state.is_some() && ENABLE_PREFILTER.get() {
            if bitmap.is_some() {
                client(|rpc| {
                    let index_scan_state = index_scan_state.unwrap();
                    let bitmap = bitmap.unwrap();
                    let k = K.get() as _;
                    let mut handler = rpc.search(id, (vector, k), true).unwrap();
                    let mut res;
                    let set = deal_pg_bitmap(bitmap);
                    let rpc = loop {
                        use crate::ipc::client::SearchHandle::*;
                        match handler.handle().unwrap() {
                            Check { p, x } => {
                                let result = set.contains(&p);
                                handler = x.leave(result).unwrap();
                            }
                            Leave { result, x } => {
                                res = result.friendly();
                                break x;
                            }
                        }
                    };
                    res.reverse();
                    *scanner = Scanner::Type2 {
                        index_scan_state,
                        bitmap,
                        data: res,
                    };
                    rpc
                });
            } else {
                client(|rpc| {
                    let index_scan_state = index_scan_state.unwrap();
                    let k = K.get() as _;
                    let mut handler = rpc.search(id, (vector, k), true).unwrap();
                    let mut res;
                    let rpc = loop {
                        use crate::ipc::client::SearchHandle::*;
                        match handler.handle().unwrap() {
                            Check { p, x } => {
                                let result = check(index_scan_state, p);
                                handler = x.leave(result).unwrap();
                            }
                            Leave { result, x } => {
                                res = result.friendly();
                                break x;
                            }
                        }
                    };
                    res.reverse();
                    *scanner = Scanner::Type1 {
                        index_scan_state,
                        data: res,
                    };
                    rpc
                });
            }
        } else {
            client(|rpc| {
                let k = K.get() as _;
                let handler = rpc.search(id, (vector, k), false).unwrap();
                let mut res;
                let rpc = loop {
                    use crate::ipc::client::SearchHandle::*;
                    match handler.handle().unwrap() {
                        Check { .. } => {
                            unreachable!()
                        }
                        Leave { result, x } => {
                            res = result.friendly();
                            break x;
                        }
                    }
                };
                res.reverse();
                *scanner = Scanner::Type0 { data: res };
                rpc
            });
        }
    }
    match scanner {
        Scanner::Initial { .. } => unreachable!(),
        Scanner::Type0 { data } => {
            if let Some(p) = data.pop() {
                #[cfg(feature = "pg11")]
                {
                    (*scan).xs_ctup.t_self = p.into_sys();
                }
                #[cfg(not(feature = "pg11"))]
                {
                    (*scan).xs_heaptid = p.into_sys();
                }
                true
            } else {
                false
            }
        }
        Scanner::Type1 { data, .. } => {
            if let Some(p) = data.pop() {
                #[cfg(feature = "pg11")]
                {
                    (*scan).xs_ctup.t_self = p.into_sys();
                }
                #[cfg(not(feature = "pg11"))]
                {
                    (*scan).xs_heaptid = p.into_sys();
                }
                true
            } else {
                false
            }
        }
        Scanner::Type2 { data, .. } => {
            if let Some(p) = data.pop() {
                #[cfg(feature = "pg11")]
                {
                    (*scan).xs_ctup.t_self = p.into_sys();
                }
                #[cfg(not(feature = "pg11"))]
                {
                    (*scan).xs_heaptid = p.into_sys();
                }
                true
            } else {
                false
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

unsafe fn deal_pg_bitmap(bitmap: *mut pgrx::pg_sys::TIDBitmap) -> HashSet<Pointer> {
    let iter = pgrx::pg_sys::tbm_begin_iterate(bitmap);
    let mut set = HashSet::new();
    loop {
        let res = pgrx::pg_sys::tbm_iterate(iter);
        if res.is_null() {
            break;
        }
        let block_num = (*res).blockno;
        let len = match (*res).ntuples {
            x if x < 0 => continue,
            x => x as usize,
        };
        let offsets = (*res).offsets.as_slice(len);
        for i in 0..len {
            let offset = offsets[i];
            let pointer = ((block_num as u64) << 16) | (offset as u64);
            set.insert(Pointer::from_u48(pointer));
        }
    }
    pgrx::pg_sys::tbm_end_iterate(iter);
    set
}
