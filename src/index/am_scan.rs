use crate::gucs::ENABLE_PREFILTER;
use crate::gucs::ENABLE_VBASE;
use crate::gucs::IVF_NPROBE;
use crate::gucs::K;
use crate::gucs::VBASE_RANGE;
use crate::index::utils::from_datum;
use crate::ipc::client::ClientGuard;
use crate::ipc::client::Vbase;
use crate::prelude::*;
use pgrx::FromDatum;
use service::index::segments::sealed::SealedSearchGucs;
use service::index::segments::SearchGucs;
use service::prelude::*;

pub enum Scanner {
    Initial {
        node: Option<*mut pgrx::pg_sys::IndexScanState>,
        vector: Option<DynamicVector>,
    },
    Search {
        node: *mut pgrx::pg_sys::IndexScanState,
        data: Vec<Pointer>,
    },
    Vbase {
        node: *mut pgrx::pg_sys::IndexScanState,
        vbase: ClientGuard<Vbase>,
    },
}

impl Scanner {
    fn node(&self) -> Option<*mut pgrx::pg_sys::IndexScanState> {
        match self {
            Scanner::Initial { node, .. } => *node,
            Scanner::Search { node, .. } => Some(*node),
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

    let vector = from_datum((*orderbys.add(0)).sk_argument);

    let scanner = &mut *((*scan).opaque as *mut Scanner);
    let scanner = std::mem::replace(
        scanner,
        Scanner::Initial {
            node: scanner.node(),
            vector: Some(vector),
        },
    );

    match scanner {
        Scanner::Initial { .. } => {}
        Scanner::Search { .. } => {}
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

        #[cfg(any(feature = "pg12", feature = "pg13", feature = "pg14", feature = "pg15"))]
        let oid = (*(*scan).indexRelation).rd_node.relNode;
        #[cfg(feature = "pg16")]
        let oid = (*(*scan).indexRelation).rd_locator.relNumber;
        let id = Handle::from_sys(oid);

        let mut rpc = crate::ipc::client::borrow_mut();

        if ENABLE_VBASE.get() {
            let vbase = rpc.vbase(id, (vector.clone(), VBASE_RANGE.get() as _));
            *scanner = Scanner::Vbase { node, vbase };
        } else {
            let k = K.get() as _;
            struct Search {
                node: *mut pgrx::pg_sys::IndexScanState,
            }

            impl crate::ipc::client::Search for Search {
                fn check(&mut self, p: Pointer) -> bool {
                    unsafe { check(self.node, p) }
                }
            }

            let search = Search { node };
            let gucs = SearchGucs {
                sealed: SealedSearchGucs {
                    ivf_nprob: IVF_NPROBE.get() as _,
                },
            };

            let mut data = rpc.search(
                id,
                (vector.clone(), k),
                ENABLE_PREFILTER.get(),
                gucs,
                search,
            );
            data.reverse();
            *scanner = Scanner::Search { node, data };
        }
    }
    match scanner {
        Scanner::Initial { .. } => unreachable!(),
        Scanner::Search { data, .. } => {
            if let Some(p) = data.pop() {
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
        Scanner::Search { .. } => {}
        Scanner::Vbase { vbase, .. } => {
            vbase.leave();
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
