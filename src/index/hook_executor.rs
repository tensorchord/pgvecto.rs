#![allow(unsafe_op_in_unsafe_fn)]

use crate::index::am_scan::Scanner;
use std::ptr::null_mut;

pub unsafe fn post_executor_start(query_desc: *mut pgrx::pg_sys::QueryDesc) {
    // Before Postgres 16, type definition of `PlanstateTreeWalker` in the source code is incorrect.
    let planstate = (*query_desc).planstate;
    let context = null_mut();
    rewrite_plan_state(planstate, context);
}

#[pgrx::pg_guard]
unsafe extern "C" fn rewrite_plan_state(
    node: *mut pgrx::pg_sys::PlanState,
    context: *mut libc::c_void,
) -> bool {
    if (*node).type_ == pgrx::pg_sys::NodeTag::T_IndexScanState {
        let node = node as *mut pgrx::pg_sys::IndexScanState;
        let index_relation = (*node).iss_RelationDesc;
        // Check the pointer of `amvalidate`.
        if index_relation
            .as_ref()
            .and_then(|p| p.rd_indam.as_ref())
            .map(|p| p.amvalidate == Some(super::am::amvalidate))
            .unwrap_or(false)
        {
            // The logic is copied from Postgres source code.
            if (*node).iss_ScanDesc.is_null() {
                (*node).iss_ScanDesc = pgrx::pg_sys::index_beginscan(
                    (*node).ss.ss_currentRelation,
                    (*node).iss_RelationDesc,
                    (*(*node).ss.ps.state).es_snapshot,
                    (*node).iss_NumScanKeys,
                    (*node).iss_NumOrderByKeys,
                );

                let scanner = &mut *((*(*node).iss_ScanDesc).opaque as *mut Scanner);
                *scanner = Scanner::Initial {
                    node: Some(node),
                    vector: None,
                };

                if (*node).iss_NumRuntimeKeys == 0 || (*node).iss_RuntimeKeysReady {
                    pgrx::pg_sys::index_rescan(
                        (*node).iss_ScanDesc,
                        (*node).iss_ScanKeys,
                        (*node).iss_NumScanKeys,
                        (*node).iss_OrderByKeys,
                        (*node).iss_NumOrderByKeys,
                    );
                }
            }
        }
    }
    #[cfg(any(feature = "pg14", feature = "pg15"))]
    {
        type PlanstateTreeWalker =
            unsafe extern "C" fn(*mut pgrx::pg_sys::PlanState, *mut libc::c_void) -> bool;
        let walker = std::mem::transmute::<PlanstateTreeWalker, _>(rewrite_plan_state);
        pgrx::pg_sys::planstate_tree_walker(node, Some(walker), context)
    }
    #[cfg(feature = "pg16")]
    {
        let walker = rewrite_plan_state;
        pgrx::pg_sys::planstate_tree_walker_impl(node, Some(walker), context)
    }
}
