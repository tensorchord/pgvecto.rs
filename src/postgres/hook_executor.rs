use super::hook_transaction::drop_if_commit;
use crate::postgres::index_scan::Scanner;
use crate::prelude::*;
use std::ptr::null_mut;

type PlanstateTreeWalker =
    unsafe extern "C" fn(*mut pgrx::pg_sys::PlanState, *mut libc::c_void) -> bool;

pub unsafe fn post_executor_start(query_desc: *mut pgrx::pg_sys::QueryDesc) {
    // Before Postgres 16, type defination of `PlanstateTreeWalker` in the source code is incorrect.
    let planstate = (*query_desc).planstate;
    let context = null_mut();
    rewrite_plan_state(planstate, context);
}

pub unsafe fn pre_process_utility(pstmt: *mut pgrx::pg_sys::PlannedStmt) {
    unsafe {
        let utility_statement = pgrx::PgBox::from_pg((*pstmt).utilityStmt);

        let is_drop = pgrx::is_a(utility_statement.as_ptr(), pgrx::pg_sys::NodeTag_T_DropStmt);

        if is_drop {
            let stat_drop =
                pgrx::PgBox::from_pg(utility_statement.as_ptr() as *mut pgrx::pg_sys::DropStmt);

            match stat_drop.removeType {
                pgrx::pg_sys::ObjectType_OBJECT_TABLE | pgrx::pg_sys::ObjectType_OBJECT_INDEX => {
                    let objects = pgrx::PgList::<pgrx::pg_sys::Node>::from_pg(stat_drop.objects);
                    for object in objects.iter_ptr() {
                        let mut rel = std::ptr::null_mut();
                        let address = pgrx::pg_sys::get_object_address(
                            stat_drop.removeType,
                            object,
                            &mut rel,
                            pgrx::pg_sys::AccessExclusiveLock as pgrx::pg_sys::LOCKMODE,
                            stat_drop.missing_ok,
                        );

                        if address.objectId == pgrx::pg_sys::InvalidOid {
                            continue;
                        }

                        match stat_drop.removeType {
                            pgrx::pg_sys::ObjectType_OBJECT_TABLE => {
                                // Memory leak here?
                                let list = pgrx::pg_sys::RelationGetIndexList(rel);
                                let list = pgrx::PgList::<pgrx::pg_sys::Oid>::from_pg(list);
                                for index in list.iter_oid() {
                                    drop_if_commit(Id::from_sys(index));
                                }
                                pgrx::pg_sys::relation_close(
                                    rel,
                                    pgrx::pg_sys::AccessExclusiveLock as _,
                                );
                            }
                            pgrx::pg_sys::ObjectType_OBJECT_INDEX => {
                                drop_if_commit(Id::from_sys((*rel).rd_id));
                                pgrx::pg_sys::relation_close(
                                    rel,
                                    pgrx::pg_sys::AccessExclusiveLock as _,
                                );
                            }
                            _ => unreachable!(),
                        }
                    }
                }

                _ => {}
            }
        }
    }
}

#[pgrx::pg_guard]
unsafe extern "C" fn rewrite_plan_state(
    node: *mut pgrx::pg_sys::PlanState,
    context: *mut libc::c_void,
) -> bool {
    match (*node).type_ {
        pgrx::pg_sys::NodeTag_T_IndexScanState => {
            let node = node as *mut pgrx::pg_sys::IndexScanState;
            let index_relation = (*node).iss_RelationDesc;
            // Check the pointer of `amvalidate`.
            if index_relation
                .as_ref()
                .and_then(|p| p.rd_indam.as_ref())
                .and_then(|p| Some(p.amvalidate == Some(super::index::amvalidate)))
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
                    if (*node).iss_NumRuntimeKeys == 0 || (*node).iss_RuntimeKeysReady {
                        pgrx::pg_sys::index_rescan(
                            (*node).iss_ScanDesc,
                            (*node).iss_ScanKeys,
                            (*node).iss_NumScanKeys,
                            (*node).iss_OrderByKeys,
                            (*node).iss_NumOrderByKeys,
                        );
                    }
                    // inject
                    let scanner = &mut *((*(*node).iss_ScanDesc).opaque as *mut Scanner);
                    let Scanner::Initial {
                        index_scan_state, ..
                    } = scanner
                    else {
                        unreachable!()
                    };
                    *index_scan_state = Some(node);
                }
            }
        }
        _ => (),
    }
    let walker = std::mem::transmute::<PlanstateTreeWalker, _>(rewrite_plan_state);
    pgrx::pg_sys::planstate_tree_walker(node, Some(walker), context)
}
