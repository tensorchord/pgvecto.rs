use crate::index::compat::pgvector_stmt_rewrite;

static mut PREV_EXECUTOR_START: pgrx::pg_sys::ExecutorStart_hook_type = None;
static mut PREV_PROCESS_UTILITY: pgrx::pg_sys::ProcessUtility_hook_type = None;

#[pgrx::pg_guard]
unsafe extern "C" fn vectors_executor_start(
    query_desc: *mut pgrx::pg_sys::QueryDesc,
    eflags: ::std::os::raw::c_int,
) {
    unsafe {
        if let Some(prev_executor_start) = PREV_EXECUTOR_START {
            prev_executor_start(query_desc, eflags);
        } else {
            pgrx::pg_sys::standard_ExecutorStart(query_desc, eflags);
        }
    }
    unsafe {
        super::hook_executor::post_executor_start(query_desc);
    }
}

#[pgrx::pg_guard]
unsafe extern "C" fn hook_pgvector_compatibility(
    pstmt: *mut pgrx::pg_sys::PlannedStmt,
    query_string: *const ::std::os::raw::c_char,
    read_only_tree: bool,
    context: pgrx::pg_sys::ProcessUtilityContext,
    params: pgrx::pg_sys::ParamListInfo,
    query_env: *mut pgrx::pg_sys::QueryEnvironment,
    dest: *mut pgrx::pg_sys::DestReceiver,
    completion_tag: *mut pgrx::pg_sys::QueryCompletion,
) {
    unsafe {
        pgvector_stmt_rewrite(pstmt);
    }
    unsafe {
        if let Some(prev_process_utility) = PREV_PROCESS_UTILITY {
            prev_process_utility(
                pstmt,
                query_string,
                read_only_tree,
                context,
                params,
                query_env,
                dest,
                completion_tag,
            );
        } else {
            pgrx::pg_sys::standard_ProcessUtility(
                pstmt,
                query_string,
                read_only_tree,
                context,
                params,
                query_env,
                dest,
                completion_tag,
            );
        }
    }
}

#[pgrx::pg_guard]
unsafe extern "C" fn xact_callback(event: pgrx::pg_sys::XactEvent, _data: pgrx::void_mut_ptr) {
    match event {
        pgrx::pg_sys::XactEvent_XACT_EVENT_PRE_COMMIT
        | pgrx::pg_sys::XactEvent_XACT_EVENT_PARALLEL_PRE_COMMIT => {
            super::hook_transaction::commit();
        }
        pgrx::pg_sys::XactEvent_XACT_EVENT_ABORT
        | pgrx::pg_sys::XactEvent_XACT_EVENT_PARALLEL_ABORT => {
            super::hook_transaction::abort();
        }
        _ => {}
    }
}

pub unsafe fn init() {
    unsafe {
        PREV_EXECUTOR_START = pgrx::pg_sys::ExecutorStart_hook;
        pgrx::pg_sys::ExecutorStart_hook = Some(vectors_executor_start);
        PREV_PROCESS_UTILITY = pgrx::pg_sys::ProcessUtility_hook;
        pgrx::pg_sys::ProcessUtility_hook = Some(hook_pgvector_compatibility);
    }
    unsafe {
        pgrx::pg_sys::RegisterXactCallback(Some(xact_callback), std::ptr::null_mut());
    }
}
